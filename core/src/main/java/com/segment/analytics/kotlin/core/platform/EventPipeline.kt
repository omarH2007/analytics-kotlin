package com.segment.analytics.kotlin.core.platform

import com.segment.analytics.kotlin.core.*
import com.segment.analytics.kotlin.core.platform.plugins.logger.LogKind
import com.segment.analytics.kotlin.core.platform.plugins.logger.log
import com.segment.analytics.kotlin.core.platform.plugins.logger.segmentLog
import com.segment.analytics.kotlin.core.platform.policies.FlushPolicy
import com.segment.analytics.kotlin.core.utilities.EncodeDefaultsJson
import com.segment.analytics.kotlin.core.utilities.StorageImpl
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withContext
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.File
import java.io.IOException
import java.io.InputStreamReader
import java.util.Collections

open class EventPipeline(
    private val analytics: Analytics,
    private val logTag: String,
    apiKey: String,
    private val flushPolicies: List<FlushPolicy>,
    var apiHost: String = Constants.DEFAULT_API_HOST
) {

    private var writeChannel: Channel<BaseEvent>

    private var uploadChannel: Channel<String>

    protected open val httpClient: HTTPClient = HTTPClient(
        apiKey,
        analytics.configuration.requestFactory,
        analytics.configuration.customTrackUrl
    )

    protected open val storage get() = analytics.storage

    protected open val scope get() = analytics.analyticsScope

    protected open val fileIODispatcher get() = analytics.fileIODispatcher

    protected open val networkIODispatcher get() = analytics.networkIODispatcher

    var running: Boolean
        private set

    companion object {
        private const val MAX_CONCURRENT_EVENT_UPLOADS = 20
        internal const val LEGACY_CUSTOM_TRACK_BOUNDARY_KEY = "segment.customTrackUrl.legacyBatchIndexBoundary"
        /** customTrackUrl only: batches being taken out of storage, shared by every pipeline. See [takeCustomTrackBatch]. */
        private val claimedCustomTrackBatches: MutableSet<String> = Collections.synchronizedSet(mutableSetOf())
        internal const val FLUSH_POISON = "#!flush"
        internal val FLUSH_EVENT = ScreenEvent(FLUSH_POISON, FLUSH_POISON, emptyJsonObject).apply { messageId = FLUSH_POISON }
        internal const val UPLOAD_SIG = "#!upload"
    }

    init {
        running = false

        writeChannel = Channel(UNLIMITED)
        uploadChannel = Channel(UNLIMITED)
    }

    fun put(event: BaseEvent) {
        writeChannel.trySend(event)
    }

    fun flush() {
        writeChannel.trySend(FLUSH_EVENT)
    }

    fun start() {
        if (running) return
        running = true

        // avoid to re-establish a channel if the pipeline just gets created
        if (writeChannel.isClosedForSend || writeChannel.isClosedForReceive) {
            writeChannel = Channel(UNLIMITED)
            uploadChannel = Channel(UNLIMITED)
        }

        schedule()
        write()
        upload()
    }

    fun stop() {
        if (!running) return
        running = false

        uploadChannel.cancel()
        writeChannel.cancel()
        unschedule()
    }

    open fun stringifyBaseEvent(payload: BaseEvent): String {
        val finalPayload = EncodeDefaultsJson.encodeToJsonElement(payload)
            .jsonObject.filterNot { (k, v) ->
                // filter out empty userId and traits values
                (k == "userId" && v.jsonPrimitive.content.isBlank()) || (k == "traits" && v == emptyJsonObject)
            }

        val stringVal = Json.encodeToString(finalPayload)
        return stringVal
    }

    private fun write() = scope.launch(fileIODispatcher) {
        // before this pipeline stores anything, so no batch written by this version is taken for legacy.
        recordLegacyCustomTrackBoundary()

        for (event in writeChannel) {
            // write to storage
            val isPoison = (event.messageId == FLUSH_POISON)
            if (!isPoison) try {
                val stringVal = stringifyBaseEvent(event)
                analytics.log("$logTag running $stringVal")
                storage.write(Storage.Constants.Events, stringVal)

                flushPolicies.forEach { flushPolicy -> flushPolicy.updateState(event) }
            }
            catch (e : Exception) {
                analytics.reportInternalError(e)
                Analytics.segmentLog("Error adding payload: $event", kind = LogKind.ERROR)
            }

            // if flush condition met, generate paths
            if (isPoison || flushPolicies.any { it.shouldFlush() }) {
                uploadChannel.trySend(UPLOAD_SIG)
                flushPolicies.forEach { it.reset() }
            }
        }
    }

    private fun upload() = scope.launch(networkIODispatcher) {
        uploadChannel.consumeEach {
            analytics.log("$logTag performing flush")
            withContext(fileIODispatcher) {
                storage.rollover()
            }

            val fileUrlList = parseFilePaths(storage.read(Storage.Constants.Events))

            if (httpClient.effectiveCustomTrackUrl != null) {
                uploadToCustomTrackUrl(fileUrlList)
                return@consumeEach
            }

            for (url in fileUrlList) {
                // upload event file
                var shouldCleanup = true
                storage.readAsStream(url)?.use { data ->
                    try {
                        val connection = httpClient.upload(apiHost)
                        connection.outputStream?.let {
                            // Write the payloads into the OutputStream
                            data.copyTo(connection.outputStream)
                            connection.outputStream.close()

                            // Upload the payloads.
                            connection.close()
                        }
                        // Cleanup uploaded payloads
                        analytics.log("$logTag uploaded $url")
                    } catch (e: Exception) {
                        analytics.reportInternalError(e)
                        shouldCleanup = handleUploadException(e, url)
                    }
                }

                if (shouldCleanup) {
                    storage.removeFile(url)
                }
            }
        }
    }

    /*
     * customTrackUrl: every event is attempted at most once and is never retried.
     *
     * A batch is taken out of storage *before* anything is sent. Whatever happens next (2xx, 4xx, 5xx,
     * timeout, offline, app killed mid-upload) it can never be read again, so a failing endpoint can't
     * make later flushes or app launches send the same events again.
     */
    private suspend fun uploadToCustomTrackUrl(fileUrlList: List<String>) {
        // the default Android storage keeps every writeKey's batches in one directory: only take this writeKey's.
        val ownBatches = if (storage is StorageImpl) fileUrlList.filter { customTrackBatchIndex(it) != null } else fileUrlList
        for (url in ownBatches) {
            val batch = takeCustomTrackBatch(url) ?: continue
            try {
                sendBatchAsOneRequestPerEvent(batch)
                analytics.log("$logTag uploaded $url (custom track)")
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                analytics.reportInternalError(e)
                analytics.log("$logTag dropped $url (custom track), it will not be retried: ${e.message}")
            }
        }
    }

    /**
     * Claims a batch, reads it and removes it from storage, returning its content to send. Returns null, and
     * nothing is sent, if another pipeline already took it, it can't be read yet, it can't be removed, or it is legacy.
     */
    private suspend fun takeCustomTrackBatch(url: String): String? = withContext(fileIODispatcher) {
        if (!claimedCustomTrackBatches.add(url)) return@withContext null
        try {
            if (isLegacyCustomTrackBatch(url)) {
                // if it can't be removed now, it stays legacy and is removed by a later flush, never sent.
                storage.removeFile(url)
                analytics.log("$logTag deleted legacy batch $url without sending it")
                return@withContext null
            }

            // unreadable: leave it for a later flush.
            val batch = try {
                storage.readAsStream(url)?.use { stream ->
                    BufferedReader(InputStreamReader(stream)).readText()
                }
            } catch (e: Exception) {
                null
            } ?: return@withContext null

            storage.removeFile(url)
            // a batch still in storage would be read and sent again by the next flush.
            if (customTrackBatchExists(url)) {
                analytics.log("$logTag unable to remove $url (custom track), it will not be sent")
                return@withContext null
            }
            batch
        } finally {
            claimedCustomTrackBatches.remove(url)
        }
    }

    private fun customTrackBatchExists(url: String): Boolean = try {
        storage.readAsStream(url)?.use { true } ?: false
    } catch (e: Exception) {
        // can't tell: assume it is still there, so it is not sent.
        true
    }

    /**
     * Sends each event in the batch as a separate POST to customTrackUrl (body = single event JSON).
     * Every event is attempted once even if others fail; the first failure is thrown afterwards.
     */
    private suspend fun sendBatchAsOneRequestPerEvent(batchContent: String) = withContext(networkIODispatcher) {
        if (httpClient.effectiveCustomTrackUrl == null) throw IOException("Custom track URL not configured")
        val json = Json.parseToJsonElement(batchContent).jsonObject
        val batch = json["batch"]?.jsonArray ?: return@withContext
        if (batch.isEmpty()) return@withContext

        val semaphore = Semaphore(MAX_CONCURRENT_EVENT_UPLOADS)
        val failures = coroutineScope {
            batch.map { element ->
                async {
                    semaphore.withPermit<Exception?> {
                        try {
                            val eventBody = element.toString()
                            val conn = httpClient.uploadToCustomTrackUrl() ?: return@withPermit null
                            try {
                                conn.outputStream.use { os ->
                                    os.write(eventBody.toByteArray(Charsets.UTF_8))
                                }
                                val code = conn.responseCode
                                if (code >= 300) {
                                    throw HTTPException(code, conn.responseMessage, null, conn.headerFields)
                                }
                            } finally {
                                conn.disconnect()
                            }
                            null
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            e
                        }
                    }
                }
            }.awaitAll()
        }
        val failure = failures.firstOrNull { it != null }
        if (failure != null) throw failure
    }

    /**
     * Batches finished before this version first ran with customTrackUrl were left by versions that re-sent failed
     * batches on every flush, so they were already attempted and failed. The first time, before anything is stored,
     * remember the storage's current file index: every such batch has a lower index, and every batch this version
     * finishes has this index or higher. Batches below it are deleted without being sent, see [takeCustomTrackBatch].
     * The boundary is saved once and never moves, so an interrupted purge can't take a later batch for legacy.
     * Needs the default storage, which names batches "<writeKey>-<index>"; a custom Storage skips the purge.
     */
    private fun recordLegacyCustomTrackBoundary() {
        try {
            if (httpClient.effectiveCustomTrackUrl == null) return
            val storage = storage as? StorageImpl ?: return
            val boundaryKey = legacyCustomTrackBoundaryKey(storage)
            if (storage.propertiesFile.contains(boundaryKey)) return
            storage.propertiesFile.put(boundaryKey, storage.propertiesFile.get(storage.fileIndexKey, 0))
        } catch (e: Exception) {
            analytics.reportInternalError(e)
        }
    }

    private fun isLegacyCustomTrackBatch(url: String): Boolean {
        val storage = storage as? StorageImpl ?: return false
        val index = customTrackBatchIndex(url) ?: return false
        val boundary = storage.propertiesFile.get(legacyCustomTrackBoundaryKey(storage), -1)
        return index < boundary
    }

    /** One boundary per file index, like the index itself. */
    private fun legacyCustomTrackBoundaryKey(storage: StorageImpl) = "$LEGACY_CUSTOM_TRACK_BOUNDARY_KEY.${storage.fileIndexKey}"

    /** Index of a batch the default storage named "<writeKey>-<index>" for this writeKey, otherwise null. */
    private fun customTrackBatchIndex(url: String): Int? {
        val name = File(url).name
        val prefix = "${analytics.configuration.writeKey}-"
        return if (name.startsWith(prefix)) name.removePrefix(prefix).toIntOrNull() else null
    }

    private fun schedule() {
        flushPolicies.forEach { it.schedule(analytics) }
    }

    private fun unschedule() {
        flushPolicies.forEach { it.unschedule() }
    }



    private fun handleUploadException(e: Exception, file: String): Boolean {
        var shouldCleanup = false
        if (e is HTTPException) {
            analytics.log("$logTag exception while uploading, ${e.message}")
            if (e.is4xx() && e.responseCode != 429) {
                // Simply log and proceed to remove the rejected payloads from the queue.
                Analytics.segmentLog(
                    message = "Payloads were rejected by server. Marked for removal.",
                    kind = LogKind.ERROR
                )
                shouldCleanup = true
            } else {
                Analytics.segmentLog(
                    message = "Error while uploading payloads",
                    kind = LogKind.ERROR
                )
            }
        }
        else {
            Analytics.segmentLog(
                """
                    | Error uploading events from batch file
                    | fileUrl="${file}"
                    | msg=${e.message}
                """.trimMargin(), kind = LogKind.ERROR
            )
        }

        return shouldCleanup
    }
}