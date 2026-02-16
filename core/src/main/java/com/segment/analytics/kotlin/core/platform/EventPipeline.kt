package com.segment.analytics.kotlin.core.platform

import com.segment.analytics.kotlin.core.*
import com.segment.analytics.kotlin.core.platform.plugins.logger.LogKind
import com.segment.analytics.kotlin.core.platform.plugins.logger.log
import com.segment.analytics.kotlin.core.platform.plugins.logger.segmentLog
import com.segment.analytics.kotlin.core.platform.policies.FlushPolicy
import com.segment.analytics.kotlin.core.utilities.EncodeDefaultsJson
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader

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

    /** File paths currently being sent via customTrackUrl (one POST per event). Prevents duplicate send when a later flush runs before completion. */
    private val customTrackFileUrlsInFlight = mutableSetOf<String>()
    private val customTrackInFlightMutex = Mutex()

    protected open val storage get() = analytics.storage

    protected open val scope get() = analytics.analyticsScope

    protected open val fileIODispatcher get() = analytics.fileIODispatcher

    protected open val networkIODispatcher get() = analytics.networkIODispatcher

    var running: Boolean
        private set

    companion object {
        internal const val FLUSH_POISON = "#!flush"
        internal val FLUSH_EVENT = ScreenEvent(FLUSH_POISON, FLUSH_POISON, emptyJsonObject).apply { messageId = FLUSH_POISON }
        internal const val UPLOAD_SIG = "#!upload"
        const val MAX_CONCURRENT_EVENT_UPLOADS = 20
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
            val inFlight = customTrackInFlightMutex.withLock { customTrackFileUrlsInFlight.toSet() }
            val filesToProcess = fileUrlList.filter { it !in inFlight }

            for (url in filesToProcess) {
                val useCustomTrackUrl = httpClient.effectiveCustomTrackUrl != null
                if (useCustomTrackUrl) {
                    customTrackInFlightMutex.withLock { customTrackFileUrlsInFlight.add(url) }
                }

                var shouldCleanup = true
                if (useCustomTrackUrl) {
                    try {
                        sendBatchAsOneRequestPerEvent(url)
                        analytics.log("$logTag uploaded $url (custom track)")
                    } catch (e: Exception) {
                        analytics.reportInternalError(e)
                        shouldCleanup = handleUploadException(e, url)
                    } finally {
                        customTrackInFlightMutex.withLock { customTrackFileUrlsInFlight.remove(url) }
                    }
                } else {
                    storage.readAsStream(url)?.use { data ->
                        try {
                            val connection = httpClient.upload(apiHost)
                            connection.outputStream?.let {
                                data.copyTo(connection.outputStream)
                                connection.outputStream.close()
                                connection.close()
                            }
                            analytics.log("$logTag uploaded $url")
                        } catch (e: Exception) {
                            analytics.reportInternalError(e)
                            shouldCleanup = handleUploadException(e, url)
                        }
                    }
                }

                if (shouldCleanup) {
                    storage.removeFile(url)
                }
            }
        }
    }

    /**
     * Sends each event in the batch as a separate POST to customTrackUrl (body = single event JSON).
     * Called only when customTrackUrl is set; does not affect the default Segment path.
     */
    private suspend fun sendBatchAsOneRequestPerEvent(fileUrl: String) = withContext(networkIODispatcher) {
        if (httpClient.uploadToCustomTrackUrl() == null) throw IOException("Custom track URL not configured")
        val fileContent = withContext(fileIODispatcher) {
            storage.readAsStream(fileUrl)?.use { stream ->
                BufferedReader(InputStreamReader(stream)).readText()
            } ?: throw IOException("Failed to read batch file: $fileUrl")
        }
        val json = Json.parseToJsonElement(fileContent).jsonObject
        val batch = json["batch"]?.jsonArray ?: return@withContext
        if (batch.isEmpty()) return@withContext

        val semaphore = Semaphore(MAX_CONCURRENT_EVENT_UPLOADS)
        coroutineScope {
            batch.map { element ->
                async {
                    semaphore.withPermit {
                        val eventBody = element.toString()
                        val conn = httpClient.uploadToCustomTrackUrl() ?: return@withPermit
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
                    }
                }
            }.awaitAll()
        }
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