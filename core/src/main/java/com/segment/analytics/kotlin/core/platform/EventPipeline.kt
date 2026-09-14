package com.segment.analytics.kotlin.core.platform

import com.segment.analytics.kotlin.core.*
import com.segment.analytics.kotlin.core.platform.plugins.logger.LogKind
import com.segment.analytics.kotlin.core.platform.plugins.logger.log
import com.segment.analytics.kotlin.core.platform.plugins.logger.segmentLog
import com.segment.analytics.kotlin.core.platform.policies.FlushPolicy
import com.segment.analytics.kotlin.core.utilities.EncodeDefaultsJson
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.io.BufferedReader
import java.io.IOException
import java.io.InputStreamReader
import java.net.ConnectException
import java.net.HttpURLConnection
import java.net.NoRouteToHostException
import java.net.UnknownHostException

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

    private var customTrackRetryEntries: MutableMap<String, CustomTrackRetryEntry>? = null

    protected open val storage get() = analytics.storage

    protected open val scope get() = analytics.analyticsScope

    protected open val fileIODispatcher get() = analytics.fileIODispatcher

    protected open val networkIODispatcher get() = analytics.networkIODispatcher

    var running: Boolean
        private set

    companion object {
        private const val MAX_CONCURRENT_EVENT_UPLOADS = 20
        private const val MAX_CUSTOM_TRACK_SERVER_FAILURES = 5
        private const val MAX_CUSTOM_TRACK_RETRY_AGE_MS = 72L * 60 * 60 * 1000
        private const val CUSTOM_TRACK_BASE_BACKOFF_MS = 30_000L
        private const val CUSTOM_TRACK_MAX_BACKOFF_MS = 10L * 60 * 1000
        private const val CUSTOM_TRACK_MAX_BACKOFF_SHIFT = 16
        private const val MAX_RETRY_AFTER_SECONDS = 60L * 60
        private const val HTTP_TOO_MANY_REQUESTS = 429
        private val customTrackRetryJson = Json { ignoreUnknownKeys = true }
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
            if (httpClient.effectiveCustomTrackUrl != null) {
                pruneCustomTrackRetryEntries(fileUrlList)
            }

            for (url in filesToProcess) {
                val useCustomTrackUrl = httpClient.effectiveCustomTrackUrl != null
                if (useCustomTrackUrl && !isCustomTrackRetryDue(url)) continue
                if (useCustomTrackUrl) {
                    customTrackInFlightMutex.withLock { customTrackFileUrlsInFlight.add(url) }
                }

                var shouldCleanup = true
                if (useCustomTrackUrl) {
                    try {
                        shouldCleanup = sendBatchAsOneRequestPerEvent(url)
                        analytics.log(
                            if (shouldCleanup) "$logTag uploaded $url (custom track)"
                            else "$logTag retry scheduled for $url (custom track)"
                        )
                    } catch (e: Exception) {
                        analytics.reportInternalError(e)
                        shouldCleanup = handleCustomTrackUploadException(e, url)
                    } finally {
                        customTrackInFlightMutex.withLock { customTrackFileUrlsInFlight.remove(url) }
                    }
                    if (shouldCleanup) {
                        removeCustomTrackRetryEntry(url)
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
     * Sends every still-pending event of the batch as a separate POST to customTrackUrl (body = single
     * event JSON) and returns true when the file is finished and can be removed, false when it has to be
     * retried later. Called only when customTrackUrl is set; does not affect the default Segment path.
     *
     * Each event gets its own outcome: one failing event no longer cancels the others, events that were
     * accepted or rejected (4xx other than 429) on an earlier attempt are not sent again, and a file that
     * cannot be read or parsed is dropped instead of being retried on every flush. Retryable failures are
     * bounded by [scheduleCustomTrackRetry].
     */
    private suspend fun sendBatchAsOneRequestPerEvent(fileUrl: String): Boolean = withContext(networkIODispatcher) {
        if (httpClient.effectiveCustomTrackUrl == null) throw IOException("Custom track URL not configured")
        val fileContent = withContext(fileIODispatcher) {
            storage.readAsStream(fileUrl)?.use { stream ->
                BufferedReader(InputStreamReader(stream)).readText()
            }
        } ?: return@withContext true
        val batch = try {
            Json.parseToJsonElement(fileContent).jsonObject["batch"]?.jsonArray
        } catch (e: Exception) {
            Analytics.segmentLog(
                message = "Unreadable custom track batch dropped: $fileUrl",
                kind = LogKind.ERROR
            )
            null
        }
        if (batch.isNullOrEmpty()) return@withContext true

        val previous = customTrackRetryEntry(fileUrl)
        val alreadyFinished = previous?.finishedMessageIds.orEmpty()
        val pending = batch.filter { it.messageIdOrNull() !in alreadyFinished }
        if (pending.isEmpty()) return@withContext true

        val semaphore = Semaphore(MAX_CONCURRENT_EVENT_UPLOADS)
        val results = supervisorScope {
            pending.map { element ->
                async { semaphore.withPermit { postCustomTrackEvent(element.toString()) } }
            }.awaitAll()
        }

        val rejectedCodes = results.filterIsInstance<CustomTrackResult.Rejected>().map { it.code }
        if (rejectedCodes.isNotEmpty()) {
            Analytics.segmentLog(
                message = "Payloads were rejected by server. Marked for removal. " +
                    "(${rejectedCodes.size} custom track event(s), HTTP ${rejectedCodes.distinct()})",
                kind = LogKind.ERROR
            )
        }
        val retries = results.filterIsInstance<CustomTrackResult.Retry>()
        if (retries.isEmpty()) return@withContext true

        val newlyFinished = pending.zip(results)
            .filter { (_, result) -> result !is CustomTrackResult.Retry }
            .mapNotNull { (element, _) -> element.messageIdOrNull() }
        scheduleCustomTrackRetry(
            fileUrl = fileUrl,
            previous = previous,
            newlyFinishedMessageIds = newlyFinished,
            reachedServer = retries.any { it.reachedServer },
            retryAfterMs = retries.mapNotNull { it.retryAfterMs }.maxOrNull()
        )
    }

    /**
     * POSTs one event to customTrackUrl and classifies the outcome: 2xx is delivered, 4xx other than 429
     * is rejected (retrying cannot fix it, so the event is dropped) and everything else is retryable.
     * Connectivity failures (no network, DNS, refused connection) are reported as not having reached the
     * server, so being offline never spends a file's retry budget.
     */
    private fun postCustomTrackEvent(eventBody: String): CustomTrackResult {
        val connection = try {
            httpClient.uploadToCustomTrackUrl()
        } catch (e: IOException) {
            null
        } ?: return CustomTrackResult.Retry(reachedServer = false, retryAfterMs = null)
        return try {
            connection.outputStream.use { os -> os.write(eventBody.toByteArray(Charsets.UTF_8)) }
            when (val code = connection.responseCode) {
                in 200..299 -> CustomTrackResult.Delivered
                HTTP_TOO_MANY_REQUESTS -> CustomTrackResult.Retry(true, connection.retryAfterMs())
                in 400..499 -> CustomTrackResult.Rejected(code)
                else -> CustomTrackResult.Retry(true, connection.retryAfterMs())
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            CustomTrackResult.Retry(reachedServer = !e.isConnectivityFailure(), retryAfterMs = null)
        } finally {
            connection.disconnect()
        }
    }

    /**
     * Records a failed attempt for [fileUrl] and returns true when the file should now be dropped: once
     * [MAX_CUSTOM_TRACK_SERVER_FAILURES] attempts have failed after reaching the server, or once the first
     * failure is older than [MAX_CUSTOM_TRACK_RETRY_AGE_MS]. Otherwise the next attempt is pushed back with
     * exponential backoff (or the server's Retry-After, whichever is later), the events that already
     * finished are remembered so they are not sent again, and false is returned.
     */
    private fun scheduleCustomTrackRetry(
        fileUrl: String,
        previous: CustomTrackRetryEntry?,
        newlyFinishedMessageIds: Collection<String>,
        reachedServer: Boolean,
        retryAfterMs: Long?
    ): Boolean {
        val now = java.lang.System.currentTimeMillis()
        val failures = (previous?.failures ?: 0) + 1
        val serverFailures = (previous?.serverFailures ?: 0) + if (reachedServer) 1 else 0
        val firstFailureAt = previous?.firstFailureAt ?: now
        if (serverFailures >= MAX_CUSTOM_TRACK_SERVER_FAILURES ||
            now - firstFailureAt >= MAX_CUSTOM_TRACK_RETRY_AGE_MS
        ) {
            Analytics.segmentLog(
                message = "Custom track batch dropped after $failures failed attempt(s): $fileUrl",
                kind = LogKind.ERROR
            )
            return true
        }
        val backoffMs = (CUSTOM_TRACK_BASE_BACKOFF_MS shl (failures - 1).coerceAtMost(CUSTOM_TRACK_MAX_BACKOFF_SHIFT))
            .coerceAtMost(CUSTOM_TRACK_MAX_BACKOFF_MS)
        val delayMs = maxOf(backoffMs, retryAfterMs ?: 0L)
        putCustomTrackRetryEntry(
            fileUrl,
            CustomTrackRetryEntry(
                failures = failures,
                serverFailures = serverFailures,
                firstFailureAt = firstFailureAt,
                nextAttemptAt = now + delayMs,
                finishedMessageIds = previous?.finishedMessageIds.orEmpty() + newlyFinishedMessageIds
            )
        )
        Analytics.segmentLog(
            message = "Error while uploading payloads. Custom track retry $failures in ${delayMs / 1000}s: $fileUrl",
            kind = LogKind.ERROR
        )
        return false
    }

    /**
     * Treats a failure outside the per-event sends (a storage error, for example) as a failed attempt, so
     * the file is retried with backoff and dropped once its budget is spent instead of looping on every
     * flush. Cancellation is rethrown untouched.
     */
    private fun handleCustomTrackUploadException(e: Exception, fileUrl: String): Boolean {
        if (e is CancellationException) throw e
        Analytics.segmentLog(
            """
                | Error uploading events from batch file
                | fileUrl="${fileUrl}"
                | msg=${e.message}
            """.trimMargin(), kind = LogKind.ERROR
        )
        return scheduleCustomTrackRetry(
            fileUrl = fileUrl,
            previous = customTrackRetryEntry(fileUrl),
            newlyFinishedMessageIds = emptyList(),
            reachedServer = true,
            retryAfterMs = null
        )
    }

    /** Retry bookkeeping for custom-track batch files, loaded from storage on first use. */
    private fun customTrackRetryEntries(): MutableMap<String, CustomTrackRetryEntry> =
        customTrackRetryEntries ?: loadCustomTrackRetryEntries().also { customTrackRetryEntries = it }

    /** Reads the persisted retry bookkeeping; an unreadable value is treated as empty. */
    private fun loadCustomTrackRetryEntries(): MutableMap<String, CustomTrackRetryEntry> {
        val stored = storage.read(Storage.Constants.CustomTrackRetryState) ?: return mutableMapOf()
        return try {
            customTrackRetryJson.decodeFromString<Map<String, CustomTrackRetryEntry>>(stored).toMutableMap()
        } catch (e: Exception) {
            mutableMapOf()
        }
    }

    /** Writes the retry bookkeeping back to storage, removing the key when nothing is pending. */
    private fun persistCustomTrackRetryEntries() {
        val entries = customTrackRetryEntries()
        if (entries.isEmpty()) {
            storage.remove(Storage.Constants.CustomTrackRetryState)
        } else {
            storage.writePrefs(
                Storage.Constants.CustomTrackRetryState,
                customTrackRetryJson.encodeToString<Map<String, CustomTrackRetryEntry>>(entries.toMap())
            )
        }
    }

    /** Batch files are tracked by name so the key survives a change of storage directory path. */
    private fun customTrackRetryKey(fileUrl: String) = fileUrl.substringAfterLast('/')

    private fun customTrackRetryEntry(fileUrl: String) = customTrackRetryEntries()[customTrackRetryKey(fileUrl)]

    /** True when [fileUrl] has never failed or its backoff has elapsed. */
    private fun isCustomTrackRetryDue(fileUrl: String): Boolean =
        customTrackRetryEntry(fileUrl)?.let { java.lang.System.currentTimeMillis() >= it.nextAttemptAt } ?: true

    private fun putCustomTrackRetryEntry(fileUrl: String, entry: CustomTrackRetryEntry) {
        customTrackRetryEntries()[customTrackRetryKey(fileUrl)] = entry
        persistCustomTrackRetryEntries()
    }

    private fun removeCustomTrackRetryEntry(fileUrl: String) {
        if (customTrackRetryEntries().remove(customTrackRetryKey(fileUrl)) != null) {
            persistCustomTrackRetryEntries()
        }
    }

    /** Forgets retry bookkeeping for batch files that no longer exist in storage. */
    private fun pruneCustomTrackRetryEntries(fileUrls: List<String>) {
        val liveKeys = fileUrls.map(::customTrackRetryKey).toSet()
        if (customTrackRetryEntries().keys.retainAll(liveKeys)) {
            persistCustomTrackRetryEntries()
        }
    }

    /** Server's Retry-After in milliseconds when given in seconds, capped at one hour. */
    private fun HttpURLConnection.retryAfterMs(): Long? =
        getHeaderField("Retry-After")?.trim()?.toLongOrNull()
            ?.takeIf { it >= 0 }
            ?.coerceAtMost(MAX_RETRY_AFTER_SECONDS)
            ?.times(1000)

    /** True when the failure (or one of its causes) means the server was never reached. */
    private fun Throwable.isConnectivityFailure(): Boolean =
        generateSequence(this) { it.cause }.take(8).any {
            it is UnknownHostException || it is ConnectException || it is NoRouteToHostException
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

/**
 * Retry bookkeeping for one custom-track batch file, persisted under
 * [Storage.Constants.CustomTrackRetryState] so limits and backoff survive app restarts.
 * [failures] counts every failed attempt (it drives the backoff), [serverFailures] only those that
 * reached the server (it drives the drop limit), and [finishedMessageIds] holds events that were
 * already delivered or rejected and must not be sent again.
 */
@Serializable
private data class CustomTrackRetryEntry(
    val failures: Int = 0,
    val serverFailures: Int = 0,
    val firstFailureAt: Long = 0L,
    val nextAttemptAt: Long = 0L,
    val finishedMessageIds: Set<String> = emptySet()
)

/** Outcome of POSTing a single event to customTrackUrl. */
private sealed class CustomTrackResult {
    object Delivered : CustomTrackResult()
    data class Rejected(val code: Int) : CustomTrackResult()
    data class Retry(val reachedServer: Boolean, val retryAfterMs: Long?) : CustomTrackResult()
}

/** The event's `messageId`, used to remember which events of a batch already finished. */
private fun JsonElement.messageIdOrNull(): String? =
    ((this as? JsonObject)?.get("messageId") as? JsonPrimitive)?.contentOrNull