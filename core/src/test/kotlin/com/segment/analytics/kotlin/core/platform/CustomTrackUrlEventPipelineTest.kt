package com.segment.analytics.kotlin.core.platform

import com.segment.analytics.kotlin.core.*
import com.segment.analytics.kotlin.core.platform.policies.CountBasedFlushPolicy
import com.segment.analytics.kotlin.core.utilities.ConcreteStorageProvider
import com.segment.analytics.kotlin.core.utilities.StorageImpl
import com.segment.analytics.kotlin.core.utils.clearPersistentStorage
import com.segment.analytics.kotlin.core.utils.mockAnalytics
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.mockk.*
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.IOException
import java.net.HttpURLConnection
import java.net.InetSocketAddress
import java.net.URL
import java.util.Collections
import java.util.Date
import java.util.concurrent.atomic.AtomicInteger

/**
 * customTrackUrl: every event is attempted at most once. A batch that fails for any reason is deleted and
 * never sent again, and batches left by versions that kept retrying are deleted without being sent.
 */
internal class CustomTrackUrlEventPipelineTest {

    private val testDispatcher = UnconfinedTestDispatcher()

    private val testScope = TestScope(testDispatcher)

    private val usedWriteKeys = mutableListOf<String>()

    private class Pipeline(
        val pipeline: EventPipeline,
        val storage: Storage,
        val sentBodies: MutableList<String>,
        val requestFactory: RequestFactory
    )

    companion object {
        private const val CUSTOM_TRACK_URL = "https://custom.example.com/track"
    }

    @AfterEach
    internal fun tearDown() {
        usedWriteKeys.forEach { clearPersistentStorage(it) }
    }

    @Test
    fun `failed events are never retried`() {
        val p = makePipeline("customTrackNeverRetried") { 500 }

        repeat(3) { p.pipeline.put(screen("NeverRetried $it")) }
        p.pipeline.flush()
        assertEquals(3, p.sentNames("NeverRetried").size)

        // later flushes (flush policies, new events, app launches) must not send them again.
        repeat(3) { p.pipeline.flush() }
        assertEquals(3, p.sentNames("NeverRetried").size, "Failed events must never be re-sent")
        assertTrue(p.pendingBatches().isEmpty(), "Failed batches must be deleted")
    }

    @Test
    fun `every failure kind is never retried`() {
        val outcomes: List<Pair<String, (String) -> Int>> = listOf(
            "offline" to { _: String -> throw IOException("offline") },
            "301" to { _: String -> 301 },
            "400" to { _: String -> 400 },
            "401" to { _: String -> 401 },
            "429" to { _: String -> 429 },
            "500" to { _: String -> 500 },
            "503" to { _: String -> 503 },
        )
        for ((name, outcome) in outcomes) {
            val p = makePipeline("customTrackFailureKind$name", outcome = outcome)

            p.pipeline.put(screen("FailureKind"))
            repeat(3) { p.pipeline.flush() }

            assertEquals(1, p.sentNames("FailureKind").size, "$name: attempted once, never again")
            assertTrue(p.pendingBatches().isEmpty(), name)
        }
    }

    @Test
    fun `one failing event does not stop the others and nothing is re-sent`() {
        val p = makePipeline("customTrackPartialFailure") { body -> if (body.contains("Partial 1")) 500 else 200 }

        repeat(3) { p.pipeline.put(screen("Partial $it")) }
        p.pipeline.flush()
        repeat(2) { p.pipeline.flush() }

        assertEquals(listOf("Partial 0", "Partial 1", "Partial 2"), p.sentNames("Partial").sorted())
        assertTrue(p.pendingBatches().isEmpty())
    }

    @Test
    fun `delivered events are sent once as plain JSON posts and removed`() {
        val p = makePipeline("customTrackDelivered") { 200 }

        repeat(3) { p.pipeline.put(screen("Delivered $it")) }
        p.pipeline.flush()
        p.pipeline.flush()

        assertEquals(listOf("Delivered 0", "Delivered 1", "Delivered 2"), p.sentNames("Delivered").sorted())
        p.sentBodies.forEach { body ->
            val json = Json.parseToJsonElement(body).jsonObject
            assertNull(json["batch"], "Each request body is a single event, not a batch")
            assertEquals("screen", json["type"]?.jsonPrimitive?.content)
        }
        assertTrue(p.pendingBatches().isEmpty())
        verify(exactly = 3) { p.requestFactory.uploadToCustomTrackUrl(any(), any()) }
    }

    @Test
    fun `legacy backlog is deleted without being sent`() {
        val writeKey = "customTrackLegacyBacklog"
        clearPersistentStorage(writeKey)
        // batches left in storage by a version that kept retrying them.
        val legacy = legacyBatches(writeKey, count = 2)

        // first launch with this version.
        val p = makePipeline(writeKey, clearStorage = false, prepare = { it.propertiesFile.put(it.fileIndexKey, 2) }) { 200 }
        p.pipeline.put(screen("Legacy Fresh"))
        p.pipeline.flush()

        assertEquals(listOf("Legacy Fresh"), p.sentNames("Legacy "), "Only this version's events are sent")
        legacy.forEach { assertFalse(it.exists(), "The legacy batch ${it.name} is deleted") }
        val storage = p.storage as StorageImpl
        assertEquals(2, storage.propertiesFile.get("${EventPipeline.LEGACY_CUSTOM_TRACK_BOUNDARY_KEY}.${storage.fileIndexKey}", -1))
    }

    @Test
    fun `an interrupted purge never deletes batches of this version`() {
        val writeKey = "customTrackInterruptedPurge"
        clearPersistentStorage(writeKey)
        val legacy = legacyBatches(writeKey, count = 1)

        // launch 1: an event is stored and its batch finished, then the app is killed before any flush.
        val launch1 = makePipeline(writeKey, clearStorage = false, prepare = { it.propertiesFile.put(it.fileIndexKey, 1) }) { 200 }
        launch1.pipeline.put(screen("Interrupted Pending"))
        runBlocking { launch1.storage.rollover() }
        launch1.pipeline.stop()

        // launch 2: the legacy batch is deleted, the batch of launch 1 is sent once.
        val launch2 = makePipeline(writeKey, clearStorage = false) { 200 }
        launch2.pipeline.flush()

        assertTrue(launch1.sentBodies.isEmpty())
        assertEquals(listOf("Interrupted Pending"), launch2.sentNames("Interrupted "))
        assertFalse(legacy.single().exists())
    }

    @Test
    fun `batches of another writeKey in the same directory are never taken`() {
        val writeKey = "customTrackOwnBatches"
        clearPersistentStorage(writeKey)
        val eventsDirectory = File("/tmp/analytics-kotlin/$writeKey/events").apply { mkdirs() }
        val otherBatch = File(eventsDirectory, "otherWriteKey-0")
        otherBatch.writeText("""{"batch":[{"type":"screen","name":"Other WriteKey"}],"sentAt":"2025-01-01T00:00:00.000Z","writeKey":"otherWriteKey"}""")

        val p = makePipeline(writeKey, clearStorage = false) { 200 }
        p.pipeline.put(screen("Own Batch"))
        p.pipeline.flush()

        assertEquals(listOf("Own Batch"), p.sentNames("Own "))
        assertTrue(p.sentNames("Other ").isEmpty(), "Another writeKey's batch is not sent")
        assertTrue(otherBatch.exists(), "Another writeKey's batch is not deleted")
    }

    @Test
    fun `batches of this launch are never taken for legacy`() {
        val p = makePipeline("customTrackNotLegacy") { 200 }

        p.pipeline.put(screen("ThisLaunch"))
        p.pipeline.flush()

        assertEquals(listOf("ThisLaunch"), p.sentNames("ThisLaunch"))
    }

    @Test
    fun `OkHttp never retries or redirects a customTrackUrl request`() {
        val scenarios: List<Pair<String, (HttpExchange, Int) -> Unit>> = listOf(
            "503 Retry-After: 0" to { exchange: HttpExchange, _: Int ->
                exchange.responseHeaders.add("Retry-After", "0")
                exchange.sendResponseHeaders(503, -1)
            },
            "408" to { exchange: HttpExchange, _: Int -> exchange.sendResponseHeaders(408, -1) },
            "307 redirect" to { exchange: HttpExchange, port: Int ->
                exchange.responseHeaders.add("Location", "http://127.0.0.1:$port/redirected")
                exchange.sendResponseHeaders(307, -1)
            },
            "connection dropped" to { exchange: HttpExchange, _: Int -> exchange.close() },
        )
        for ((name, respond) in scenarios) {
            val requests = AtomicInteger()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            val port = server.address.port
            server.createContext("/") { exchange ->
                requests.incrementAndGet()
                exchange.requestBody.readBytes()
                respond(exchange, port)
                exchange.close()
            }
            server.start()
            try {
                val connection = RequestFactory().uploadToCustomTrackUrl(URL("http://127.0.0.1:$port/track"), "writeKey")
                connection.outputStream.use { it.write("""{"type":"track","event":"Once"}""".toByteArray()) }
                try {
                    connection.responseCode
                } catch (e: IOException) {
                    // a failure, which must not be retried
                }
                connection.disconnect()
            } finally {
                server.stop(0)
            }
            assertEquals(1, requests.get(), "$name: the event must reach the network exactly once")
        }
    }

    @Test
    fun `unreadable batch is kept and sent once it can be read`() {
        val p = makePipeline("customTrackUnreadable") { 200 }
        every { p.storage.readAsStream(any()) } throws IOException("locked")

        p.pipeline.put(screen("Unreadable"))
        p.pipeline.flush()
        assertTrue(p.sentNames("Unreadable").isEmpty(), "Nothing is sent while the batch can't be read")
        assertEquals(1, p.pendingBatches().size, "A batch that was never attempted is not deleted")

        every { p.storage.readAsStream(any()) } answers { callOriginal() }
        p.pipeline.flush()
        p.pipeline.flush()
        assertEquals(listOf("Unreadable"), p.sentNames("Unreadable"), "Sent once as soon as it can be read")
        assertTrue(p.pendingBatches().isEmpty())
    }

    @Test
    fun `batch that cannot be removed is not sent`() {
        val p = makePipeline("customTrackNotRemovable") { 200 }
        every { p.storage.removeFile(any()) } returns true

        p.pipeline.put(screen("NotRemovable"))
        p.pipeline.flush()
        p.pipeline.flush()
        assertTrue(p.sentNames("NotRemovable").isEmpty(), "A batch still in storage would be sent again, so it is not sent")

        every { p.storage.removeFile(any()) } answers { callOriginal() }
        p.pipeline.flush()
        p.pipeline.flush()
        assertEquals(listOf("NotRemovable"), p.sentNames("NotRemovable"), "Sent once as soon as it can be removed")
        assertTrue(p.pendingBatches().isEmpty())
    }

    // Helpers

    /** [outcome] answers each POSTed event body with an HTTP status code, or throws (e.g. offline). */
    private fun makePipeline(
        writeKey: String,
        clearStorage: Boolean = true,
        prepare: (StorageImpl) -> Unit = {},
        outcome: (String) -> Int
    ): Pipeline {
        if (clearStorage) clearPersistentStorage(writeKey)
        usedWriteKeys.add(writeKey)

        val analytics = mockAnalytics(testScope, testDispatcher)
        every { analytics.configuration } returns Configuration(writeKey = writeKey)
        val storage = spyk(ConcreteStorageProvider.createStorage(analytics))
        prepare(storage as StorageImpl)
        every { analytics.storage } returns storage

        val sentBodies: MutableList<String> = Collections.synchronizedList(mutableListOf())
        val requestFactory = spyk(RequestFactory())
        every { requestFactory.uploadToCustomTrackUrl(any(), any()) } answers { fakeConnection(sentBodies, outcome) }
        val httpClient = spyk(HTTPClient(writeKey, requestFactory, URL(CUSTOM_TRACK_URL)))

        val pipeline = object : EventPipeline(analytics, "test", writeKey, listOf(CountBasedFlushPolicy(9999))) {
            override val httpClient: HTTPClient
                get() = httpClient
        }
        pipeline.start()
        return Pipeline(pipeline, storage, sentBodies, requestFactory)
    }

    private fun fakeConnection(sentBodies: MutableList<String>, outcome: (String) -> Int): HttpURLConnection {
        val body = ByteArrayOutputStream()
        val connection = mockk<HttpURLConnection>(relaxed = true)
        every { connection.outputStream } answers {
            object : java.io.OutputStream() {
                override fun write(b: Int) = body.write(b)
                override fun write(b: ByteArray, off: Int, len: Int) = body.write(b, off, len)
                override fun close() {
                    sentBodies.add(body.toString(Charsets.UTF_8.name()))
                }
            }
        }
        every { connection.responseCode } answers { outcome(body.toString(Charsets.UTF_8.name())) }
        every { connection.responseMessage } returns "scripted"
        every { connection.headerFields } returns mutableMapOf()
        return connection
    }

    /** Finished batches "<writeKey>-0" to "<writeKey>-(count-1)", as a previous version left them. */
    private fun legacyBatches(writeKey: String, count: Int): List<File> {
        val eventsDirectory = File("/tmp/analytics-kotlin/$writeKey/events").apply { mkdirs() }
        return (0 until count).map { index ->
            File(eventsDirectory, "$writeKey-$index").apply {
                writeText("""{"batch":[{"type":"screen","name":"Legacy Old $index"}],"sentAt":"2025-01-01T00:00:00.000Z","writeKey":"$writeKey"}""")
            }
        }
    }

    private fun screen(name: String) = ScreenEvent(name, "", emptyJsonObject).apply {
        messageId = "$name-id"
        anonymousId = "anonId"
        integrations = emptyJsonObject
        context = emptyJsonObject
        timestamp = Date(0).toInstant().toString()
    }

    /** Names of the events sent starting with [prefix], one entry per request. */
    private fun Pipeline.sentNames(prefix: String): List<String> = sentBodies.toList().mapNotNull { body ->
        Json.parseToJsonElement(body).jsonObject["name"]?.jsonPrimitive?.content?.takeIf { it.startsWith(prefix) }
    }

    /** Finished batches still in storage. */
    private fun Pipeline.pendingBatches(): List<String> = parseFilePaths(storage.read(Storage.Constants.Events))
}
