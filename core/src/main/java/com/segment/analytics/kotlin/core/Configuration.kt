package com.segment.analytics.kotlin.core

import com.segment.analytics.kotlin.core.Constants.DEFAULT_API_HOST
import com.segment.analytics.kotlin.core.Constants.DEFAULT_CDN_HOST
import com.segment.analytics.kotlin.core.platform.policies.FlushPolicy
import com.segment.analytics.kotlin.core.utilities.ConcreteStorageProvider
import kotlinx.coroutines.*
import sovran.kotlin.Store
import java.net.URL

/**
 * Configuration that analytics can use
 * @property writeKey the Segment writeKey
 * @property application defaults to `null`
 * @property storageProvider Provider for storage class, defaults to `ConcreteStorageProvider`
 * @property collectDeviceId collect deviceId, defaults to `false`
 * @property trackApplicationLifecycleEvents automatically send track for Lifecycle events (eg: Application Opened, Application Backgrounded, etc.), defaults to `false`
 * @property useLifecycleObserver enables the use of LifecycleObserver to track Application lifecycle events. Defaults to `false`.
 * @property trackDeepLinks automatically track [Deep link][https://developer.android.com/training/app-links/deep-linking] opened based on intents, defaults to `false`
 * @property flushAt count of events at which we flush events, defaults to `20`
 * @property flushInterval interval in seconds at which we flush events, defaults to `30 seconds`
 * @property defaultSettings Settings object that will be used as fallback in case of network failure, defaults to empty
 * @property autoAddSegmentDestination automatically add SegmentDestination plugin, defaults to `true`
 * @property apiHost set a default apiHost to which Segment sends events, defaults to `api.segment.io/v1`
 * @property customTrackUrl when set, used only for sending event batches; settings and all other requests use default Segment hosts. Invalid or empty URL is stored as `null`.
 */
data class Configuration(
    val writeKey: String,
    var application: Any? = null,
    var storageProvider: StorageProvider = ConcreteStorageProvider,
    var collectDeviceId: Boolean = false,
    var trackApplicationLifecycleEvents: Boolean = false,
    var useLifecycleObserver: Boolean = false,
    var trackDeepLinks: Boolean = false,
    var flushAt: Int = 20,
    var flushInterval: Int = 30,
    var flushPolicies: List<FlushPolicy> = emptyList<FlushPolicy>(),
    var defaultSettings: Settings? = null,
    var autoAddSegmentDestination: Boolean = true,
    var apiHost: String = DEFAULT_API_HOST,
    var cdnHost: String = DEFAULT_CDN_HOST,
    var requestFactory: RequestFactory = RequestFactory(),
    var errorHandler: ErrorHandler? = null,
    var customTrackUrl: URL? = null
) {
    fun isValid(): Boolean {
        return writeKey.isNotBlank() && application != null
    }

    /**
     * Use this URL only for sending event batches. Settings and all other requests keep using Segment's default hosts.
     * @param url full URL (e.g. `URL("https://your-server.com/ingest")`). Null, empty, or invalid host is stored as `null`.
     * @return this Configuration for chaining
     */
    fun customTrackUrl(url: URL?): Configuration {
        customTrackUrl = when {
            url == null -> null
            url.host.isNullOrBlank() -> null
            url.toExternalForm().trim().isEmpty() -> null
            else -> url
        }
        return this
    }

    /**
     * Use this URL only for sending event batches. Settings and all other requests keep using Segment's default hosts.
     * @param urlString full URL string (e.g. `"https://your-server.com/ingest"`). Empty or invalid is stored as `null`.
     * @return this Configuration for chaining
     */
    fun customTrackUrl(urlString: String?): Configuration {
        val trimmed = urlString?.trim().orEmpty()
        customTrackUrl = when {
            trimmed.isEmpty() -> null
            else -> try {
                val url = URL(trimmed)
                if (url.host.isNullOrBlank()) null else url
            } catch (_: Exception) {
                null
            }
        }
        return this
    }
}

interface CoroutineConfiguration {
    val store: Store

    val analyticsScope: CoroutineScope

    val analyticsDispatcher: CoroutineDispatcher

    val networkIODispatcher: CoroutineDispatcher

    val fileIODispatcher: CoroutineDispatcher
}

typealias ErrorHandler = (Throwable) -> Unit