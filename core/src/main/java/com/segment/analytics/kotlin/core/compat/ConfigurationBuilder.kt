package com.segment.analytics.kotlin.core.compat

import com.segment.analytics.kotlin.core.Configuration
import com.segment.analytics.kotlin.core.RequestFactory
import java.net.URL

/**
 * This class serves as a helper class for Java compatibility, which makes the
 * @see Configuration buildable through a builder pattern.
 * It's strongly discouraged to use this builder in a Kotlin based project, since
 * the optional parameters is the way to go in Kotlin.
 */
class ConfigurationBuilder (writeKey: String) {

    private val configuration: Configuration = Configuration(writeKey)

    fun setApplication(application: Any?) = apply { configuration.application = application }

    fun setCollectDeviceId(collectDeviceId: Boolean) = apply { configuration.collectDeviceId = collectDeviceId }

    fun setTrackApplicationLifecycleEvents(trackApplicationLifecycleEvents: Boolean) = apply { configuration.trackApplicationLifecycleEvents = trackApplicationLifecycleEvents }

    fun setUseLifecycleObserver(useLifecycleObserver: Boolean) = apply { configuration.useLifecycleObserver = useLifecycleObserver }

    fun setTrackDeepLinks(trackDeepLinks: Boolean) = apply { configuration.trackDeepLinks = trackDeepLinks }

    fun setFlushAt(flushAt: Int) = apply { configuration.flushAt = flushAt }

    fun setFlushInterval(flushInterval: Int) = apply { configuration.flushInterval = flushInterval }

    fun setAutoAddSegmentDestination(autoAddSegmentDestination: Boolean) = apply { configuration.autoAddSegmentDestination = autoAddSegmentDestination}

    fun setApiHost(apiHost: String) = apply { configuration.apiHost = apiHost}

    fun setCdnHost(cdnHost: String) = apply { configuration.cdnHost = cdnHost}

    fun setRequestFactory(requestFactory: RequestFactory) = apply { configuration.requestFactory = requestFactory }

    /**
     * Use this URL only for sending event batches. Settings and all other requests keep using Segment's default hosts.
     * @param url full URL, or null. Empty or invalid host is stored as null.
     */
    fun setCustomTrackUrl(url: URL?) = apply { configuration.customTrackUrl(url) }

    /**
     * Use this URL only for sending event batches. Settings and all other requests keep using Segment's default hosts.
     * @param urlString full URL string. Empty or invalid is stored as null.
     */
    fun setCustomTrackUrl(urlString: String?) = apply { configuration.customTrackUrl(urlString) }

    fun build() = configuration
}