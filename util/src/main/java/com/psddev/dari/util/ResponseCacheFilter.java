package com.psddev.dari.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ResponseCacheFilter extends AbstractFilter implements AbstractFilter.Auto {

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseCacheFilter.class);
    private static final Stats STATS = new Stats("Response Cache");
    // TODO: settings
    private static final int MEASURE_SECONDS = 30;
    private static final double HITS_PER_SECOND_THRESHOLD = 10;
    private static final Set<String> ALLOWED_REQUEST_HEADERS = new HashSet<>();
    private static final Collection<String> STRIP_RESPONSE_HEADERS = new HashSet<>();
    private static final Map<String, String> ADD_RESPONSE_HEADERS = new CompactMap<>();
    private static final String CACHE_KEY_REQUEST_ATTRIBUTE_NAME = "responseCache.cacheKey";

    static {
        // TODO: allow more headers (User-Agent, for starters)
        // but require the app to provide something that can be
        // used to build the cache key (e.g. key("Mozilla") -> "desktop")
        ALLOWED_REQUEST_HEADERS.add("Host");
        ALLOWED_REQUEST_HEADERS.add("X-Forwarded-Host");

        STRIP_RESPONSE_HEADERS.add("Set-Cookie");

        ADD_RESPONSE_HEADERS.put("X-Degraded-Experience", "true");
    }

    // TODO: expire these some number of seconds after last read
    private final ConcurrentHashMap<String, AtomicIntegerArray> hitCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> cumulativeHitCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> previousHitSeconds = new ConcurrentHashMap<>();
    // TODO: settings for all of this
    private final Cache<String, CachedResponse> responseOutputCache = CacheBuilder.newBuilder().concurrencyLevel(20).maximumSize(50).expireAfterWrite(60000L, TimeUnit.MILLISECONDS).build();
    private final AtomicIntegerArrayFactory atomicIntegerArrayFactory = new AtomicIntegerArrayFactory();
    private final AtomicLongFactory atomicLongFactory = new AtomicLongFactory();

    // --- AbstractFilter support ---
    @Override
    public void updateDependencies(Class<? extends AbstractFilter> filterClass, List<Class<? extends Filter>> dependencies) {
        if (PageContextFilter.class.isAssignableFrom(filterClass)) {
            if (isEnabled()) {
                dependencies.add(getClass());
            }
        }
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (shouldCache(request)) {
            doCache(request, response, chain);
        } else {
            chain.doFilter(request, response);
        }
        incrementHitCount(getCacheKey(request));
    }

    private boolean isEnabled() {
        // TODO: setting.
        return true;
    }

    // TODO: come up with a better measuring stick than hits per second.
    private boolean shouldCache(HttpServletRequest request) {
        String cacheKey = getCacheKey(request);
        double hitsPerSecond = getHitsPerSecond(cacheKey);
        Boolean forceParam = ObjectUtils.to(Boolean.class, request.getParameter("_responseCache"));

        return ((hitsPerSecond >= HITS_PER_SECOND_THRESHOLD ||
                Boolean.TRUE.equals(forceParam)) &&
                !Boolean.FALSE.equals(forceParam));
    }

    private void doCache(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Stats.Timer timer = STATS.startTimer();
        String cacheKey = getCacheKey(request);
        // TODO: if getOutput() is empty, next time don't try to cache it. . .
        request = new HeaderStrippingRequest(request, ALLOWED_REQUEST_HEADERS);
        CachedResponse cachedResponse = responseOutputCache.getIfPresent(cacheKey);

        if (cachedResponse != null) {
            cachedResponse.writeOutput(response);
            timer.stop("Cache Hit");
        } else {
            CapturingResponse capturingResponse = new CapturingResponse(response);
            try {
                chain.doFilter(request, capturingResponse);
                timer.stop("Cache Miss");
            } finally {
                Map<String, String> extraHeaders = new CompactMap<>();
                extraHeaders.putAll(ADD_RESPONSE_HEADERS);
                cachedResponse = CachedResponse.createInstanceOrNull(request, capturingResponse, STRIP_RESPONSE_HEADERS, extraHeaders);
                if (cachedResponse != null) {
                    cachedResponse.writeOutput(response);
                    responseOutputCache.put(cacheKey, cachedResponse);
                }
            }
        }
    }

    private void incrementHitCount(String key) {
        final AtomicIntegerArray hitCounter = hitCounters.computeIfAbsent(key, atomicIntegerArrayFactory);
        AtomicLong atomicPreviousHit = previousHitSeconds.computeIfAbsent(key, atomicLongFactory);
        long timestampSeconds = getTimestampSeconds();
        long previousTimestampSeconds = atomicPreviousHit.getAndSet(timestampSeconds);
        int secondsPointer = getSecondsPointer(timestampSeconds);

        if (previousTimestampSeconds != timestampSeconds) {
            for (long i = previousTimestampSeconds + 1; i <= timestampSeconds && i <= previousTimestampSeconds + MEASURE_SECONDS; i++) {
                int removeSecondsPointer = getSecondsPointer(i);
                hitCounter.set(removeSecondsPointer, 0);
            }
            int cumulativeHits = 0;
            for (int i = 0; i < MEASURE_SECONDS; i++) {
                cumulativeHits += hitCounter.get(i);
            }
            cumulativeHitCounters.put(key, cumulativeHits);
        }
        hitCounter.incrementAndGet(secondsPointer);
    }

    private int getHitCount(String key) {
        return cumulativeHitCounters.getOrDefault(key, 0);
    }

    private double getHitsPerSecond(String key) {
        return (double) getHitCount(key) / MEASURE_SECONDS;
    }

    private static String getCacheKey(HttpServletRequest request) {
        String cacheKey = (String) request.getAttribute(CACHE_KEY_REQUEST_ATTRIBUTE_NAME);
        if (cacheKey == null) {
            String queryString = request.getQueryString();
            cacheKey = JspUtils.getHostUrl(request) + request.getRequestURI() + (queryString != null ? '?' + queryString : "");
            request.setAttribute(CACHE_KEY_REQUEST_ATTRIBUTE_NAME, cacheKey);
        }
        return cacheKey;
    }

    // Static methods
    private static long getTimestampSeconds() {
        return (System.currentTimeMillis() / 1000);
    }

    private static int getSecondsPointer(long timestampSeconds) {
        return (int) (timestampSeconds % MEASURE_SECONDS);
    }

    // Helper classes
    private static class AtomicLongFactory implements Function<String, AtomicLong> {

        @Override
        public AtomicLong apply(String s) {
            return new AtomicLong();
        }
    }

    private static class AtomicIntegerArrayFactory implements Function<String, AtomicIntegerArray> {

        @Override
        public AtomicIntegerArray apply(String s) {
            return new AtomicIntegerArray(MEASURE_SECONDS);
        }
    }

    private static final class CachedResponse {

        private final Map<String, String> headers;
        private final String output;
        private final String contentType;

        public static CachedResponse createInstanceOrNull(HttpServletRequest request, CapturingResponse response, Iterable<String> stripHeaders, Map<String, String> addHeaders) {

            // Check method: GET only
            if (!"GET".equals(request.getMethod())) {
                return null;
            }

            // Check status: 0 or 200 only, otherwise null.
            int statusCode = response.getStatus();
            if (statusCode != 0 && statusCode != HttpServletResponse.SC_OK) {
                return null;
            }

            // lowercase headers for comparison
            Set<String> lowercaseStripHeaders = new HashSet<>();
            if (stripHeaders != null) {
                for (String header : stripHeaders) {
                    if (header != null) {
                        lowercaseStripHeaders.add(header.toLowerCase(Locale.ENGLISH));
                    }
                }
            }

            // copy headers that shouldn't be stripped or return null if Cache-Control: no-cache is set.
            Map<String, String> headers = new CompactMap<>();
            Object headerResponseObj = JspUtils.getHeaderResponse(request, response);
            HttpServletResponse headerResponse = (headerResponseObj instanceof HttpServletResponse ? (HttpServletResponse) headerResponseObj : response);
            for (String headerName : headerResponse.getHeaderNames()) {
                if (headerName != null) {
                    String headerNameLower = headerName.toLowerCase(Locale.ENGLISH);
                    if (!lowercaseStripHeaders.contains(headerNameLower)) {
                        String headerValue = response.getHeader(headerName);
                        headers.put(headerName, headerValue);
                        if ("cache-control".equals(headerNameLower) &&
                                "no-cache".equalsIgnoreCase(headerValue)) {
                            return null;
                        }
                    }
                }
            }
            if (addHeaders != null) {
                headers.putAll(addHeaders);
            }

            // copy response output or return null if it's empty
            String output = response.getOutput();
            if (output == null || output.isEmpty()) {
                return null;
            }

            return new CachedResponse(response.getContentType(), headers, output);
        }

        private CachedResponse(String contentType, Map<String, String> headers, String output) {
            this.headers = headers;
            this.output = output;
            this.contentType = contentType;
        }

        public void writeOutput(HttpServletResponse response) throws IOException {
            response.setContentType(contentType);
            for (Map.Entry<String, String> header : headers.entrySet()) {
                response.addHeader(header.getKey(), header.getValue());
            }
            response.getWriter().write(output);
        }
    }
}
