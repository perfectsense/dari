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
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class ResponseCacheFilter extends AbstractFilter implements AbstractFilter.Auto {

    // TODO: settings
    private static final Long TIMEOUT_THRESHOLD = 2000L;

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseCacheFilter.class);
    private static final Stats STATS = new Stats("Response Cache");
    private static final Set<String> ALLOWED_REQUEST_HEADERS = new HashSet<>();
    private static final Collection<String> STRIP_RESPONSE_HEADERS = new HashSet<>();
    private static final String CACHE_KEY_REQUEST_ATTRIBUTE_NAME = "responseCache.cacheKey";
    private static final String DEGRADED_HEADER_NAME = "X-Degraded";

    private static final Cache<String, CachedResponse> RESPONSE_OUTPUT_CACHE = CacheBuilder.newBuilder().
            concurrencyLevel(20).
            maximumSize(50).
            expireAfterWrite(60000L, TimeUnit.MILLISECONDS).
            build();

    private static final Cache<String, Double> TIMED_OUT_RESPONSES = CacheBuilder.newBuilder().
            concurrencyLevel(20).
            maximumSize(50).
            expireAfterWrite(60000L, TimeUnit.MILLISECONDS).
            build();

    static {
        // TODO: allow more headers (User-Agent, for starters)
        // but require the app to provide something that can be
        // used to build the cache key (e.g. key("Mozilla") -> "desktop")
        ALLOWED_REQUEST_HEADERS.add("Host");
        ALLOWED_REQUEST_HEADERS.add("X-Forwarded-Host");

        STRIP_RESPONSE_HEADERS.add("Set-Cookie");
    }

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

        String cacheKey = getCacheKey(request);
        Boolean forceParam = ObjectUtils.to(Boolean.class, request.getParameter("_responseCache"));
        Double responseTime = TIMED_OUT_RESPONSES.getIfPresent(cacheKey);
        boolean isTimedOut = responseTime != null;
        boolean cache = false;

        if (isTimedOut && !Boolean.FALSE.equals(forceParam)) {

            cache = true;
            response.setHeader(DEGRADED_HEADER_NAME, "response time " + Math.round(responseTime) + "ms");
        } else if (Boolean.TRUE.equals(forceParam)) {
            cache = true;
            response.setHeader(DEGRADED_HEADER_NAME, "forced");
        }

        if (cache) {
            doCache(request, response, chain);
        } else {
            timeResponse(request, response, chain);
        }
    }

    private void timeResponse(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {
        ResponseTimer.startTimer(getCacheKey(request), TIMEOUT_THRESHOLD);
        try {
            chain.doFilter(request, response);
        } finally {
            ResponseTimer.stopTimer();
        }
    }

    private boolean isEnabled() {
        // TODO: setting.
        return true;
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
        CachedResponse cachedResponse = RESPONSE_OUTPUT_CACHE.getIfPresent(cacheKey);

        if (cachedResponse != null) {
            cachedResponse.writeOutput(response);
            timer.stop("Cache Hit");
        } else {
            CapturingResponse capturingResponse = new CapturingResponse(response);
            try {
                timeResponse(request, capturingResponse, chain);
                timer.stop("Cache Miss");
            } finally {
                Map<String, String> extraHeaders = new CompactMap<>();
                cachedResponse = CachedResponse.createInstanceOrNull(request, capturingResponse, STRIP_RESPONSE_HEADERS);
                if (cachedResponse != null) {
                    cachedResponse.writeOutput(response);
                    RESPONSE_OUTPUT_CACHE.put(cacheKey, cachedResponse);
                }
            }
        }
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

    protected static final class ResponseTimer extends RepeatingTask {

        private static final Map<Long, TimedResponse> RUNNING_THREADS = new ConcurrentHashMap<>();

        @Override
        protected DateTime calculateRunTime(DateTime currentTime) {
            return currentTime;
        }

        @Override
        protected void doRepeatingTask(DateTime runTime) throws Exception {
            for (Map.Entry<Long, TimedResponse> entry : RUNNING_THREADS.entrySet()) {
                TimedResponse tr = entry.getValue();
                if (tr.isTimedOut()) {
                    reportTimeout(tr.responseCacheKey(), tr.elapsedTimeMillis(), tr.timeoutMillis());
                }
            }
        }

        private void reportTimeout(String responseCacheKey, Double elapsedTime, Long timeoutMillis) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Response Timeout - " + responseCacheKey + " : " + elapsedTime + " > " + timeoutMillis);
            }
            TIMED_OUT_RESPONSES.put(responseCacheKey, elapsedTime);
        }

        public static void startTimer(String cacheKey, long timeoutMillis) {
            RUNNING_THREADS.put(Thread.currentThread().getId(), new TimedResponse(cacheKey, timeoutMillis));
        }

        public static void stopTimer() {
            TimedResponse tr = RUNNING_THREADS.remove(Thread.currentThread().getId());
        }

        private static final class TimedResponse {

            private final String responseCacheKey;
            private final long startNanoTime;
            private final long timeoutMillis;

            public TimedResponse(String responseCacheKey, long timeoutMillis) {
                this.startNanoTime = System.nanoTime();
                this.responseCacheKey = responseCacheKey;
                this.timeoutMillis = timeoutMillis;
            }

            public String responseCacheKey() {
                return responseCacheKey;
            }

            public long startNanoTime() {
                return startNanoTime;
            }

            public long timeoutMillis() {
                return timeoutMillis;
            }

            public boolean isTimedOut() {
                return elapsedTimeMillis() > timeoutMillis;
            }

            public double elapsedTimeMillis() {
                return (System.nanoTime() - startNanoTime) / 1e6;
            }
        }
    }

    private static final class CachedResponse {

        private final Map<String, String> headers;
        private final String output;
        private final String contentType;

        public static CachedResponse createInstanceOrNull(HttpServletRequest request, CapturingResponse response, Iterable<String> stripHeaders) {

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
                response.setHeader(header.getKey(), header.getValue());
            }
            response.getWriter().write(output);
        }
    }
}
