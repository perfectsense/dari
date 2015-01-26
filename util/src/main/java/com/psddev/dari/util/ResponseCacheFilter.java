package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Caches the complete HTML response of any request that takes longer than {@code n} seconds to render.
 *
 * <br />
 * Required parameters to enable ResponseCache are:
 *
 * <pre>
 * {@code
 * dari/responseCache/enabled = true
 * dari/responseCache/storage = responseCache (name of configured StorageItem; must be a LocalStorageItem)
 * }
 * </pre>
 *
 * Optional parameters and their default values:
 * <pre>
 * {@code
 * dari/responseCache/timeoutSeconds = 2
 * dari/responseCache/expireSeconds = 300
 * dari/responseCache/maximumSize = 1000
 * }
 * </pre>
 *
 * Recommended settings for responseCache storage item:
 * <pre>
 * {@code
 * dari/storage/responseCache/class = LocalStorage.class
 * dari/storage/responseCache/rootPath = /tmp/responseCache/
 * dari/storage/responseCache/baseUrl = file:///tmp/responseCache/
 * }
 * </pre>
 *
 */
public class ResponseCacheFilter extends AbstractFilter implements AbstractFilter.Auto {

    public static final String SETTINGS_PREFIX = "dari/responseCache";
    public static final String ENABLED_SUB_SETTING = "enabled";
    public static final String STORAGE_SUB_SETTING = "storage";
    public static final String MAXIMUM_SIZE_SUB_SETTING = "maximumSize";
    public static final String TIMEOUT_SECONDS_SUB_SETTING = "timeoutSeconds";
    public static final String EXPIRE_SECONDS_SUB_SETTING = "expireSeconds";
    public static final String FORCE_RESPONSE_CACHE_PARAM = "_responseCache";

    private static final boolean DEFAULT_ENABLED = false;
    private static final int DEFAULT_TIMEOUT_SECONDS = 2;
    private static final int DEFAULT_EXPIRE_SECONDS = 300;
    private static final int DEFAULT_MAXIMUM_SIZE = 1000;
    private static final int CACHE_CONCURRENCY_LEVEL = 20;

    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseCacheFilter.class);
    private static final Stats STATS = new Stats("Response Cache");

    private static final Set<String> ALLOWED_REQUEST_HEADERS = new HashSet<>();
    private static final Collection<String> STRIP_RESPONSE_HEADERS = new HashSet<>();
    private static final String CACHE_KEY_REQUEST_ATTRIBUTE_NAME = "responseCache.cacheKey";
    private static final String X_RESPONSE_CACHE_REASON_HEADER_NAME = "X-Response-Cache-Reason";
    private static final String X_RESPONSE_CACHE_KEY_HEADER_NAME = "X-Response-Cache-Key";
    private static final String CACHED_FILE_EXTENSION = ".responseCache";

    private static final Cache<String, CachedResponse> RESPONSE_OUTPUT_CACHE = CacheBuilder.newBuilder().
            removalListener(new ResponseCacheRemovalListener()).
            concurrencyLevel(CACHE_CONCURRENCY_LEVEL).
            maximumSize(getMaximumSize()).
            expireAfterWrite(getExpireSeconds(), TimeUnit.SECONDS).
            build();

    private static final Cache<String, Long> TIMED_OUT_RESPONSES = CacheBuilder.newBuilder().
            concurrencyLevel(CACHE_CONCURRENCY_LEVEL).
            maximumSize(getMaximumSize()).
            expireAfterWrite(getExpireSeconds(), TimeUnit.SECONDS).
            build();

    static {
        ALLOWED_REQUEST_HEADERS.add("Host");
        ALLOWED_REQUEST_HEADERS.add("X-Forwarded-Host");

        STRIP_RESPONSE_HEADERS.add("Set-Cookie");
    }

    private final Collection<RequestNormalizer> requestNormalizers = new ArrayList<>();

    // --- AbstractFilter support ---
    @Override
    protected void doInit() throws Exception {
        super.doInit();
        requestNormalizers.clear();
        requestNormalizers.addAll(RequestNormalizer.Static.findGlobalInstances());
    }

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

        boolean cache = false;
        Boolean forceParam = ObjectUtils.to(Boolean.class, request.getParameter(FORCE_RESPONSE_CACHE_PARAM));

        if (Boolean.FALSE.equals(forceParam)) {
            invalidateCache(request);
        } else if (ableToCache(request)) {
            String cacheKey = generateCacheKey(request);
            if (Boolean.TRUE.equals(forceParam)) {
                cache = true;
                response.setHeader(X_RESPONSE_CACHE_REASON_HEADER_NAME, "forced");
                if (!Settings.isProduction()) {
                    response.setHeader(X_RESPONSE_CACHE_KEY_HEADER_NAME, cacheKey);
                }
            }
            Long responseTime = TIMED_OUT_RESPONSES.getIfPresent(cacheKey);
            if (responseTime != null) {
                cache = true;
                response.setHeader(X_RESPONSE_CACHE_REASON_HEADER_NAME, "response time " + responseTime + "ms");
                if (!Settings.isProduction()) {
                    response.setHeader(X_RESPONSE_CACHE_KEY_HEADER_NAME, cacheKey);
                }
            }
        }

        if (cache) {
            doCachedResponse(request, response, chain);

        } else {
            timeResponse(request, response, chain);
        }
    }

    /** Write the cached response for this request, optionally generating the response if it is not yet cached. */
    private void doCachedResponse(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Stats.Timer timer = STATS.startTimer();
        String cacheKey = generateCacheKey(request);

        Collection<String> allowedHeaders = getAccessedHeaders(request);
        request = new HeaderStrippingRequest(request, allowedHeaders);
        CachedResponse cachedResponse = RESPONSE_OUTPUT_CACHE.getIfPresent(cacheKey);

        if (cachedResponse != null) {
            try {
                if (cachedResponse.writeOutput(response)) {
                    timer.stop("Cache Hit");
                    return;
                } else {
                    LOGGER.warn("Cached response unable to write output . . .");
                    invalidateCache(request);
                }
            } catch (IOException e) {
                LOGGER.warn("Error when returning cached response", e);
                invalidateCache(request);
            }
        }
        CapturingResponse capturingResponse = new CapturingResponse(response);

        try {
            timeResponse(request, capturingResponse, chain);
            timer.stop("Cache Miss");

        } finally {
            capturingResponse.writeOutput();
            Map<String, String> extraHeaders = new CompactMap<>();
            try {
                cachedResponse = CachedResponse.createInstanceOrNull(request, capturingResponse, STRIP_RESPONSE_HEADERS);
                if (cachedResponse != null) {
                    RESPONSE_OUTPUT_CACHE.put(cacheKey, cachedResponse);
                } else {
                    // Unable to cache. Remove this from the list of timed out responses.
                    TIMED_OUT_RESPONSES.invalidate(cacheKey);
                }

            } catch (IOException e) {
                LOGGER.error("Error when writing to StorageItem " + getStorage(), e);
            }
        }
    }

    /** Wraps the actual response in calls to ResponseTimer to calculate the execution time. */
    private void timeResponse(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {
        ResponseTimer.startTimer(generateCacheKey(request), getTimeoutSeconds());
        try {
            chain.doFilter(request, response);

        } finally {
            ResponseTimer.stopTimer();
        }
    }

    /** Invalidate cached response for the given request. */
    private void invalidateCache(HttpServletRequest request) {
        String cacheKey = generateCacheKey(request);
        RESPONSE_OUTPUT_CACHE.invalidate(cacheKey);
        TIMED_OUT_RESPONSES.invalidate(cacheKey);
    }

    private boolean ableToCache(HttpServletRequest request) {
        if (accessedCookie(request) || !"GET".equals(request.getMethod())) {
            return false;
        }
        String cacheControlHeader = request.getHeader("Cache-Control");
        if (cacheControlHeader != null) {
            if (cacheControlHeader.toLowerCase(Locale.ENGLISH).contains("no-cache")) {
                return false;
            }
        }
        return true;
    }

    private Map<String, Collection<String>> getNormalizedHeader(HttpServletRequest request) {
        ensureRequestNormalizersExecuteOnce(request);
        return RequestNormalizer.Static.getNormalizedHeaders(request);
    }

    private Collection<String> getAccessedHeaders(HttpServletRequest request) {
        ensureRequestNormalizersExecuteOnce(request);
        return RequestNormalizer.Static.getAccessedHeaderNames(request);
    }

    private boolean accessedCookie(HttpServletRequest request) {
        ensureRequestNormalizersExecuteOnce(request);
        return RequestNormalizer.Static.accessedCookies(request);
    }

    private void ensureRequestNormalizersExecuteOnce(HttpServletRequest request) {
        if (!RequestNormalizer.Static.hasExecuted(request)) {
            RequestNormalizer.NormalizingRequest req = new RequestNormalizer.NormalizingRequest(request);
            for (RequestNormalizer requestNormalizer : requestNormalizers) {
                requestNormalizer.normalizeRequest(req);
            }
        }
    }

    /**
     * Generates a string suitable for a cache key based on the request.
     * Currently returns the absolute URL including scheme and querystring.
     * */
    private String generateCacheKey(HttpServletRequest request) {
        String cacheKey = (String) request.getAttribute(CACHE_KEY_REQUEST_ATTRIBUTE_NAME);
        if (cacheKey == null) {
            StringBuilder cacheKeyBuilder = new StringBuilder();
            cacheKeyBuilder.append(JspUtils.getHostUrl(request));
            cacheKeyBuilder.append(request.getRequestURI());
            String queryString = request.getQueryString();
            if (queryString != null && !queryString.isEmpty()) {
                Matcher matcher = StringUtils.getPattern(FORCE_RESPONSE_CACHE_PARAM + "=([Ff][Aa][Ll][Ss][Ee]|[Tt][Rr][Uu][Ee])").matcher(queryString);
                queryString = matcher.replaceAll("");
                if (!queryString.isEmpty()) {
                    cacheKeyBuilder.append('?');
                    cacheKeyBuilder.append(queryString);
                }
            }
            Map<String, Collection<String>> normalizedHeaders = getNormalizedHeader(request);
            if (!normalizedHeaders.isEmpty()) {
                Map<String, Collection<String>> sortedNormalizedHeaders = new TreeMap<>(normalizedHeaders);
                for (Map.Entry<String, Collection<String>> header : sortedNormalizedHeaders.entrySet()) {
                    String headerName = header.getKey();
                    Iterable<String> sortedHeaderValues = new TreeSet<>(header.getValue());
                    for (String headerValue : sortedHeaderValues) {
                        cacheKeyBuilder.append(';');
                        cacheKeyBuilder.append(headerName);
                        cacheKeyBuilder.append('=');
                        cacheKeyBuilder.append(String.valueOf(headerValue).replace('\n', ' ').replace('\r', ' '));
                    }
                }
            }
            cacheKey = cacheKeyBuilder.toString();
            request.setAttribute(CACHE_KEY_REQUEST_ATTRIBUTE_NAME, cacheKey);
        }
        return cacheKey;
    }

    private static boolean isEnabled() {
        return Settings.getOrDefault(Boolean.class, SETTINGS_PREFIX + '/' + ENABLED_SUB_SETTING, DEFAULT_ENABLED);
    }

    private static String getStorage() {
        return Settings.getOrError(String.class, SETTINGS_PREFIX + '/' + STORAGE_SUB_SETTING, SETTINGS_PREFIX + '/' + STORAGE_SUB_SETTING + " must be specified in settings!");
    }

    private static int getTimeoutSeconds() {
        return Settings.getOrDefault(int.class, SETTINGS_PREFIX + '/' + TIMEOUT_SECONDS_SUB_SETTING, DEFAULT_TIMEOUT_SECONDS);
    }

    private static int getExpireSeconds() {
        return Settings.getOrDefault(int.class, SETTINGS_PREFIX + '/' + EXPIRE_SECONDS_SUB_SETTING, DEFAULT_EXPIRE_SECONDS);
    }

    private static int getMaximumSize() {
        return Settings.getOrDefault(int.class, SETTINGS_PREFIX + '/' + MAXIMUM_SIZE_SUB_SETTING, DEFAULT_MAXIMUM_SIZE);
    }

    private static StorageItem createStorageItem() {
        return StorageItem.Static.createIn(getStorage());
    }

    /** Calculates response time with a precision of approximately 1 second. */
    protected static final class ResponseTimer extends RepeatingTask {

        private static final Map<Long, TimedResponse> RUNNING_THREADS = new ConcurrentHashMap<>();

        @Override
        protected DateTime calculateRunTime(DateTime currentTime) {
            return currentTime;
        }

        @Override
        protected void doRepeatingTask(DateTime runTime) throws Exception {
            if (!isEnabled()) {
                return;
            }
            setProgressTotal(RUNNING_THREADS.size());
            for (Map.Entry<Long, TimedResponse> entry : RUNNING_THREADS.entrySet()) {
                setProgressIndex(getProgressIndex() + 1);
                TimedResponse tr = entry.getValue();
                if (tr.isTimedOut()) {
                    reportTimeout(tr.responseCacheKey(), tr.elapsedTimeMillis(), tr.timeoutMillis());
                }
            }
        }

        private static void reportTimeout(String responseCacheKey, Long elapsedTime, Long timeoutSeconds) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Response Timeout - " + responseCacheKey + " : " + elapsedTime + "ms > " + timeoutSeconds + 's');
            }
            TIMED_OUT_RESPONSES.put(responseCacheKey, elapsedTime);
        }

        public static void startTimer(String cacheKey, int timeoutSeconds) {
            RUNNING_THREADS.put(Thread.currentThread().getId(), new TimedResponse(cacheKey, timeoutSeconds));
        }

        public static void stopTimer() {
            RUNNING_THREADS.remove(Thread.currentThread().getId());
        }

        private static final class TimedResponse {

            private final String responseCacheKey;
            private final long startNanoTime;
            private final long timeoutSeconds;

            public TimedResponse(String responseCacheKey, long timeoutSeconds) {
                this.startNanoTime = System.nanoTime();
                this.responseCacheKey = responseCacheKey;
                this.timeoutSeconds = timeoutSeconds;
            }

            public String responseCacheKey() {
                return responseCacheKey;
            }

            public long startNanoTime() {
                return startNanoTime;
            }

            public long timeoutMillis() {
                return timeoutSeconds;
            }

            public boolean isTimedOut() {
                return elapsedTimeMillis() > (timeoutSeconds * 1000L);
            }

            public long elapsedTimeMillis() {
                return (System.nanoTime() - startNanoTime) / 1000000L;
            }
        }
    }

    /** Contains a reference to the StorageItem with the cached content. This class is not a real HttpServletResponse. */
    private static final class CachedResponse {

        private final Map<String, Collection<String>> headers;
        private final String contentType;
        private final StorageItem storageItem;

        /** Creates the CachedResponse, including the underlying StorageItem */
        public static CachedResponse createInstanceOrNull(HttpServletRequest request, CapturingResponse response, Iterable<String> stripHeaders) throws IOException {
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

            // copy headers that shouldn't be stripped and return null if Cache-Control: no-cache is set.
            Map<String, Collection<String>> headers = new CompactMap<>();
            Object headerResponseObj = JspUtils.getHeaderResponse(request, response);
            HttpServletResponse headerResponse = (headerResponseObj instanceof HttpServletResponse ? (HttpServletResponse) headerResponseObj : response);
            for (String headerName : headerResponse.getHeaderNames()) {
                if (headerName != null) {
                    String headerNameLower = headerName.toLowerCase(Locale.ENGLISH);
                    if (!lowercaseStripHeaders.contains(headerNameLower)) {
                        Collection<String> headerValues = response.getHeaders(headerName);
                        if (headerValues != null) {
                            headers.put(headerName, headerValues);
                            for (String headerValue : headerValues) {
                                if ("cache-control".equals(headerNameLower) && headerValue != null) {
                                    String headerValueLower = headerValue.toLowerCase(Locale.ENGLISH);
                                    if (headerValueLower.contains("no-cache") || headerValueLower.contains("private")) {
                                        return null;
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // copy response output or return null if it's empty
            String output = response.getOutput();
            if (output == null || output.isEmpty()) {
                return null;
            }

            StorageItem storageItem = createStorageItem();
            if (!(storageItem instanceof LocalStorageItem)) {
                return null;
            }
            if (storageItem.getPath() == null) {
                storageItem.setPath(UuidUtils.createSequentialUuid() + CACHED_FILE_EXTENSION);
            }
            byte[] data = output.getBytes();
            storageItem.getMetadata().put("length", data.length);
            storageItem.setData(new ByteArrayInputStream(data));
            storageItem.save();

            return new CachedResponse(response.getContentType(), headers, storageItem);
        }

        private CachedResponse(String contentType, Map<String, Collection<String>> headers, StorageItem storageItem) {
            this.headers = headers;
            this.storageItem = storageItem;
            this.contentType = contentType;
        }

        /** @return never null. */
        public StorageItem getStorageItem() {
            return storageItem;
        }

        /** Write cached headers and output to the response. */
        public boolean writeOutput(HttpServletResponse response) throws IOException {
            if (storageItem.isInStorage()) {
                response.setContentType(contentType);
                for (Map.Entry<String, Collection<String>> header : headers.entrySet()) {
                    String headerName = header.getKey();
                    if (!response.containsHeader(headerName)) {
                        for (String headerValue : header.getValue()) {
                            response.addHeader(headerName, headerValue);
                        }
                    }
                }
                OutputStream responseOut = response.getOutputStream();
                InputStream cachedData;
                synchronized (storageItem) {
                    // Force StorageItem to re-create the InputStream every hit.
                    cachedData = storageItem.getData();
                    storageItem.setData(null);
                }
                Integer length;
                if ((length = (Integer) storageItem.getMetadata().get("length")) != null) {
                    response.setContentLength(length);
                }
                IoUtils.copy(cachedData, responseOut);
                responseOut.flush();
                return true;
            } else {
                return false;
            }
        }
    }

    /** Removes underlying file when cached item expires. */
    private static class ResponseCacheRemovalListener implements RemovalListener<String, CachedResponse> {

        @Override
        public void onRemoval(RemovalNotification<String, CachedResponse> notification) {
            CachedResponse cachedResponse = notification.getValue();
            StorageItem item = cachedResponse != null ? cachedResponse.getStorageItem() : null;
            if (item != null && item.getPath() != null) {
                // We could find the path to the file a different way, but the file:// requirement is to short-circuit things like ImageResizeStorageItemListener.
                if (item instanceof LocalStorageItem && item.getPublicUrl().startsWith("file://")) {
                    try {
                        File cachedFile = new File(new URL(item.getPublicUrl()).toURI());
                        if (cachedFile.exists() && !cachedFile.delete()) {
                            LOGGER.error("Unable to delete cached response file: " + cachedFile.getAbsolutePath());
                        }
                    } catch (URISyntaxException | MalformedURLException e) {
                        LOGGER.error("Unable to delete cached response file: " + item.getPublicUrl(), e);
                    }
                } else {
                    LOGGER.error("Unable to resolve cached response file for deletion: " + item.getPublicUrl() + ". It must be a LocalStorageItem with a baseUrl beginning with 'file://'.");
                }
            }
        }
    }

    /** Finds files in StorageItem's rootPath that aren't referenced by anything in the cache and wipes them out. */
    protected static class ResponseCacheCleanupTask extends RepeatingTask {

        @Override
        protected DateTime calculateRunTime(DateTime currentTime) {
            return everyHour(currentTime);
        }

        @Override
        protected void doRepeatingTask(DateTime runTime) throws Exception {
            if (!isEnabled()) {
                return;
            }
            StorageItem item = createStorageItem();
            if (!(item instanceof LocalStorageItem) || !item.getPublicUrl().startsWith("file://")) {
                throw new IllegalStateException("Unable to resolve cached response files for deletion during cleanup. Configured storage \"" + getStorage() + "\" must be a LocalStorageItem with a baseUrl beginning with 'file://'.");
            }
            String rootPath = ((LocalStorageItem) item).getRootPath();
            File[] directoryListing = new File(rootPath).listFiles();
            if (directoryListing == null) {
                throw new IllegalStateException("Error during cleanup: " + rootPath + " is not a directory.");
            }

            Set<String> knownFiles = allKnownFilenames();
            setProgressTotal(directoryListing.length);
            for (File cachedFile : directoryListing) {
                setProgressIndex(getProgressIndex() + 1);
                String filename = cachedFile.getName();
                if (filename.endsWith(CACHED_FILE_EXTENSION) && !knownFiles.contains(filename)) {
                    // This file ends with .responseCache and is unknown to the in-memory cache
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Deleting file: " + cachedFile.getName());
                    }
                    if (cachedFile.exists() && !cachedFile.delete()) {
                        LOGGER.warn("Unable to delete cached response file: " + cachedFile.getName());
                    }
                }
            }
        }

        private static Set<String> allKnownFilenames() {
            Set<String> knownFiles = new HashSet<>();
            for (CachedResponse cachedResponse : RESPONSE_OUTPUT_CACHE.asMap().values()) {
                knownFiles.add(cachedResponse.getStorageItem().getPath());
            }
            return knownFiles;
        }
    }

    /**
     *
     * The default request normalizer passes through the ALLOWED_REQUEST_HEADERS untouched.
     */
    protected static class DefaultRequestNormalizer implements RequestNormalizer.Global {

        @Override
        public void normalizeRequest(NormalizingRequest request) {
            for (String headerName : ALLOWED_REQUEST_HEADERS) {
                request.setNormalizedHeaders(headerName, request.getHeaders(headerName));
            }
        }
    }
}
