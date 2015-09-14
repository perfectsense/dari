package com.psddev.dari.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.ServletResponse;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

/**
 * @deprecated 2015-07-23 - No replacement.
 */
@Deprecated
public class FrameFilter extends AbstractFilter {

    private static final String PARAMETER_PREFIX = "_frame.";
    public static final String PATH_PARAMETER = PARAMETER_PREFIX + "path";
    public static final String NAME_PARAMETER = PARAMETER_PREFIX + "name";
    public static final String LAZY_PARAMETER = PARAMETER_PREFIX + "lazy";

    private static final String ATTRIBUTE_PREFIX = FrameFilter.class.getName() + ".";
    private static final String DISCARDING_RESPONSE_ATTRIBUTE = ATTRIBUTE_PREFIX + "discardingResponse";
    private static final String DISCARDING_DONE_ATTRIBUTE = ATTRIBUTE_PREFIX + "discardingDone";
    public static final String BODY_ATTRIBUTE = ATTRIBUTE_PREFIX + "frameBody";

    private static Map<String, String> hashToPathMap = Collections.emptyMap();

    @Override
    protected void doInit() throws Exception {

        Map<String, String> map = new ConcurrentHashMap<String, String>();

        List<String> resourcePaths = new ArrayList<String>();
        collectResourcePaths(resourcePaths, "/");

        for (String path : resourcePaths) {
            map.put(StringUtils.hex(StringUtils.md5(path)), path);
        }

        hashToPathMap = map;
    }

    @Override
    protected void doRequest(
            final HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String path = decodePath(request.getParameter(PATH_PARAMETER));

        if (ObjectUtils.isBlank(path)) {
            chain.doFilter(request, response);

        } else {
            String name = request.getParameter(NAME_PARAMETER);

            DiscardingResponse discarding = new DiscardingResponse(response, path + (name != null ? "_" + name : ""));

            request.setAttribute(DISCARDING_RESPONSE_ATTRIBUTE, discarding);
            chain.doFilter(request, discarding);

            ServletResponse headerResponse = JspUtils.getHeaderResponse(request, response);

            if (headerResponse instanceof HeaderResponse) {
                String location = ((HeaderResponse) headerResponse).getHeader("Location");

                if (location != null) {
                    response.setHeader("Location",
                            StringUtils.addQueryParameters(location,
                                    PATH_PARAMETER, encodePath(path),
                                    NAME_PARAMETER, name));
                    return;
                }
            }

            String body = (String) request.getAttribute(BODY_ATTRIBUTE);

            if (body != null) {
                PrintWriter writer = response.getWriter();

                if (JspUtils.isAjaxRequest(request)
                        || "html".equals(request.getParameter("_result"))) {
                    response.setContentType("text/plain");
                    writer.write(body);

                } else {
                    response.setContentType("text/html");
                    writer.write("<div class=\"dari-frame-body\" name=\"");
                    writer.write(name);
                    writer.write("\">");
                    writer.write(StringUtils.escapeHtml(body));
                    writer.write("</div>");
                }
            }
        }
    }

    @Override
    protected void doError(
            final HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    @Override
    protected void doForward(
            final HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    @Override
    protected void doInclude(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Boolean.TRUE.equals(request.getAttribute(DISCARDING_DONE_ATTRIBUTE))) {
            return;
        }

        try {
            chain.doFilter(request, response);

        } finally {
            DiscardingResponse discarding = (DiscardingResponse) request.getAttribute(DISCARDING_RESPONSE_ATTRIBUTE);

            if (discarding != null
                    && JspUtils.getCurrentServletPath(request).equals(discarding.donePath)) {
                request.setAttribute(DISCARDING_DONE_ATTRIBUTE, Boolean.TRUE);
            }
        }
    }

    private static class DiscardingResponse extends HttpServletResponseWrapper {

        public final String donePath;

        private final ServletOutputStream output = new DiscardingOutputStream();
        private final PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));

        public DiscardingResponse(HttpServletResponse response, String donePath) {
            super(response);
            this.donePath = donePath;
        }

        @Override
        public ServletOutputStream getOutputStream() throws IOException {
            return output;
        }

        @Override
        public PrintWriter getWriter() throws IOException {
            return writer;
        }
    }

    private static final class DiscardingOutputStream extends ServletOutputStream {

        @Override
        public boolean isReady() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void write(int b) {
        }
    }

    /**
     * Recursively gathers all resource paths available to the current servlet
     * context in the given directory {@code path} and adds them to the provided
     * {@code allResourcePaths} list.
     *
     * @param allResourcePaths the list containing all the resource paths.
     * @param path the directory path to search for resources.
     */
    private void collectResourcePaths(List<String> allResourcePaths, String path) {

        if (path.endsWith("/")) {

            List<String> subPaths = new ArrayList<String>();

            Set<String> resourcePaths = getServletContext().getResourcePaths(path);
            if (resourcePaths != null) {
                subPaths.addAll(resourcePaths);
            }
            Collections.sort(subPaths);

            for (String subPath : subPaths) {
                collectResourcePaths(allResourcePaths, subPath);
            }

        } else {
            allResourcePaths.add(path);
        }
    }

    /**
     * @param path the path to hash.
     * @return a hex-encoded md5 hash string of the given {@code path}. If the
     *      hash is not present in the lookup map, the original path is returned
     *      instead.
     */
    public static String encodePath(String path) {

        if (path != null) {
            String hash = StringUtils.hex(StringUtils.md5(path));

            return hashToPathMap.containsKey(hash) ? hash : path;
        }

        return null;
    }

    /**
     * @param hash the md5 hash to look up.
     * @return the resource path that would generate the given md5 {@code hash}
     *      based on a hash look up table containing all known resource paths.
     *      If the hash can not be found, return it verbatim instead.
     */
    public static String decodePath(String hash) {

        if (hash != null) {
            String path = hashToPathMap.get(hash);

            return path != null ? path : hash;
        }

        return null;
    }
}
