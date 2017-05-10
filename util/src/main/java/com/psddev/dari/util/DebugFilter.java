package com.psddev.dari.util;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.ldap.LdapContext;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Filter that provides various debugging capabilities, such as a debugging
 * interface that serve {@link DebugServlet}s.
 *
 * <p>This filter depends on:</p>
 *
 * <ul>
 * <li>{@link SourceFilter}</li>
 * <li>{@link ResourceFilter}</li>
 * </ul>
 */
public class DebugFilter extends AbstractFilter {

    /**
     * Default intercept path where all {@link DebugServlet}s will be made
     * accessible.
     *
     * @see #getPath(HttpServletRequest, Class, Object...)
     */
    public static final String DEFAULT_INTERCEPT_PATH = "/_debug/";

    /**
     * Setting name for specifying the intercept path where all
     * {@link DebugServlet}s will be made accessible.
     *
     * @see #getPath(HttpServletRequest, Class, Object...)
     */
    public static final String INTERCEPT_PATH_SETTING = "dari/debugFilterInterceptPath";

    /**
     * Setting name for specifying the username that will be granted access
     * to the debugging interface.
     *
     * @see #authenticate(HttpServletRequest, HttpServletResponse)
     */
    public static final String USERNAME_SETTING = "dari/debugUsername";

    /**
     * Setting name for specifying the password that will grant access to the
     * debugging interface.
     *
     * @see #authenticate(HttpServletRequest, HttpServletResponse)
     */
    public static final String PASSWORD_SETTING = "dari/debugPassword";

    /**
     * Setting name for specifying the realm to display during debugging
     * interface authentication prompt.
     *
     * @see #authenticate(HttpServletRequest, HttpServletResponse)
     */
    public static final String REALM_SETTING = "dari/debugRealm";

    /**
     * HTTP request parameter name for putting the application into debugging
     * mode.
     *
     * @see Settings#isDebug()
     */
    public static final String DEBUG_PARAMETER = "_debug";

    /**
     * HTTP request parameter name for putting the application into production
     * mode.
     *
     * @see Settings#isProduction()
     */
    public static final String PRODUCTION_PARAMETER = "_prod";

    /**
     * Path within the web application where all debugging JSPs reside.
     */
    public static final String WEB_INF_DEBUG = "/WEB-INF/_debug/";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugFilter.class);
    private static final Pattern NO_FILE_PATTERN = Pattern.compile("File &quot;(.*)&quot; not found");

    /**
     * Returns the path to the debugging servlet of the given {@code type}
     * with the given query {@code parameters}.
     *
     * @param request Nonnull.
     * @param type Nonnull.
     * @param parameters Nullable.
     * @return Nonnull.
     * @see #DEFAULT_INTERCEPT_PATH
     * @see #INTERCEPT_PATH_SETTING
     */
    public static String getPath(
            HttpServletRequest request,
            Class<? extends Servlet> type,
            Object... parameters) {

        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(type);

        DebugServletWrapper wrapper = DebugServletWrapper.INSTANCES
                .getUnchecked(request.getServletContext())
                .get(type);

        if (wrapper == null) {
            throw new IllegalStateException(String.format(
                    "[%s] debug servlet not available!",
                    type.getName()));
        }

        return request.getContextPath()
                + getInterceptPath()
                + StringUtils.addQueryParameters(
                        StringUtils.removeEnd(wrapper.getPaths().get(0), "/"),
                        parameters);
    }

    private static String getInterceptPath() {
        return StringUtils.ensureSurrounding(
                Settings.getOrDefault(String.class, INTERCEPT_PATH_SETTING, DEFAULT_INTERCEPT_PATH),
                "/");
    }

    /**
     * Writes a pretty message about the given {@code error}.
     *
     * @param request Nonnull.
     * @param response Nonnull.
     * @param error Nonnull.
     */
    public static void writeError(
            HttpServletRequest request,
            HttpServletResponse response,
            Throwable error)
            throws IOException {

        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(response);
        Preconditions.checkNotNull(error);

        @SuppressWarnings("resource")
        WebPageContext page = new WebPageContext((ServletContext) null, request, response);
        String id = page.createId();
        String servletPath = JspUtils.getCurrentServletPath(request);

        response.setContentType("text/html");
        page.putAllStandardDefaults();

        page.writeStart("div", "id", id);
            page.writeStart("pre", "class", "alert alert-error");
                String noFile = null;

                if (error instanceof ServletException) {
                    String message = error.getMessage();

                    if (message != null) {
                        Matcher noFileMatcher = NO_FILE_PATTERN.matcher(message);

                        if (noFileMatcher.matches()) {
                            noFile = noFileMatcher.group(1);
                        }
                    }
                }

                if (noFile != null) {
                    page.writeStart("strong");
                        page.writeHtml(servletPath);
                    page.writeEnd();
                    page.writeHtml(" doesn't exist!");

                } else {
                    page.writeHtml("Can't render ");
                    page.writeStart("a",
                            "target", "_blank",
                            "href", getPath(
                                    request,
                                    CodeDebugServlet.class,
                                    "action", "edit",
                                    "type", "JSP",
                                    "servletPath", servletPath));
                        page.writeHtml(servletPath);
                    page.writeEnd();
                    page.writeHtml("!");
                }

                page.writeHtml("\n\n");
                page.writeObject(error);

                List<String> paramNames = page.paramNamesList();

                if (!ObjectUtils.isBlank(paramNames)) {
                    page.writeHtml("Parameters:\n");

                    for (String name : paramNames) {
                        for (String value : page.params(String.class, name)) {
                            page.writeHtml(name);
                            page.writeHtml('=');
                            page.writeHtml(value);
                            page.writeHtml('\n');
                        }
                    }

                    page.writeHtml('\n');
                }
            page.writeEnd();
        page.writeEnd();

        page.writeStart("script", "type", "text/javascript");
            page.writeRaw("(function() {");
                page.writeRaw("var f = document.createElement('iframe');");
                page.writeRaw("f.frameBorder = '0';");
                page.writeRaw("var fs = f.style;");
                page.writeRaw("fs.background = 'transparent';");
                page.writeRaw("fs.border = 'none';");
                page.writeRaw("fs.overflow = 'hidden';");
                page.writeRaw("fs.width = '100%';");
                page.writeRaw("f.src = '");
                page.writeRaw(page.js(JspUtils.getAbsolutePath(page.getRequest(), "/_resource/dari/alert.html", "id", id)));
                page.writeRaw("';");
                page.writeRaw("var a = document.getElementById('");
                page.writeRaw(id);
                page.writeRaw("');");
                page.writeRaw("a.parentNode.insertBefore(f, a.nextSibling);");
            page.writeRaw("})();");
        page.writeEnd();
    }

    /**
     * Makes sure that the given {@code request} is authenticated for access
     * to the debugging interface.
     *
     * <p>When this method returns {@code false}, it sets the appropriate
     * HTTP headers in the given {@code response} to require authentication,
     * and the caller should stop processing:</p>
     *
     * <blockquote><pre>
     *     if (!authenticate(request, response)) {
     *         return;
     *     }
     * </pre></blockquote>
     *
     * @param request Nonnull.
     * @param response Nonnull.
     * @return {@code true} if authenticated.
     * @see #USERNAME_SETTING
     * @see #PASSWORD_SETTING
     * @see #REALM_SETTING
     */
    public static boolean authenticate(HttpServletRequest request, HttpServletResponse response) {
        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(response);

        LdapContext context = LdapUtils.createContext();
        String[] credentials = JspUtils.getBasicCredentials(request);

        if (context != null
                && credentials != null
                && LdapUtils.authenticate(context, credentials[0], credentials[1])) {

            return true;
        }

        String username = ObjectUtils.firstNonNull(
                Settings.get(String.class, USERNAME_SETTING),
                Settings.get(String.class, "servlet/debugUsername"));

        String password = ObjectUtils.firstNonNull(
                Settings.get(String.class, PASSWORD_SETTING),
                Settings.get(String.class, "servlet/debugPassword"));

        if (context == null
                && (StringUtils.isBlank(username)
                || StringUtils.isBlank(password))) {

            if (!Settings.isProduction()) {
                return true;
            }

        } else if (credentials != null
                && credentials[0].equals(username)
                && credentials[1].equals(password)) {

            return true;
        }

        String realm = ObjectUtils.firstNonNull(
                Settings.get(String.class, REALM_SETTING),
                Settings.get(String.class, "servlet/debugRealm"),
                "Dari Debugging Interface");

        JspUtils.setBasicAuthenticationHeader(response, realm);

        return false;
    }

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<>();

        dependencies.add(DebugSettingsOverrideFilter.class);
        dependencies.add(SourceFilter.class);
        dependencies.add(ResourceFilter.class);

        return dependencies;
    }

    @Override
    protected void doDestroy() {
        DebugServletWrapper.INSTANCES.invalidate(getServletContext());
    }

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Settings.isProduction()) {
            super.doDispatch(request, response, chain);

        } else {
            CapturingResponse capturing = new CapturingResponse(response);

            try {
                super.doDispatch(request, capturing, chain);
                capturing.writeOutput();

            } catch (Exception error) {

                // If the request is a form post and there's been no output
                // so far, it's probably form processing code. Custom error
                // message here will most likely not reach the user, so don't
                // substitute.
                if (JspUtils.isFormPost(request)
                        && StringUtils.isBlank(capturing.getOutput())) {

                    throw error;

                // Discard the output so far and write the error instead.
                } else {
                    writeError(request, response, error);
                }
            }
        }
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String interceptPath = getInterceptPath();
        String servletPath = request.getServletPath();

        if (StringUtils.removeEnd(interceptPath, "/").equals(servletPath)) {
            response.sendRedirect(request.getContextPath() + servletPath + "/");
            return;
        }

        if (!servletPath.startsWith(interceptPath)) {
            chain.doFilter(request, response);
            return;
        }

        if (!authenticate(request, response)) {
            return;
        }

        String debugPath = servletPath.substring(interceptPath.length());
        ServletContext context = getServletContext();
        Map<Class<? extends Servlet>, DebugServletWrapper> wrappers = DebugServletWrapper.INSTANCES.getUnchecked(context);

        if (!StringUtils.isBlank(debugPath)) {
            String debugPathSlash = StringUtils.ensureEnd(debugPath, "/");

            for (DebugServletWrapper wrapper : wrappers.values()) {
                for (String path : wrapper.getPaths()) {
                    if (debugPathSlash.startsWith(path)) {
                        int pathLength = path.length();

                        wrapper.service(
                                request,
                                debugPath.length() - pathLength > 1
                                        ? debugPath.substring(pathLength)
                                        : null,
                                response);

                        return;
                    }
                }
            }

            String debugJsp = WEB_INF_DEBUG + debugPath;

            try {
                if (context.getResource(debugJsp) != null) {
                    JspUtils.forward(request, response, debugJsp);
                    return;
                }

            } catch (MalformedURLException error) {
                chain.doFilter(request, response);
                return;
            }
        }

        new PageWriter(context, request, response) { {
            startPage();
                writeStart("div", "class", "row-fluid");

                    writeStart("div", "class", "span3");
                        writeStart("h2").writeHtml("Standard Tools").writeEnd();

                        writeStart("ul");
                            for (Iterator<DebugServletWrapper> i = wrappers.values().stream()
                                    .sorted(Comparator.comparing(DebugServletWrapper::getName))
                                    .iterator(); i.hasNext();) {

                                DebugServletWrapper wrapper = i.next();

                                writeStart("li");
                                    writeStart("a", "href", wrapper.getPaths().get(0));
                                        writeHtml(wrapper.getName());
                                    writeEnd();
                                writeEnd();
                            }
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "span3");
                        writeStart("h2").writeHtml("Custom Tools").writeEnd();

                        Set<String> debugResources = context.getResourcePaths(WEB_INF_DEBUG);

                        if (debugResources != null && !debugResources.isEmpty()) {
                            Set<String> debugJsps = debugResources.stream()
                                    .filter(r -> r.endsWith(".jsp"))
                                    .collect(Collectors.toCollection(TreeSet::new));

                            if (!debugJsps.isEmpty()) {
                                writeStart("ul");
                                    for (String debugJsp : debugJsps) {
                                        debugJsp = debugJsp.substring(WEB_INF_DEBUG.length());

                                        writeStart("li");
                                            writeStart("a", "href", debugJsp);
                                                writeHtml(debugJsp);
                                            writeEnd();
                                        writeEnd();
                                    }
                                writeEnd();
                            }
                        }
                    writeEnd();

                    writeStart("div", "class", "span6");
                        writeStart("h2").writeHtml("Pings").writeEnd();

                        Map<String, Throwable> errors = new CompactMap<>();

                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("Class").writeEnd();
                                    writeStart("th").writeHtml("Status").writeEnd();
                                writeEnd();
                            writeEnd();

                            writeStart("tbody");
                                for (Map.Entry<Class<?>, Throwable> entry : Ping.pingAll().entrySet()) {
                                    String name = entry.getKey().getName();
                                    Throwable error = entry.getValue();

                                    writeStart("tr");
                                        writeStart("td").writeHtml(name).writeEnd();
                                        writeStart("td");
                                            if (error == null) {
                                                writeStart("span", "class", "label label-success").writeHtml("OK").writeEnd();
                                            } else {
                                                writeStart("span", "class", "label label-important").writeHtml("ERROR").writeEnd();
                                                errors.put(name, error);
                                            }
                                        writeEnd();
                                    writeEnd();
                                }
                            writeEnd();
                        writeEnd();

                        if (!errors.isEmpty()) {
                            writeStart("dl");
                                for (Map.Entry<String, Throwable> entry : errors.entrySet()) {
                                    writeStart("dt").writeHtml("Error for ").writeHtml(entry.getKey()).writeEnd();
                                    writeStart("dd").writeObject(entry.getValue()).writeEnd();
                                }
                            writeEnd();
                        }
                    writeEnd();

                writeEnd();
            endPage();
        } };
    }

    /**
     * {@link DebugFilter} utility methods.
     *
     * @deprecated Use the static methods in {@link DebugFilter} instead.
     */
    @Deprecated
    public static final class Static {

        /**
         * Returns the path to the debugging servlet described by the method
         * parameters.
         *
         * @deprecated Use {@link DebugFilter#getPath(HttpServletRequest, Class, Object...)} instead.
         */
        @Deprecated
        public static String getServletPath(HttpServletRequest request, String path, Object... parameters) {
            return JspUtils.getAbsolutePath(request, getInterceptPath() + path, parameters);
        }

        /**
         * Writes a pretty message about the given {@code error}.
         *
         * @param request Nonnull.
         * @param response Nonnull.
         * @param error Nonnnull.
         * @deprecated Use {@link DebugFilter#writeError(HttpServletRequest, HttpServletResponse, Throwable)} instead.
         */
        @Deprecated
        public static void writeError(
                HttpServletRequest request,
                HttpServletResponse response,
                Throwable error)
                throws IOException {

            DebugFilter.writeError(request, response, error);
        }
    }

    /**
     * Specifies that the target servlet should be available through the
     * debugging interface under the given path {@code value}.
     *
     * @deprecated Use {@link DebugServlet} instead.
     */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Path {
        String value();
    }

    /**
     * Specifies that the target servlet should be available through the
     * debugging interface under the given name {@code value}.
     *
     * @deprecated Use {@link DebugServlet} instead.
     */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Name {
        String value();
    }

    /**
     * HTML writer that's specialized for writing debugging interface
     * pages.
     */
    public static class PageWriter extends HtmlWriter {

        protected final WebPageContext page;

        public PageWriter(WebPageContext page) throws IOException {
            super(JspUtils.getWriter(page.getResponse()));
            this.page = page;
            page.getResponse().setContentType("text/html");
            page.getResponse().setCharacterEncoding("UTF-8");
            putAllStandardDefaults();
        }

        public PageWriter(
                ServletContext context,
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {

            this(new WebPageContext(context, request, response));
        }

        public void startHtml() throws IOException {
            writeTag("!DOCTYPE html");
            writeStart("html");
        }

        public void startHead(String title) throws IOException {
            writeStart("head");
                writeStart("title").writeHtml(title).writeEnd();
        }

        public void includeStylesheet(String url) throws IOException {
            writeElement("link", "href", page.url(url), "rel", "stylesheet", "type", "text/css");
        }

        public void includeScript(String url) throws IOException {
            writeStart("script", "src", page.url(url), "type", "text/javascript").writeEnd();
        }

        public void includeStandardStylesheetsAndScripts() throws IOException {
            includeStylesheet("/_resource/bootstrap/css/bootstrap.min.css");
            includeStylesheet("/_resource/codemirror/lib/codemirror.css");
            includeStylesheet("/_resource/codemirror/addon/dialog/dialog.css");

            HttpServletRequest request = page.getRequest();

            writeStart("style", "type", "text/css");
                write("@font-face { font-family: 'AauxNextMedium'; src: url('"); write(JspUtils.getAbsolutePath(request, "/_resource/aauxnext-md-webfont.eot")); write("'); src: local('☺'), url('"); write(JspUtils.getAbsolutePath(request, "/_resource/aauxnext-md-webfont.woff")); write("') format('woff'), url('"); write(JspUtils.getAbsolutePath(request, "/_resource/aauxnext-md-webfont.ttf")); write("') format('truetype'), url('"); write(JspUtils.getAbsolutePath(request, "/_resource/aauxnext-md-webfont.svg#webfontfLsPAukW")); write("') }");
                write("body { word-wrap: break-word; }");
                write("select { word-wrap: normal; }");
                write("h1, h2, h3, h4, h5, h6 { font-family: AauxNextMedium, sans-serif; }");
                write(".navbar-inner { background: #0a5992; }");
                write(".navbar .brand { background: url("); write(JspUtils.getAbsolutePath(request, "/_resource/bridge.png")); write(") no-repeat 55px 0; font-family: AauxNextMedium, sans-serif; height: 40px; line-height: 40px; margin: 0; min-width: 200px; padding: 0; }");
                write(".navbar .brand a { color: #fff; float: left; font-size: 30px; padding-right: 60px; text-transform: uppercase; }");
                write(".popup { width: 60%; }");
                write(".popup .content { background-color: white; -moz-border-radius: 5px; -webkit-border-radius: 5px; border-radius: 5px; -moz-box-shadow: 0 0 10px #777; -webkit-box-shadow: 0 0 10px #777; box-shadow: 0 0 10px #777; position: relative; top: 10px; }");
                write(".popup .content .marker { border-color: transparent transparent white transparent; border-style: solid; border-width: 10px; left: 5px; position: absolute; top: -20px; }");
                write(".CodeMirror-scroll { height: auto; overflow-x: auto; overflow-y: hidden; width: 100%; }");
                write(".CodeMirror { height: auto; }");
                write(".CodeMirror pre { font-family: Menlo, Monaco, 'Courier New', monospace; font-size: 12px; line-height: 1.5em;}");
                write(".CodeMirror .selected { background-color: #FCF8E3; }");
                write(".CodeMirror .errorLine { background-color: #F2DEDE; }");
                write(".CodeMirror .errorColumn { background-color: #B94A48; color: white; }");
                write(".json { position: relative; }");
                write(".json:after { background: #ccc; content: 'JSON'; font-size: 9px; line-height: 9px; padding: 4px; position: absolute; right: 0; top: 0; }");
            writeEnd();

            includeScript("/_resource/jquery/jquery-1.7.1.min.js");
            includeScript("/_resource/jquery/jquery.livequery.js");
            includeScript("/_resource/jquery/jquery.misc.js");
            includeScript("/_resource/jquery/jquery.frame.js");
            includeScript("/_resource/jquery/jquery.popup.js");
            includeScript("/_resource/codemirror/lib/codemirror.js");
            includeScript("/_resource/codemirror/mode/clike.js");
            includeScript("/_resource/codemirror/keymap/vim.js");
            includeScript("/_resource/codemirror/addon/dialog/dialog.js");
            includeScript("/_resource/codemirror/addon/search/searchcursor.js");
            includeScript("/_resource/codemirror/addon/search/search.js");

            writeStart("script", "type", "text/javascript");
                write("$(function() {");
                    write("$('body').frame();");
                write("});");
            writeEnd();
        }

        public void endHead() throws IOException {
            writeEnd();
            flush();
        }

        public void startBody(String... titles) throws IOException {
            writeStart("body");
                writeStart("div", "class", "navbar navbar-fixed-top");
                    writeStart("div", "class", "navbar-inner");
                        writeStart("div", "class", "container-fluid");
                            writeStart("span", "class", "brand");
                                writeStart("a", "href", page.getRequest().getContextPath() + getInterceptPath());
                                    writeHtml("Dari");
                                writeEnd();
                                if (!ObjectUtils.isBlank(titles)) {
                                    for (int i = 0, length = titles.length; i < length; ++ i) {
                                        String title = titles[i];
                                        writeHtml(title);
                                        if (i + 1 < length) {
                                            writeHtml(" \u2192 ");
                                        }
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();
                writeEnd();
                writeStart("div", "class", "container-fluid", "style", "padding-top: 54px;");
        }

        public void endBody() throws IOException {
                writeEnd();
            writeEnd();
        }

        public void endHtml() throws IOException {
            writeEnd();
        }

        /** Writes all necessary elements to start the page. */
        public void startPage(String... titles) throws IOException {
            startHtml();
                startHead(ObjectUtils.isBlank(titles) ? null : titles[0]);
                    includeStandardStylesheetsAndScripts();
                endHead();
                startBody(titles);
        }

        /** Writes all necessary elements to end the page. */
        public void endPage() throws IOException {
                endBody();
            endHtml();
        }
    }
}
