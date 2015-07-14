package com.psddev.dari.util;

import java.io.IOException;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.naming.ldap.LdapContext;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides debugging tools for troubleshooting the application.
 *
 * <p>This filter loads:</p>
 *
 * <ul>
 * <li>{@link SourceFilter}</li>
 * <li>{@link LogCaptureFilter}</li>
 * <li>{@link ResourceFilter}</li>
 * </ul>
 */
public class DebugFilter extends AbstractFilter {

    public static final String DEFAULT_INTERCEPT_PATH = "/_debug/";
    public static final String INTERCEPT_PATH_SETTING = "dari/debugFilterInterceptPath";

    public static final String DEBUG_PARAMETER = "_debug";
    public static final String PRODUCTION_PARAMETER = "_prod";

    private static final Logger LOGGER = LoggerFactory.getLogger(DebugFilter.class);
    private static final Pattern NO_FILE_PATTERN = Pattern.compile("File &quot;(.*)&quot; not found");

    public static final String WEB_INF_DEBUG = "/WEB-INF/_debug/";

    private final transient Lazy<Map<String, ServletWrapper>> debugServletWrappers = new Lazy<Map<String, ServletWrapper>>() {

        {
            CodeUtils.addRedefineClassesListener(new CodeUtils.RedefineClassesListener() {
                @Override
                public void redefined(Set<Class<?>> classes) {
                    for (Class<?> c : classes) {
                        if (Servlet.class.isAssignableFrom(c)) {
                            reset();
                            break;
                        }
                    }
                }
            });
        }

        @Override
        protected Map<String, ServletWrapper> create() {
            Map<String, ServletWrapper> wrappers = new TreeMap<String, ServletWrapper>();

            for (Class<? extends Servlet> servletClass : ClassFinder.Static.findClasses(Servlet.class)) {
                try {
                    if (Modifier.isAbstract(servletClass.getModifiers())) {
                        continue;
                    }

                    String path = null;
                    Path pathAnnotation = servletClass.getAnnotation(Path.class);

                    if (pathAnnotation != null) {
                        path = pathAnnotation.value();
                    }

                    if (ObjectUtils.isBlank(path)) {
                        Name nameAnnotation = servletClass.getAnnotation(Name.class);

                        if (nameAnnotation != null) {
                            path = nameAnnotation.value();
                        }
                    }

                    if (ObjectUtils.isBlank(path)) {
                        continue;
                    }

                    wrappers.put(path, new ServletWrapper(servletClass));

                } catch (Throwable ex) {
                    LOGGER.warn(String.format(
                            "Can't load debug servlet [%s]!",
                            servletClass.getName()), ex);
                }
            }

            LOGGER.info("Found debug servlets: {}", wrappers.keySet());
            return wrappers;
        }

        @Override
        protected void destroy(Map<String, ServletWrapper> wrappers) {
            for (ServletWrapper wrapper : wrappers.values()) {
                wrapper.destroyServlet();
            }
        }
    };

    // --- AbstractFilter support ---

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<Class<? extends Filter>>();
        dependencies.add(SettingsOverrideFilter.class);
        dependencies.add(SourceFilter.class);
        dependencies.add(LogCaptureFilter.class);
        dependencies.add(ResourceFilter.class);
        return dependencies;
    }

    @Override
    protected void doDestroy() {
        debugServletWrappers.reset();
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
                        && ObjectUtils.isBlank(capturing.getOutput())) {
                    throw error;

                // Discard the output so far and write the error instead.
                } else {
                    Static.writeError(request, response, error);
                }
            }
        }
    }

    private static String getInterceptPath() {
        return StringUtils.ensureSurrounding(Settings.getOrDefault(String.class, INTERCEPT_PATH_SETTING, DEFAULT_INTERCEPT_PATH), "/");
    }

    private static boolean authenticate(HttpServletRequest request, HttpServletResponse response) {
        LdapContext context = LdapUtils.createContext();
        String[] credentials = JspUtils.getBasicCredentials(request);

        if (context != null
                && credentials != null
                && LdapUtils.authenticate(context, credentials[0], credentials[1])) {
            return true;
        }

        String username = ObjectUtils.firstNonNull(
                Settings.get(String.class, "dari/debugUsername"),
                Settings.get(String.class, "servlet/debugUsername"));

        String password = ObjectUtils.firstNonNull(
                Settings.get(String.class, "dari/debugPassword"),
                Settings.get(String.class, "servlet/debugPassword"));

        if (context == null
                && (ObjectUtils.isBlank(username)
                || ObjectUtils.isBlank(password))) {
            if (!Settings.isProduction()) {
                return true;
            }

        } else if (credentials != null
                && credentials[0].equals(username)
                && credentials[1].equals(password)) {
            return true;
        }

        String realm = ObjectUtils.firstNonNull(
                Settings.get(String.class, "dari/debugRealm"),
                Settings.get(String.class, "servlet/debugRealm"),
                "Debugging Tool");

        JspUtils.setBasicAuthenticationHeader(response, realm);
        return false;
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String interceptPath = getInterceptPath();
        String path = request.getServletPath();
        if (path.equals(interceptPath.substring(0, interceptPath.length() - 1))) {
            JspUtils.redirect(request, response, interceptPath);
            return;

        } else if (!path.startsWith(interceptPath)) {
            chain.doFilter(request, response);
            return;
        }

        if (!authenticate(request, response)) {
            return;
        }

        final ServletContext context = getServletContext();
        final String pathInfo;
        String action = path.substring(interceptPath.length());
        int slashAt = action.indexOf('/');

        if (slashAt > -1) {
            pathInfo = action.substring(slashAt);
            action = action.substring(0, slashAt);

        } else {
            pathInfo = "/";
        }

        if (!ObjectUtils.isBlank(action)) {
            ServletWrapper wrapper = debugServletWrappers.get().get(action);
            if (wrapper != null) {
                wrapper.serviceServlet(new PathInfoOverrideRequest(request, pathInfo), response);
                return;
            }

            String actionJsp = WEB_INF_DEBUG + action;
            try {
                if (context.getResource(actionJsp) != null) {
                    JspUtils.forward(request, response, actionJsp);
                    return;
                }
            } catch (MalformedURLException error) {
                // If actionJsp isn't a valid URL, pretend that the action
                // parameter wasn't provided by falling through to the code
                // path below.
            }
        }

        new PageWriter(getServletContext(), request, response) { {
            startPage();
                writeStart("div", "class", "row-fluid");

                    writeStart("div", "class", "span3");
                        writeStart("h2").writeHtml("Standard Tools").writeEnd();
                        writeStart("ul");
                            for (Map.Entry<String, ServletWrapper> entry : debugServletWrappers.get().entrySet()) {
                                writeStart("li");
                                    writeStart("a", "href", entry.getKey());
                                        writeHtml(entry.getKey());
                                    writeEnd();
                                writeEnd();
                            }
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "span3");
                        writeStart("h2").writeHtml("Custom Tools").writeEnd();

                        @SuppressWarnings("unchecked")
                        Set<String> resources = (Set<String>) context.getResourcePaths(WEB_INF_DEBUG);
                        if (resources != null && !resources.isEmpty()) {
                            Set<String> jsps = new TreeSet<String>();
                            for (String jsp : resources) {
                                if (jsp.endsWith(".jsp")) {
                                    jsps.add(jsp);
                                }
                            }

                            if (!jsps.isEmpty()) {
                                writeStart("ul");
                                    for (String jsp : jsps) {
                                        jsp = jsp.substring(WEB_INF_DEBUG.length());
                                        writeStart("li");
                                            writeStart("a", "href", jsp);
                                                writeHtml(jsp);
                                            writeEnd();
                                        writeEnd();
                                    }
                                writeEnd();
                            }
                        }
                    writeEnd();

                    writeStart("div", "class", "span6");
                        writeStart("h2").writeHtml("Pings").writeEnd();

                        Map<String, Throwable> errors = new CompactMap<String, Throwable>();
                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("Class").writeEnd();
                                    writeStart("th").writeHtml("Status").writeEnd();
                                writeEnd();
                            writeEnd();
                            writeStart("tbody");
                                for (Map.Entry<Class<?>, Throwable> entry : Ping.Static.pingAll().entrySet()) {
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

    private static final class PathInfoOverrideRequest extends HttpServletRequestWrapper {

        private final String pathInfo;

        public PathInfoOverrideRequest(HttpServletRequest request, String pathInfo) {
            super(request);
            this.pathInfo = pathInfo;
        }

        @Override
        public String getPathInfo() {
            return pathInfo;
        }
    }

    /** {@link DebugFilter} utility methods. */
    public static final class Static {

        /**
         * Returns the path to the debugging servlet described in the
         * given parameters.
         */
        public static String getServletPath(HttpServletRequest request, String path, Object... parameters) {
            return JspUtils.getAbsolutePath(request, getInterceptPath() + path, parameters);
        }

        /**
         * Writes a pretty message about the given {@code error}.
         *
         * @param request Can't be {@code null}.
         * @param response Can't be {@code null}.
         * @param error Can't be {@code null}.
         */
        public static void writeError(
                HttpServletRequest request,
                HttpServletResponse response,
                Throwable error)
                throws IOException {

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
                                "href", DebugFilter.Static.getServletPath(request, "code",
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
    }

    /**
     * Specifies that the target servlet should be available through
     * the debugging interface at the given path {@code value}.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Path {
        String value();
    }

    /** Use {@link Path} instead. */
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

            writeStart("style", "type", "text/css");
                write("@font-face { font-family: 'AauxNextMedium'; src: url('/_resource/aauxnext-md-webfont.eot'); src: local('☺'), url('/_resource/aauxnext-md-webfont.woff') format('woff'), url('/_resource/aauxnext-md-webfont.ttf') format('truetype'), url('/_resource/aauxnext-md-webfont.svg#webfontfLsPAukW') }");
                write("body { word-wrap: break-word; }");
                write("select { word-wrap: normal; }");
                write("h1, h2, h3, h4, h5, h6 { font-family: AauxNextMedium, sans-serif; }");
                write(".navbar-inner { background: #0a5992; }");
                write(".navbar .brand { background: url(/_resource/bridge.png) no-repeat 55px 0; font-family: AauxNextMedium, sans-serif; height: 40px; line-height: 40px; margin: 0; min-width: 200px; padding: 0; }");
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
                                writeStart("a", "href", DebugFilter.Static.getServletPath(page.getRequest(), ""));
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

    private class ServletWrapper implements ServletConfig {

        private final Servlet servlet;
        private final AtomicBoolean isInitialized = new AtomicBoolean();

        public ServletWrapper(Class<? extends Servlet> servletClass) {
            this.servlet = TypeDefinition.getInstance(servletClass).newInstance();
        }

        public void serviceServlet(
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException, ServletException {

            if (isInitialized.compareAndSet(false, true)) {
                servlet.init(this);
                LOGGER.info("Initialized [{}] servlet", getServletName());
            }
            servlet.service(request, response);
        }

        public void destroyServlet() {
            if (isInitialized.compareAndSet(true, false)) {
                servlet.destroy();
                LOGGER.info("Destroyed [{}] servlet", getServletName());
            }
        }

        // --- ServletConfig support ---

        @Override
        public String getInitParameter(String name) {
            return null;
        }

        @Override
        public Enumeration<String> getInitParameterNames() {
            return Collections.enumeration(Collections.<String>emptyList());
        }

        @Override
        public ServletContext getServletContext() {
            return DebugFilter.this.getServletContext();
        }

        @Override
        public String getServletName() {
            return getFilterConfig().getFilterName() + "$" + servlet.getClass().getName();
        }
    }

    /** Overrides certain settings based on request parameters. */
    private static class SettingsOverrideFilter extends AbstractFilter {

        @Override
        protected void doRequest(
                HttpServletRequest request,
                HttpServletResponse response,
                FilterChain chain)
                throws IOException, ServletException {

            try {
                Boolean production = ObjectUtils.to(Boolean.class, request.getParameter(PRODUCTION_PARAMETER));
                if (production != null && !authenticate(request, response)) {
                    return;
                }

                Settings.setOverride(Settings.PRODUCTION_SETTING, production);

                if (ObjectUtils.to(boolean.class, request.getParameter(DEBUG_PARAMETER))) {
                    if (authenticate(request, response)) {
                        Settings.setOverride(Settings.DEBUG_SETTING, Boolean.TRUE);
                        if (production == null) {
                            Settings.setOverride(Settings.PRODUCTION_SETTING, Boolean.FALSE);
                        }
                    } else {
                        return;
                    }
                }

                chain.doFilter(request, response);

            } finally {
                Settings.setOverride(Settings.PRODUCTION_SETTING, null);
                Settings.setOverride(Settings.DEBUG_SETTING, null);
            }
        }
    }
}
