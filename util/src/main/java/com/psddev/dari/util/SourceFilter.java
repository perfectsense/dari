package com.psddev.dari.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.instrument.ClassDefinition;
import java.lang.instrument.Instrumentation;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.psddev.dari.util.sa.JvmAnalyzer;
import com.psddev.dari.util.sa.JvmLogger;

/**
 * Enables rapid web application development by making source code changes
 * immediately available.
 *
 * <p>To configure, add these definitions to the {@code web.xml} deployment
 * descriptor file:
 *
 * <p><blockquote><pre>{@literal
<filter>
    <filter-name>SourceFilter</filter-name>
    <filter-class>com.psddev.dari.util.SourceFilter</filter-class>
</filter>
<filter-mapping>
    <filter-name>SourceFilter</filter-name>
    <url-pattern>/*</url-pattern>
</filter-mapping>
 * }</pre></blockquote>
 *
 * <p>And the application must include a {@code build.properties} file
 * that specifies the locations of the source code:
 *
 * <ul>
 * <li>{@link CodeUtils#JAVA_SOURCE_DIRECTORY_PROPERTY}
 * <li>{@link #WEBAPP_SOURCES_PROPERTY}
 * </ul>
 *
 * <p>You can skip this step if the project uses Apache Maven to manage
 * the build and inherits from {@code com.psddev:dari-parent}, because
 * that parent POM automatically adds those properties during the
 * {@code generate-resources} phase.
 */
public class SourceFilter extends AbstractFilter {

    /**
     * Build property that specifies the directory containing the
     * web application sources, such as JSPs.
     */
    public static final String WEBAPP_SOURCES_PROPERTY = "webappSourceDirectory";

    public static final String DEFAULT_INTERCEPT_PATH = "/_sourceFilter";
    public static final String INTERCEPT_PATH_SETTING = "dari/sourceFilterInterceptPath";

    public static final String DEFAULT_RELOADER_PATH = "/_reloader/";
    public static final String RELOADER_PATH_SETTING = "dari/sourceFilterReloaderPath";
    public static final String RELOADER_ACTION_PARAMETER = "action";
    public static final String RELOADER_PING_ACTION = "ping";
    public static final String RELOADER_RELOAD_ACTION = "reload";
    public static final String RELOADER_CONTEXT_PATH_PARAMETER = "contextPath";
    public static final String RELOADER_REQUEST_PATH_PARAMETER = "requestPath";

    private static final Logger LOGGER = LoggerFactory.getLogger(SourceFilter.class);

    private static final Pattern TEXT_HTML_PATTERN = Pattern.compile("(?i)(^|,)text/html([,;]|$)");

    private static final String CLASSES_PATH = "/WEB-INF/classes/";
    private static final String BUILD_PROPERTIES_PATH = "build.properties";
    private static final String ISOLATING_RESPONSE_ATTRIBUTE = SourceFilter.class.getName() + ".isolatingResponse";
    private static final String IS_ISOLATION_DONE_ATTRIBUTE = SourceFilter.class.getName() + ".isIsolationDone";
    private static final String COPIED_ATTRIBUTE = SourceFilter.class.getName() + ".copied";

    private static final String CATALINA_BASE_PROPERTY = "catalina.base";
    private static final String RELOADER_MAVEN_ARTIFACT_ID = "dari-reloader-tomcat";
    private static final String RELOADER_MAVEN_VERSION = "3.2-SNAPSHOT";
    private static final String RELOADER_MAVEN_URL = "https://artifactory.psdops.com/public/com/psddev/" + RELOADER_MAVEN_ARTIFACT_ID + "/" + RELOADER_MAVEN_VERSION + "/";
    private static final Pattern BUILD_NUMBER_PATTERN = Pattern.compile("<buildNumber>([^<]*)</buildNumber>");
    private static final Pattern TIMESTAMP_PATTERN = Pattern.compile("<timestamp>([^<]*)</timestamp>");

    private File classOutput;
    private final Set<File> javaSourcesSet = new HashSet<File>();
    private Map<JavaFileObject, Long> javaSourceFileModifieds;
    private final Map<String, File> webappSourcesMap = new HashMap<String, File>();
    private final Map<String, Date> changedClassTimes = new TreeMap<String, Date>();

    private final Map<Class<?>, List<AnalysisResult>> analysisResultsByClass = new TreeMap<Class<?>, List<AnalysisResult>>(new Comparator<Class<?>>() {

        @Override
        public int compare(Class<?> x, Class<?> y) {
            return x.getName().compareTo(y.getName());
        }
    });

    // --- AbstractFilter support ---

    @Override
    protected void doInit() {
        if (ToolProvider.getSystemJavaCompiler() == null) {
            LOGGER.info("Java compiler not available!");
            return;
        }

        ServletContext context = getServletContext();
        String classOutputString = context.getRealPath(CLASSES_PATH);
        if (classOutputString == null) {
            LOGGER.info("Can't get the real path to [{}]!", CLASSES_PATH);
            return;
        }

        classOutput = new File(classOutputString);
        if (classOutput.exists()) {
            LOGGER.info("Saving recompiled Java classes to [{}]", classOutput);
        } else {
            LOGGER.info("[{}] doesn't exist!", classOutput);
            classOutput = null;
            return;
        }

        javaSourcesSet.addAll(CodeUtils.getSourceDirectories());

        processWarBuildProperties(context, "");
        for (String contextPath : JspUtils.getEmbeddedSettings(context).keySet()) {
            processWarBuildProperties(context, contextPath);
        }
    }

    /**
     * Processes the build properties file associated with the given
     * {@code context} and {@code contextPath} and adds its source
     * directories.
     *
     * @param context Can't be {@code null}.
     * @param contextPath Can't be {@code null}.
     */
    private void processWarBuildProperties(ServletContext context, String contextPath) {
        InputStream buildPropertiesInput = context.getResourceAsStream(contextPath + CLASSES_PATH + BUILD_PROPERTIES_PATH);
        if (buildPropertiesInput == null) {
            return;
        }

        try {
            try {
                Properties buildProperties = new Properties();
                buildProperties.load(buildPropertiesInput);

                String javaSourcesString = buildProperties.getProperty(CodeUtils.JAVA_SOURCE_DIRECTORY_PROPERTY);
                if (javaSourcesString != null) {
                    File javaSources = new File(javaSourcesString);
                    if (javaSources.exists()) {
                        javaSourcesSet.add(javaSources);
                        LOGGER.info("Found Java sources in [{}]", javaSources);
                    }
                }

                String webappSourcesString = buildProperties.getProperty(WEBAPP_SOURCES_PROPERTY);
                if (webappSourcesString != null) {
                    File webappSources = new File(webappSourcesString);
                    if (webappSources.exists()) {
                        LOGGER.info("Copying webapp sources from [{}] to [{}/]", webappSources, contextPath);
                        webappSourcesMap.put(contextPath, webappSources);
                    }
                }

            } finally {
                buildPropertiesInput.close();
            }

        } catch (IOException error) {
            LOGGER.debug("Can't read WAR build properties!", error);
        }
    }

    @Override
    protected void doDestroy() {
        classOutput = null;
        javaSourcesSet.clear();
        javaSourceFileModifieds = null;
        webappSourcesMap.clear();
        changedClassTimes.clear();
    }

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Settings.isProduction()) {
            chain.doFilter(request, response);
            return;
        }

        if (request.getAttribute(IS_ISOLATION_DONE_ATTRIBUTE) != null) {
            return;
        }

        IsolatingResponse isolatingResponse = (IsolatingResponse) request.getAttribute(ISOLATING_RESPONSE_ATTRIBUTE);
        if (isolatingResponse != null) {
            if (JspUtils.getCurrentServletPath(request).equals(isolatingResponse.jsp)) {
                @SuppressWarnings("all")
                HtmlWriter html = new HtmlWriter(isolatingResponse.getResponse().getWriter());
                html.putAllStandardDefaults();

                try {
                    StringWriter writer = new StringWriter();
                    JspUtils.include(request, response, writer, request.getParameter("_draft"));
                    html.writeStart("pre");
                        html.writeHtml(writer.toString().trim());
                    html.writeEnd();

                } catch (RuntimeException ex) {
                    html.writeStart("pre", "class", "alert alert-error");
                        html.writeObject(ex);
                    html.writeEnd();

                } finally {
                    request.setAttribute(IS_ISOLATION_DONE_ATTRIBUTE, Boolean.TRUE);
                }
                return;
            }
        }

        copyWebappSource(request);
        super.doDispatch(request, response, chain);
    }

    @Override
    protected void doRequest(
            final HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        copyResources.get();

        // Intercept special actions.
        if (!ObjectUtils.isBlank(request.getParameter("_reload"))
                && isReloaderAvailable(request)) {
            compileJavaSourceFiles();
            response.sendRedirect(StringUtils.addQueryParameters(
                    getReloaderPath(),
                    RELOADER_CONTEXT_PATH_PARAMETER, request.getContextPath(),
                    RELOADER_REQUEST_PATH_PARAMETER, new UrlBuilder(request)
                            .currentPath()
                            .currentParameters()
                            .parameter("_reload", null)
                            .toString(),
                    RELOADER_ACTION_PARAMETER, RELOADER_RELOAD_ACTION));
            return;
        }

        String servletPath = request.getServletPath();
        if (servletPath.startsWith(getInterceptPath())) {
            String action = request.getParameter("action");

            if ("ping".equals(action)) {
                response.setContentType("text/plain");
                response.setCharacterEncoding("UTF-8");
                response.getWriter().write("OK");

            } else if ("install".equals(action)) {
                if (isReloaderAvailable(request)) {
                    String requestPath = request.getParameter("requestPath");
                    response.sendRedirect(ObjectUtils.isBlank(requestPath) ? "/" : requestPath);
                } else {
                    @SuppressWarnings("all")
                    ReloaderInstaller installer = new ReloaderInstaller(request, response);
                    installer.writeStart();
                }

            } else if ("clear".equals(action)) {
                analysisResultsByClass.clear();

            } else {
                throw new IllegalArgumentException(String.format(
                        "[%s] isn't a valid intercept action!", action));
            }
            return;
        }

        // Try to detect if this request was initiated by an actual person.
        // There's no reliable way of doing this, but all modern browsers
        // seem to send text/html in the Accept header only for the main
        // request.
        String accept = request.getHeader("Accept");

        if (accept == null
                || !TEXT_HTML_PATTERN.matcher(accept).find()) {

            chain.doFilter(request, response);
            return;
        }

        List<Diagnostic<? extends JavaFileObject>> diagnostics = compileJavaSourceFiles();
        boolean requiresReload;
        boolean hasBackgroundTasks;

        if (diagnostics == null
                && !changedClassTimes.isEmpty()
                && !JspUtils.isAjaxRequest(request)) {

            requiresReload = true;

            if (hasBackgroundTasks()) {
                hasBackgroundTasks = true;

            } else if (isReloaderAvailable(request)) {
                changedClassTimes.clear();
                response.sendRedirect(StringUtils.addQueryParameters(
                        getReloaderPath(),
                        RELOADER_CONTEXT_PATH_PARAMETER, request.getContextPath(),
                        RELOADER_REQUEST_PATH_PARAMETER, new UrlBuilder(request)
                                .currentPath()
                                .currentParameters()
                                .toString(),
                        RELOADER_ACTION_PARAMETER, RELOADER_RELOAD_ACTION));
                return;

            } else {
                hasBackgroundTasks = false;
            }

        } else {
            requiresReload = false;
            hasBackgroundTasks = false;
        }

        IsolatingResponse isolatingResponse = null;
        String jsp = request.getParameter("_jsp");
        if (!ObjectUtils.isBlank(jsp)) {
            response.setContentType("text/plain");
            isolatingResponse = new IsolatingResponse(response, jsp);
            response = isolatingResponse;
            request.setAttribute(ISOLATING_RESPONSE_ATTRIBUTE, isolatingResponse);
        }

        chain.doFilter(request, response);
        if (diagnostics == null
                && analysisResultsByClass.isEmpty()
                && (changedClassTimes.isEmpty()
                || JspUtils.isAjaxRequest(request)
                || isolatingResponse != null)) {
            return;
        }

        // Can't reload automatically so at least let the user know
        // if viewing an HTML page.
        String responseContentType = response.getContentType();
        if (responseContentType == null
                || !responseContentType.startsWith("text/html")) {
            return;
        }

        HtmlWriter noteWriter = new HtmlWriter(response.getWriter());

        noteWriter.putDefault(StackTraceElement.class, HtmlFormatter.STACK_TRACE_ELEMENT);
        noteWriter.putDefault(Throwable.class, HtmlFormatter.THROWABLE);

        noteWriter.writeStart("div",
                "class", "dari-sourceFilter-notification",
                "style", noteWriter.cssString(
                        "background", "#002b36",
                        "border-bottom-left-radius", "2px",
                        "box-sizing", "border-box",
                        "color", "#839496",
                        "font-family", "'Helvetica Neue', 'Arial', sans-serif",
                        "font-size", "13px",
                        "font-weight", "normal",
                        "line-height", "18px",
                        "margin", "0",
                        "max-height", "50%",
                        "max-width", "350px",
                        "overflow", "auto",
                        "padding", "0 35px 10px 10px",
                        "position", "fixed",
                        "right", "0",
                        "top", "0",
                        "word-break", "break-all",
                        "word-wrap", "break-word",
                        "z-index", "1000000"));

            noteWriter.writeStart("span",
                    "onclick",
                            "var notifications = document.querySelectorAll('.dari-sourceFilter-notification');"
                            + "var notificationsIndex = 0;"
                            + "var notificationsLength = notifications.length;"
                            + "for (; notificationsIndex < notificationsLength; ++ notificationsIndex) {"
                                + "var notification = notifications[notificationsIndex];"
                                + "notification.parentNode.removeChild(notification);"
                            + "}"
                            + "var xhr = new XMLHttpRequest();"
                            + "xhr.open('get', '" + StringUtils.escapeJavaScript(JspUtils.getAbsolutePath(request, getInterceptPath(), "action", "clear")) + "', true);"
                            + "xhr.send('');"
                            + "return false;",
                    "style", noteWriter.cssString(
                            "cursor", "pointer",
                            "font-size", "20px",
                            "height", "20px",
                            "line-height", "20px",
                            "position", "absolute",
                            "right", "5px",
                            "text-align", "center",
                            "top", "5px",
                            "width", "20px"));
                noteWriter.writeHtml("\u00d7");
            noteWriter.writeEnd();

            if (requiresReload) {
                if (hasBackgroundTasks) {
                    noteWriter.writeHtml("The application wasn't reloaded automatically because there are background tasks running!");

                } else {
                    noteWriter.writeHtml("The application must be reloaded before the changes to these classes become visible. ");
                    noteWriter.writeStart("a",
                            "href", JspUtils.getAbsolutePath(request, getInterceptPath(),
                                    "action", "install",
                                    "requestPath", JspUtils.getAbsolutePath(request, "")),
                            "style", "color: white; text-decoration: underline;");
                        noteWriter.writeHtml("Install the reloader");
                    noteWriter.writeEnd();
                    noteWriter.writeHtml(" to automate this process.");
                    noteWriter.writeElement("br");
                    noteWriter.writeElement("br");

                    for (Map.Entry<String, Date> entry : changedClassTimes.entrySet()) {
                        noteWriter.writeHtml(entry.getKey());
                        noteWriter.writeHtml(" - ");
                        noteWriter.writeObject(entry.getValue());
                        noteWriter.writeElement("br");
                    }
                }
            }

            if (diagnostics != null) {
                for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics) {
                    JavaFileObject source = diagnostic.getSource();

                    writeDiagnostic(
                            request,
                            noteWriter,
                            diagnostic.getKind(),
                            source != null ? source.getName() : "Unknown Source",
                            null,
                            diagnostic.getLineNumber(),
                            diagnostic.getColumnNumber(),
                            diagnostic.getMessage(null));
                }
            }

            for (Map.Entry<Class<?>, List<AnalysisResult>> entry : analysisResultsByClass.entrySet()) {
                File source = CodeUtils.getSource(entry.getKey().getName());

                if (source != null) {
                    String sourceFileName = source.getPath();

                    for (AnalysisResult ar : entry.getValue()) {
                        writeDiagnostic(
                                request,
                                noteWriter,
                                ar.getKind(),
                                sourceFileName,
                                ar.getMethod(),
                                ar.getLine(),
                                -1,
                                ar.getMessage());
                    }
                }
            }
        noteWriter.writeEnd();
    }

    private void writeDiagnostic(
            HttpServletRequest request,
            HtmlWriter writer,
            Diagnostic.Kind kind,
            String fileName,
            Method method,
            long lineNumber,
            long columnNumber,
            String message)
            throws IOException {

        String color;

        switch (kind) {
            case ERROR :
                color = "#dc322f";
                break;

            case MANDATORY_WARNING :
            case WARNING :
                color = "#b58900";
                break;

            default :
                color = "#839496";
                break;
        }

        writer.writeStart("div", "style", writer.cssString(
                "color", color,
                "margin-top", "10px"));

            writer.writeHtml('[');
            writer.writeHtml(kind);
            writer.writeHtml("] ");

            writer.writeStart("a",
                    "target", "_blank",
                    "style", writer.cssString(
                            "color", color,
                            "text-decoration", "underline"),
                    "href", JspUtils.getAbsolutePath(
                            request, "/_debug/code",
                            "action", "edit",
                            "file", fileName,
                            "line", lineNumber));

                if (method != null) {
                    writer.writeHtml(method.getDeclaringClass().getName());
                    writer.writeHtml('.');
                    writer.writeHtml(method.getName());

                } else {
                    int separatorAt = fileName.lastIndexOf(File.separatorChar);

                    writer.writeHtml(separatorAt > -1
                            ? fileName.substring(separatorAt + 1)
                            : fileName);
                }

                if (lineNumber > 0) {
                    writer.writeHtml(':');
                    writer.writeHtml(lineNumber);
                }

                if (columnNumber > 0) {
                    writer.writeHtml(':');
                    writer.writeHtml(columnNumber);
                }
            writer.writeEnd();

            writer.writeHtml(' ');
            writer.writeHtml(message);
        writer.writeEnd();
    }

    /**
     * Returns the path that intercepts all special actions.
     *
     * @return Always starts and ends with a slash.
     */
    private static String getInterceptPath() {
        return StringUtils.ensureStart(Settings.getOrDefault(String.class, INTERCEPT_PATH_SETTING, DEFAULT_INTERCEPT_PATH), "/");
    }

    /**
     * Returns the path to the application that can reload this one.
     *
     * @return Always starts and ends with a slash.
     */
    private static String getReloaderPath() {
        return StringUtils.ensureSurrounding(Settings.getOrDefault(String.class, RELOADER_PATH_SETTING, DEFAULT_RELOADER_PATH), "/");
    }

    /**
     * Returns {@code true} if the reloader is available in the same
     * server as this application.
     *
     * @param request Can't be {@code null}.
     */
    private boolean isReloaderAvailable(HttpServletRequest request) {
        String servletPath = request.getServletPath();
        String reloaderPath = getReloaderPath();

        if (!servletPath.startsWith(reloaderPath)) {
            try {
                URL pingUrl = new URL(StringUtils.addQueryParameters(
                        JspUtils.getHostUrl(request) + reloaderPath,
                        RELOADER_ACTION_PARAMETER, RELOADER_PING_ACTION));

                // To avoid infinite redirects in case the ping hits this
                // application.
                URLConnection pingConnection = pingUrl.openConnection();
                if (pingConnection instanceof HttpURLConnection) {
                    ((HttpURLConnection) pingConnection).setInstanceFollowRedirects(false);
                }

                InputStream pingInput = pingConnection.getInputStream();
                try {
                    return "OK".equals(IoUtils.toString(pingInput, StandardCharsets.UTF_8));
                } finally {
                    pingInput.close();
                }

            } catch (IOException error) {
                // If the ping fails for any reason, assume that the reloader
                // isn't available.
            }
        }

        return false;
    }

    /** Returns {@code true} if there are any background tasks running. */
    private boolean hasBackgroundTasks() {
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            String executorName = executor.getName();
            if (!("Periodic Caches".equals(executorName)
                    || "Miscellaneous Tasks".equals(executorName))) {
                for (Object task : executor.getTasks()) {
                    if (task instanceof Task && !((Task) task).isSafeToStop() && ((Task) task).isRunning()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    // Copies all resources, throttled to run at most once per second.
    private final Supplier<Void> copyResources = Suppliers.memoizeWithExpiration(new Supplier<Void>() {

        @Override
        public Void get() {
            if (classOutput != null) {
                for (File resourceDirectory : CodeUtils.getResourceDirectories()) {
                    Long jarModified = CodeUtils.getJarLastModified(resourceDirectory);

                    try {
                        copy(resourceDirectory, jarModified, resourceDirectory);

                    } catch (IOException error) {
                        throw new IllegalStateException(error);
                    }
                }
            }

            ResourceBundle.clearCache();

            return null;
        }

        private void copy(File resourceDirectory, Long jarModified, File resource) throws IOException {
            if (resource.isDirectory()) {
                for (File child : resource.listFiles()) {
                    copy(resourceDirectory, jarModified, child);
                }

            } else {
                File output = new File(classOutput, resource.toString().substring(resourceDirectory.toString().length()).replace(File.separatorChar, '/'));
                long resourceModified = resource.lastModified();
                long outputModified = output.lastModified();

                if ((jarModified == null
                        || resourceModified > jarModified)
                        && resourceModified > outputModified) {
                    IoUtils.copy(resource, output);
                    LOGGER.info("Copied [{}]", resource);
                }
            }
        }
    }, 1, TimeUnit.SECONDS);

    // Compile any Java source files that's changed and redefine them
    // in place if possible.
    private synchronized List<Diagnostic<? extends JavaFileObject>> compileJavaSourceFiles() throws IOException {
        if (javaSourcesSet.isEmpty()) {
            return null;
        }

        boolean firstRun;

        if (javaSourceFileModifieds == null) {
            firstRun = true;
            javaSourceFileModifieds = new HashMap<JavaFileObject, Long>();

        } else {
            firstRun = false;
        }

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
        Map<JavaFileObject, Long> newSourceFiles = new HashMap<JavaFileObject, Long>();
        Map<String, Date> newChangedClassTimes = new HashMap<String, Date>();
        List<ClassDefinition> toBeRedefined = new ArrayList<ClassDefinition>();
        List<Diagnostic<? extends JavaFileObject>> diagnostics = null;

        try {
            fileManager.setLocation(StandardLocation.SOURCE_PATH, javaSourcesSet);
            fileManager.setLocation(StandardLocation.CLASS_OUTPUT, Collections.singleton(classOutput));

            // Find all source files that's changed.
            for (JavaFileObject sourceFile : fileManager.list(
                    StandardLocation.SOURCE_PATH,
                    "",
                    Collections.singleton(JavaFileObject.Kind.SOURCE),
                    true)) {

                // On first run, assume nothing has changed.
                if (firstRun) {
                    javaSourceFileModifieds.put(sourceFile, sourceFile.getLastModified());

                } else {
                    Long oldModified = javaSourceFileModifieds.get(sourceFile);
                    long newModified = sourceFile.getLastModified();

                    if (oldModified == null || oldModified != newModified) {
                        newSourceFiles.put(sourceFile, newModified);
                    }
                }
            }

            if (!newSourceFiles.isEmpty()) {
                LOGGER.info("Recompiling {}", newSourceFiles.keySet());

                // Compiler can't use the current class loader so try to
                // guess all of its class paths.
                Set<File> classPaths = new LinkedHashSet<File>();

                for (ClassLoader loader = ObjectUtils.getCurrentClassLoader();
                        loader != null;
                        loader = loader.getParent()) {
                    if (loader instanceof URLClassLoader) {
                        for (URL url : ((URLClassLoader) loader).getURLs()) {
                            File file = IoUtils.toFile(url, StandardCharsets.UTF_8);

                            if (file != null) {
                                classPaths.add(file);
                            }
                        }
                    }
                }

                fileManager.setLocation(StandardLocation.CLASS_PATH, classPaths);

                // Remember the current class file modified times for later
                // when we need to figure out what changed.
                Map<JavaFileObject, Long> outputFileModifieds = new HashMap<JavaFileObject, Long>();

                for (JavaFileObject outputFile : fileManager.list(
                        StandardLocation.CLASS_OUTPUT,
                        "",
                        Collections.singleton(JavaFileObject.Kind.CLASS),
                        true)) {
                    outputFileModifieds.put(outputFile, outputFile.getLastModified());
                }

                DiagnosticCollector<JavaFileObject> diagnosticsCollector = new DiagnosticCollector<JavaFileObject>();

                if (!compiler.getTask(null, fileManager, diagnosticsCollector, Arrays.asList("-g"), null, newSourceFiles.keySet()).call()) {
                    diagnostics = diagnosticsCollector.getDiagnostics();

                    for (Diagnostic<? extends JavaFileObject> d : diagnostics) {
                        LOGGER.warn("Failed to compile: {}", d.getSource());
                    }

                    return diagnostics;

                } else {
                    javaSourceFileModifieds.putAll(newSourceFiles);

                    Set<Class<? extends ClassEnhancer>> enhancerClasses = new HashSet<>();
                    ServletContext context = getServletContext();

                    if (context != null) {
                        ClassFinder.getThreadDefaultServletContext().with(context, () -> {
                            enhancerClasses.addAll(ClassFinder.findClasses(ClassEnhancer.class));
                        });

                    } else {
                        enhancerClasses.addAll(ClassFinder.findClasses(ClassEnhancer.class));
                    }

                    // Process any class files that's changed.
                    for (JavaFileObject outputFile : fileManager.list(
                            StandardLocation.CLASS_OUTPUT,
                            "",
                            Collections.singleton(JavaFileObject.Kind.CLASS),
                            true)) {
                        Long oldModified = outputFileModifieds.get(outputFile);
                        long newModified = outputFile.getLastModified();

                        if (oldModified != null && oldModified == newModified) {
                            continue;
                        }

                        // Enhance the bytecode.
                        InputStream input = outputFile.openInputStream();
                        byte[] bytecode;

                        try {
                            bytecode = IoUtils.toByteArray(input);

                        } finally {
                            input.close();
                        }

                        byte[] enhancedBytecode = ClassEnhancer.Static.enhance(bytecode, enhancerClasses);

                        if (enhancedBytecode != null) {
                            bytecode = enhancedBytecode;
                        }

                        OutputStream output = outputFile.openOutputStream();

                        try {
                            output.write(bytecode);

                        } finally {
                            output.close();
                        }

                        String outputClassName = fileManager.inferBinaryName(StandardLocation.CLASS_OUTPUT, outputFile);
                        Class<?> outputClass = ObjectUtils.getClassByName(outputClassName);

                        newChangedClassTimes.put(outputClassName, new Date(newModified));

                        if (outputClass != null) {
                            toBeRedefined.add(new ClassDefinition(outputClass, bytecode));
                        }
                    }

                    // Try to redefine the classes in place.
                    List<ClassDefinition> failures = CodeUtils.redefineClasses(toBeRedefined);
                    List<Class<?>> toBeAnalyzed = new ArrayList<Class<?>>();

                    toBeRedefined.removeAll(failures);

                    for (ClassDefinition success : toBeRedefined) {
                        Class<?> c = success.getDefinitionClass();

                        newChangedClassTimes.remove(c.getName());
                        toBeAnalyzed.add(c);
                        analysisResultsByClass.remove(c);
                    }

                    if (!toBeAnalyzed.isEmpty()) {
                        try {
                            JvmAnalyzer.Static.analyze(toBeAnalyzed, new AnalysisResultLogger());

                        } catch (Exception error) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(String.format("Can't analyze %s!", toBeAnalyzed), error);
                            }
                        }
                    }

                    if (!failures.isEmpty() && LOGGER.isInfoEnabled()) {
                        StringBuilder messageBuilder = new StringBuilder();

                        messageBuilder.append("Can't redefine [");

                        for (ClassDefinition failure : failures) {
                            messageBuilder.append(failure.getDefinitionClass().getName());
                            messageBuilder.append(", ");
                        }

                        messageBuilder.setLength(messageBuilder.length() - 2);
                        messageBuilder.append("]!");
                    }
                }
            }

        } finally {
            fileManager.close();
        }

        // Remember all classes that's changed but not yet redefined.
        changedClassTimes.putAll(newChangedClassTimes);
        return diagnostics != null && !diagnostics.isEmpty() ? diagnostics : null;
    }

    // Copies the webapp source associated with the given request.
    private void copyWebappSource(HttpServletRequest request) throws IOException {
        ServletContext context = getServletContext();
        String path = JspUtils.getCurrentServletPath(request);

        if (path.startsWith("/WEB-INF/_draft/")) {
            return;
        }

        String contextPath = JspUtils.getEmbeddedContextPath(context, path);
        File webappSources = webappSourcesMap.get(contextPath);

        if (webappSources == null) {
            return;
        }

        String outputFileString = context.getRealPath(path);

        if (outputFileString == null) {
            return;
        }

        @SuppressWarnings("unchecked")
        Set<String> copied = (Set<String>) request.getAttribute(COPIED_ATTRIBUTE);

        if (copied == null) {
            copied = new HashSet<String>();
            request.setAttribute(COPIED_ATTRIBUTE, copied);
        }

        if (copied.contains(outputFileString)) {
            return;

        } else {
            copied.add(outputFileString);
        }

        File sourceFile = new File(webappSources, path.substring(contextPath.length()).replace('/', File.separatorChar));
        File outputFile = new File(outputFileString);

        if (sourceFile.isDirectory()
                || outputFile.isDirectory()) {
            return;
        }

        if (sourceFile.exists()) {
            long sourceModified = sourceFile.lastModified();
            long sourceLength = sourceFile.length();

            if (!outputFile.exists()) {
                IoUtils.createParentDirectories(outputFile);

            } else if (sourceModified == outputFile.lastModified()
                    && sourceLength == outputFile.length()) {
                return;
            }

            IoUtils.copy(sourceFile, outputFile);

            if (!outputFile.setLastModified(sourceModified)) {
                LOGGER.debug("Can't set last modified on [{}] to [{}]!", outputFile, sourceModified);
            }

            LOGGER.info("Copied [{}]", sourceFile);

        } else if (outputFile.exists()
                && !outputFile.isDirectory()) {
            LOGGER.debug("[{}] disappeared!", sourceFile);
        }
    }

    /** {@linkplain SourceFilter} utility methods. */
    public static final class Static {

        /**
         * Returns the servlet path for pinging this web application to
         * make sure that it's running.
         *
         * @return Never {@code null}.
         */
        public static String getInterceptPingPath() {
            return StringUtils.addQueryParameters(getInterceptPath(), "action", "ping");
        }

        /** @deprecated Use {@link CodeUtils#getInstrumentation} instead. */
        @Deprecated
        public static Instrumentation getInstrumentation() {
            return CodeUtils.getInstrumentation();
        }
    }

    /** Installs an web application that can reload other web applications. */
    private class ReloaderInstaller extends HtmlWriter {

        private final HttpServletRequest request;
        private final HttpServletResponse response;

        public ReloaderInstaller(
                HttpServletRequest request,
                HttpServletResponse response)
                throws IOException {

            super(response.getWriter());
            this.request = request;
            this.response = response;
        }

        public void writeStart() throws IOException {
            response.setContentType("text/html");
            response.setCharacterEncoding("UTF-8");

            writeTag("!doctype html");
            writeStart("html");
                writeStart("head");

                    writeStart("title").writeHtml("Installing Reloader").writeEnd();

                    writeStart("link",
                            "href", JspUtils.getAbsolutePath(request, "/_resource/bootstrap/css/bootstrap.css"),
                            "rel", "stylesheet",
                            "type", "text/css");
                    writeStart("style", "type", "text/css");
                        write(".hero-unit { background: transparent; left: 0; margin: -72px 0 0 60px; padding: 0; position: absolute; top: 50%; }");
                        write(".hero-unit h1 { line-height: 1.33; }");
                    writeEnd();

                writeEnd();
                writeStart("body");

                    writeStart("div", "class", "hero-unit");
                        writeStart("h1");
                            writeHtml("Installing Reloader");
                        writeEnd();
                        try {
                            writeStart("ul", "class", "muted");
                            try {
                                flush();
                                install();
                                writeStart("script", "type", "text/javascript");
                                    write("location.href = '" + StringUtils.escapeJavaScript(
                                            JspUtils.getAbsolutePath(request, "")) + "';");
                                writeEnd();
                            } finally {
                                writeEnd();
                            }
                        } catch (RuntimeException ex) {
                            writeObject(ex);
                        }
                    writeEnd();

                writeEnd();
            writeEnd();
        }

        private void addProgress(Object... messageParts) throws IOException {
            writeStart("li");
                for (int i = 0, length = messageParts.length; i < length; ++ i) {
                    Object part = messageParts[i];
                    if (i % 2 == 0) {
                        writeHtml(part);
                    } else {
                        writeStart("em");
                            writeHtml(part);
                        writeEnd();
                    }
                }
            writeEnd();
            flush();
        }

        private void install() throws IOException {
            String catalinaBase = System.getProperty(CATALINA_BASE_PROPERTY);
            if (ObjectUtils.isBlank(catalinaBase)) {
                throw new IllegalStateException(String.format(
                        "[%s] system property isn't set!", CATALINA_BASE_PROPERTY));
            }

            URL metadataUrl = new URL(RELOADER_MAVEN_URL + "maven-metadata.xml");
            String metadata = IoUtils.toString(metadataUrl);
            addProgress("Looking for it using ", metadataUrl);

            Matcher timestampMatcher = TIMESTAMP_PATTERN.matcher(metadata);
            if (!timestampMatcher.find()) {
                throw new IllegalStateException("No timestamp in Maven metadata!");
            }

            Matcher buildNumberMatcher = BUILD_NUMBER_PATTERN.matcher(metadata);
            if (!buildNumberMatcher.find()) {
                throw new IllegalStateException("No build number in Maven metadata!");
            }

            File webappsDirectory = new File(catalinaBase, "webapps");
            File war = File.createTempFile("dari-reloader-", null, webappsDirectory);
            try {

                URL warUrl = new URL(
                        RELOADER_MAVEN_URL
                        + RELOADER_MAVEN_ARTIFACT_ID + "-"
                        + RELOADER_MAVEN_VERSION.replace("-SNAPSHOT", "") + "-"
                        + timestampMatcher.group(1) + "-"
                        + buildNumberMatcher.group(1) + ".war");
                addProgress("Downloading it from ", warUrl);
                addProgress("Saving it to ", war);

                InputStream warInput = warUrl.openStream();
                try {
                    FileOutputStream warOutput = new FileOutputStream(war);
                    try {
                        IoUtils.copy(warInput, warOutput);
                    } finally {
                        warOutput.close();
                    }
                } finally {
                    warInput.close();
                }

                String reloaderPath = getReloaderPath();
                reloaderPath = reloaderPath.substring(1, reloaderPath.length() - 1);
                File reloaderWar = new File(webappsDirectory, reloaderPath + ".war");

                IoUtils.delete(reloaderWar);
                IoUtils.delete(new File(catalinaBase,
                        "conf" + File.separator
                        + "Catalina" + File.separator
                        + "localhost" + File.separator
                        + reloaderPath + ".xml"));

                addProgress("Deploying it to ", "/" + reloaderPath);
                IoUtils.rename(war, reloaderWar);

                for (int i = 0; i < 20; ++ i) {
                    if (isReloaderAvailable(request)) {
                        addProgress("Finished!");
                        return;
                    }

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        break;
                    }
                }

                throw new IllegalStateException("Can't deploy!");

            } finally {
                IoUtils.delete(war);
            }
        }
    }

    private static class IsolatingResponse extends HttpServletResponseWrapper {

        public final String jsp;

        private final ServletOutputStream output = new IsolatingOutputStream();
        private final PrintWriter writer = new PrintWriter(new OutputStreamWriter(output, StandardCharsets.UTF_8));

        public IsolatingResponse(HttpServletResponse response, String jsp) {
            super(response);
            this.jsp = jsp;
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

    private static final class IsolatingOutputStream extends ServletOutputStream {

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

    private class AnalysisResultLogger extends JvmLogger {

        private List<AnalysisResult> getOrCreateAnalysisResults(Method method) {
            Class<?> c = method.getDeclaringClass();
            List<AnalysisResult> analysisResults = analysisResultsByClass.get(c);

            if (analysisResults == null) {
                analysisResults = new ArrayList<AnalysisResult>();
                analysisResultsByClass.put(c, analysisResults);
            }

            return analysisResults;
        }

        @Override
        public void warn(Method method, int line, String message) {
            LOGGER.warn(format(method, line, message));
            getOrCreateAnalysisResults(method).add(new AnalysisResult(Diagnostic.Kind.WARNING, method, line, message));
        }

        @Override
        public void error(Method method, int line, String message) {
            LOGGER.error(format(method, line, message));
            getOrCreateAnalysisResults(method).add(new AnalysisResult(Diagnostic.Kind.ERROR, method, line, message));
        }
    }

    private static class AnalysisResult {

        private final Diagnostic.Kind kind;
        private final Method method;
        private final int line;
        private final String message;

        public AnalysisResult(Diagnostic.Kind kind, Method method, int line, String message) {
            this.kind = kind;
            this.method = method;
            this.line = line;
            this.message = message;
        }

        public Diagnostic.Kind getKind() {
            return kind;
        }

        public Method getMethod() {
            return method;
        }

        public int getLine() {
            return line;
        }

        public String getMessage() {
            return message;
        }
    }
}
