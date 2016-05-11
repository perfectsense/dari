package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;

@DebugFilter.Path("code")
public class CodeDebugServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    public static final String INPUTS_ATTRIBUTE = CodeDebugServlet.class.getName() + ".inputs";
    public static final String INCLUDE_IMPORTS_SETTING = "dari/code/includeImports";
    public static final String EXCLUDE_IMPORTS_SETTING = "dari/code/excludeImports";

    private static final String WEB_INF_CLASSES_PATH = "/WEB-INF/classes/";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() { };

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        WebPageContext page = new WebPageContext(this, request, response);
        String action = page.param(String.class, "action");

        if ("run".equals(action)) {
            if (page.param(String.class, "isSave") != null) {
                doSave(page);

            } else {
                page.paramOrDefault(Type.class, "type", Type.JAVA).run(page);
            }

        } else {
            doEdit(page);
        }
    }

    private static File getFile(WebPageContext page) throws IOException {
        String file = page.param(String.class, "file");

        if (file == null) {
            String servletPath = page.param(String.class, "servletPath");

            if (servletPath != null) {
                file = page.getServletContext().getRealPath(servletPath);
            }
        }

        if (file == null) {
            return null;
        }

        File fileInstance = new File(file);

        if (!fileInstance.exists()) {
            IoUtils.createParentDirectories(fileInstance);
        }

        return fileInstance;
    }

    private void doSave(WebPageContext page) throws IOException, ServletException {
        if (!page.isFormPost()) {
            throw new IllegalArgumentException("Must post!");
        }

        new DebugFilter.PageWriter(page) { {
            File file = getFile(page);
            Preconditions.checkNotNull(file);
            String code = page.paramOrDefault(String.class, "code", "");

            try {
                CLASS_FOUND:
                    if (file.isDirectory()) {
                        Object result = evaluateJava(page.getServletContext(), code);

                        if (result instanceof Collection) {
                            for (Object item : (Collection<?>) result) {
                                if (item instanceof Class) {
                                    file = new File(file, ((Class<?>) item).getName().replace('.', File.separatorChar) + ".java");
                                    IoUtils.createParentDirectories(file);
                                    break CLASS_FOUND;
                                }
                            }
                        }

                        throw new IllegalArgumentException("Syntax error!");
                    }

                IoUtils.createFile(file);

                FileOutputStream fileOutput = new FileOutputStream(file);

                try {
                    fileOutput.write(code.replaceAll("(?:\r\n|[\r\n])", "\n").getBytes("UTF-8"));
                    writeStart("p", "class", "alert alert-success");
                        writeHtml("Saved Successfully! (");
                        writeObject(new Date());
                        writeHtml(")");
                    writeEnd();

                } finally {
                    fileOutput.close();
                }

            } catch (Exception ex) {
                writeStart("pre", "class", "alert alert-error");
                    writeObject(ex);
                writeEnd();
            }
        } };
    }

    private void doEdit(WebPageContext page) throws IOException, ServletException {
        final Type type = page.paramOrDefault(Type.class, "type", Type.JAVA);
        final File file = getFile(page);
        final StringBuilder codeBuilder = new StringBuilder();

        if (file != null) {
            if (file.exists()) {
                if (file.isDirectory()) {
                    // Can't edit a directory.

                } else {
                    codeBuilder.append(IoUtils.toString(file, StandardCharsets.UTF_8));
                }

            } else {
                String filePath = file.getPath();

                if (filePath.endsWith(".java")) {
                    filePath = filePath.substring(0, filePath.length() - 5);

                    for (File sourceDirectory : CodeUtils.getSourceDirectories()) {
                        String sourceDirectoryPath = sourceDirectory.getPath();

                        if (filePath.startsWith(sourceDirectoryPath)) {
                            String classPath = filePath.substring(sourceDirectoryPath.length());

                            if (classPath.startsWith(File.separator)) {
                                classPath = classPath.substring(1);
                            }

                            int lastSepAt = classPath.lastIndexOf(File.separatorChar);

                            if (lastSepAt < 0) {
                                codeBuilder.append("public class ");
                                codeBuilder.append(classPath);

                            } else {
                                codeBuilder.append("package ");
                                codeBuilder.append(classPath.substring(0, lastSepAt).replace(File.separatorChar, '.'));
                                codeBuilder.append(";\n\npublic class ");
                                codeBuilder.append(classPath.substring(lastSepAt + 1));
                            }

                            codeBuilder.append(" {\n}");
                            break;
                        }
                    }
                }
            }

        } else {
            Set<String> imports = findImports();

            imports.add("com.psddev.dari.db.*");
            imports.add("com.psddev.dari.util.*");
            imports.add("java.util.*");

            String includes = Settings.get(String.class, INCLUDE_IMPORTS_SETTING);

            if (!ObjectUtils.isBlank(includes)) {
                Collections.addAll(imports, includes.trim().split("\\s*,?\\s+"));
            }

            String excludes = Settings.get(String.class, EXCLUDE_IMPORTS_SETTING);

            if (!ObjectUtils.isBlank(excludes)) {
                for (String exclude : excludes.trim().split("\\s*,?\\s+")) {
                    imports.remove(exclude);
                }
            }

            for (String i : imports) {
                codeBuilder.append("import ");
                codeBuilder.append(i);
                codeBuilder.append(";\n");
            }

            codeBuilder.append('\n');
            codeBuilder.append("public class Code {\n");
            codeBuilder.append("    public static Object main() throws Throwable {\n");

            String query = page.param(String.class, "query");
            String objectClass = page.paramOrDefault(String.class, "objectClass", "Object");

            if (query == null) {
                codeBuilder.append("        return null;\n");

            } else {
                codeBuilder.append("        Query<").append(objectClass).append("> query = ").append(query).append(";\n");
                codeBuilder.append("        PaginatedResult<").append(objectClass).append("> result = query.select(0L, 10);\n");
                codeBuilder.append("        return result;\n");
            }

            codeBuilder.append("    }\n");
            codeBuilder.append("}\n");
        }

        new DebugFilter.PageWriter(page) { {
            List<Object> inputs = CodeDebugServlet.Static.getInputs(getServletContext());
            Object input = inputs == null || inputs.isEmpty() ? null : inputs.get(0);
            String name;

            if (file == null) {
                name = null;

            } else {
                name = file.toString();
                int slashAt = name.lastIndexOf('/');

                if (slashAt > -1) {
                    name = name.substring(slashAt + 1);
                }
            }

            startPage("Code Editor", name);
                writeStart("div", "class", "row-fluid");

                    if (input != null) {
                        writeStart("div", "class", "codeInput", "style", "bottom: 65px; position: fixed; top: 55px; width: 18%; z-index: 1000;");
                            writeStart("h2").writeHtml("Input").writeEnd();
                            writeStart("div", "style", "bottom: 0; overflow: auto; position: absolute; top: 38px; width: 100%;");
                                writeObject(input);
                            writeEnd();
                        writeEnd();
                        writeStart("style", "type", "text/css");
                            write(".codeInput pre { white-space: pre; word-break: normal; word-wrap: normal; }");
                        writeEnd();
                        writeStart("script", "type", "text/javascript");
                            write("$('.codeInput').hover(function() {");
                                write("$(this).css('width', '50%');");
                            write("}, function() {");
                                write("$(this).css('width', '18%');");
                            write("});");
                        writeEnd();
                    }

                    writeStart("div",
                            "class", input != null ? "span9" : "span12",
                            "style", input != null ? "margin-left: 20%" : null);
                        writeStart("form",
                                "action", page.url(null),
                                "class", "code",
                                "method", "post",
                                "style", "margin-bottom: 70px;",
                                "target", "result");
                            writeElement("input", "name", "action", "type", "hidden", "value", "run");
                            writeElement("input", "name", "type", "type", "hidden", "value", type);
                            writeElement("input", "name", "file", "type", "hidden", "value", file);
                            writeElement("input", "name", "jspPreviewUrl", "type", "hidden", "value", page.param(String.class, "jspPreviewUrl"));

                            writeStart("textarea", "name", "code");
                                writeHtml(codeBuilder);
                            writeEnd();
                            writeStart("div",
                                    "class", "form-actions",
                                    "style", "bottom: 0; left: 0; margin: 0; padding: 10px 20px; position:fixed; right: 0; z-index: 1000;");
                                writeElement("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                                writeStart("label", "class", "checkbox", "style", "display: inline-block; margin-left: 10px; white-space: nowrap;");
                                    writeElement("input", "name", "isLiveResult", "type", "checkbox");
                                    writeHtml("Live Result");
                                writeEnd();
                                writeStart("label", "style", "display: inline-block; margin-left: 10px; white-space: nowrap;", "title", "Shortcut: ?_vim=true");
                                    boolean vimMode = page.param(boolean.class, "_vim");
                                    writeStart("label", "class", "checkbox", "style", "display: inline-block; margin-left: 10px; white-space: nowrap;");
                                        writeElement("input", "name", "_vim", "type", "checkbox", "value", "true", vimMode ? "checked" : "_unchecked", "true");
                                        writeHtml("Vim Mode");
                                    writeEnd();
                                writeEnd();
                                writeElement("input",
                                        "class", "btn btn-success pull-right",
                                        "name", "isSave",
                                        "type", "submit",
                                        "value", "Save");
                            writeEnd();
                        writeEnd();

                        writeStart("div",
                                "class", "resultContainer",
                                "style",
                                        "background: rgba(255, 255, 255, 0.8);"
                                        + "border-color: rgba(0, 0, 0, 0.2);"
                                        + "border-style: solid;"
                                        + "border-width: 0 0 0 1px;"
                                        + "max-height: 45%;"
                                        + "top: 55px;"
                                        + "overflow: auto;"
                                        + "padding: 0px 20px 5px 10px;"
                                        + "position: fixed;"
                                        + "z-index: 3;"
                                        + "right: 0px;"
                                        + "width: 35%;");
                            writeStart("h2").writeHtml("Result").writeEnd();
                            writeStart("div", "class", "frame", "name", "result");
                            writeEnd();
                        writeEnd();

                        writeStart("script", "type", "text/javascript");
                            write("$('body').frame();");
                            write("var $codeForm = $('form.code');");

                            write("var lineMarkers = [ ];");
                            write("var columnMarkers = [ ];");
                            write("var codeMirror = CodeMirror.fromTextArea($('textarea')[0], {");
                                write("'indentUnit': 4,");
                                write("'lineNumbers': true,");
                                write("'lineWrapping': true,");
                                write("'matchBrackets': true,");
                                write("'mode': 'text/x-java',");
                                write("'onChange': $.throttle(1000, function() {");
                                    write("if ($codeForm.find(':checkbox[name=isLiveResult]').is(':checked')) {");
                                        write("$codeForm.submit();");
                                    write("}");
                                write("})");
                            write("});");
                            write("$('input[name=_vim]').change(function() {");
                                write("codeMirror.setOption('vimMode', $(this).is(':checked'));");
                            write("});");
                            write("$('input[name=_vim]').change();");

                            int line = page.param(int.class, "line");
                            if (line > 0) {
                                write("var line = ");
                                write(String.valueOf(line));
                                write(" - 1;");
                                write("codeMirror.setCursor(line);");
                                write("codeMirror.addLineClass(line, 'selected', 'selected');");
                                write("$(window).scrollTop(codeMirror.cursorCoords().top - $(window).height() / 2);");
                            }

                            write("var $resultContainer = $('.resultContainer');");
                            write("$resultContainer.find('.frame').bind('load', function() {");
                                write("$.each(lineMarkers, function() { codeMirror.clearMarker(this); codeMirror.setLineClass(this, null, null); });");
                                write("$.each(columnMarkers, function() { this.clear(); });");
                                write("var $frame = $(this).find('.syntaxErrors li').each(function() {");
                                    write("var $error = $(this);");
                                    write("var line = parseInt($error.attr('data-line')) - 1;");
                                    write("var column = parseInt($error.attr('data-column')) - 1;");
                                    write("if (line > -1 && column > -1) {");
                                        write("lineMarkers.push(codeMirror.setMarker(line, '!'));");
                                        write("codeMirror.setLineClass(line, 'errorLine', 'errorLine');");
                                        write("columnMarkers.push(codeMirror.markText({ 'line': line, 'ch': column }, { 'line': line, 'ch': column + 1 }, 'errorColumn'));");
                                    write("}");
                                write("});");
                            write("});");
                        writeEnd();

                    writeEnd();
                writeEnd();
            endPage();
        }

            @Override
            public void startBody(String... titles) throws IOException {
                writeStart("body");
                    writeStart("div", "class", "navbar navbar-fixed-top");
                        writeStart("div", "class", "navbar-inner");
                            writeStart("div", "class", "container-fluid");
                                writeStart("span", "class", "brand");
                                    writeStart("a", "href", DebugFilter.Static.getServletPath(page.getRequest(), ""));
                                        writeHtml("Dari");
                                    writeEnd();
                                    writeHtml("Code Editor \u2192 ");
                                writeEnd();

                                writeStart("form",
                                        "action", page.url(null),
                                        "method", "get",
                                        "style", "float: left; height: 40px; line-height: 40px; margin: 0; padding-left: 10px;");
                                    writeStart("select",
                                            "class", "span6",
                                            "name", "file",
                                            "onchange", "$(this).closest('form').submit();");
                                        writeStart("option", "value", "");
                                            writeHtml("PLAYGROUND");
                                        writeEnd();
                                        for (File sourceDirectory : CodeUtils.getSourceDirectories()) {
                                            writeStart("optgroup", "label", sourceDirectory);
                                                writeStart("option",
                                                        "selected", sourceDirectory.equals(file) ? "selected" : null,
                                                        "value", sourceDirectory);
                                                    writeHtml("NEW CLASS IN ").writeHtml(sourceDirectory);
                                                writeEnd();
                                                writeFileOption(file, sourceDirectory, sourceDirectory);
                                            writeEnd();
                                        }
                                    writeEnd();
                                writeEnd();

                                includeStylesheet("/_resource/chosen/chosen.css");
                                includeScript("/_resource/chosen/chosen.jquery.min.js");
                                writeStart("script", "type", "text/javascript");
                                    write("(function() {");
                                        write("$('select[name=file]').chosen({ 'search_contains': true });");
                                    write("})();");
                                writeEnd();
                            writeEnd();
                        writeEnd();
                    writeEnd();
                    writeStart("div", "class", "container-fluid", "style", "padding-top: 54px;");
            }

            private void writeFileOption(File file, File sourceDirectory, File source) throws IOException {
                if (source.isDirectory()) {
                    for (File child : source.listFiles()) {
                        writeFileOption(file, sourceDirectory, child);
                    }

                } else {
                    writeStart("option",
                            "selected", source.equals(file) ? "selected" : null,
                            "value", source);
                        writeHtml(source.toString().substring(sourceDirectory.toString().length()));
                    writeEnd();
                }
            }
        };
    }

    private Set<String> findImports() {
        Set<String> imports = new TreeSet<String>();
        addImports(imports, WEB_INF_CLASSES_PATH.length(), WEB_INF_CLASSES_PATH);
        return imports;
    }

    @SuppressWarnings("unchecked")
    private void addImports(Set<String> imports, int prefixLength, String path) {
        Set<String> resourcePaths = getServletContext().getResourcePaths(path);
        if (resourcePaths != null) {
            for (String subPath : resourcePaths) {
                if (subPath.endsWith("/")) {
                    addImports(imports, prefixLength, subPath);
                } else if (subPath.endsWith(".class")) {
                    imports.add(path.substring(prefixLength).replace('/', '.') + "*");
                }
            }
        }
    }

    private static Object evaluateJava(ServletContext context, String code) throws Exception {

        if (context != null) {
            CodeUtils.THREAD_DEFAULT_SERVLET_CONTEXT.push(context);
        }

        try {
            return CodeUtils.evaluateJava(code);

        } finally {
            if (context != null) {
                CodeUtils.THREAD_DEFAULT_SERVLET_CONTEXT.popOrError();
            }
        }
    }

    private enum Type {

        JAVA("Java") {
            @Override
            public void run(WebPageContext page) throws IOException, ServletException {

                new DebugFilter.PageWriter(page) { {
                    try {
                        Object result = evaluateJava(page.getServletContext(), page.paramOrDefault(String.class, "code", ""));

                        if (result instanceof DiagnosticCollector) {
                            writeStart("pre", "class", "alert alert-error");
                                writeHtml("Syntax error!\n\n");
                                writeStart("ol", "class", "syntaxErrors");
                                    for (Diagnostic<?> diagnostic : ((DiagnosticCollector<?>) result).getDiagnostics()) {
                                        if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
                                            writeStart("li", "data-line", diagnostic.getLineNumber(), "data-column", diagnostic.getColumnNumber());
                                                writeHtml(diagnostic.getMessage(null));
                                            writeEnd();
                                        }
                                    }
                                writeEnd();
                            writeEnd();

                        } else if (result instanceof Collection) {
                            for (Object item : (Collection<?>) result) {

                                if (item instanceof Class) {
                                    List<Object> inputs = CodeDebugServlet.Static.getInputs(page.getServletContext());
                                    Object input = inputs == null || inputs.isEmpty() ? null : inputs.get(0);

                                    if (input != null) {
                                        Class<?> inputClass = input.getClass();
                                        Class<?> itemClass = (Class<?>) item;

                                        for (Method method : ((Class<?>) item).getDeclaredMethods()) {
                                            Class<?>[] parameterClasses = method.getParameterTypes();

                                            if (parameterClasses.length == 1
                                                    && parameterClasses[0].isAssignableFrom(inputClass)
                                                    && method.getReturnType().isAssignableFrom(inputClass)) {
                                                Map<String, Object> inputMap = ObjectUtils.to(MAP_TYPE, input);
                                                Map<String, Object> processedMap = ObjectUtils.to(MAP_TYPE, method.invoke(itemClass.newInstance(), input));

                                                Set<String> keys = new HashSet<String>(inputMap.keySet());
                                                keys.addAll(processedMap.keySet());
                                                for (String key : keys) {
                                                    Object inputValue = inputMap.get(key);
                                                    Object processedValue = processedMap.get(key);
                                                    if (ObjectUtils.equals(inputValue, processedValue)) {
                                                        processedMap.remove(key);
                                                    }
                                                }

                                                result = processedMap;
                                                break;
                                            }
                                        }
                                    }
                                }
                            }

                            writeObject(result);

                        } else {
                            writeObject(result);
                        }

                    } catch (Exception ex) {
                        writeStart("pre", "class", "alert alert-error");
                            writeObject(ex);
                        writeEnd();
                    }
                } };
            }
        },

        JSP("JSP") {

            private final Map<String, Integer> draftIndexes = new HashMap<String, Integer>();

            @Override
            public void run(WebPageContext page) throws IOException, ServletException {

                new DebugFilter.PageWriter(page) { {
                    ServletContext context = page.getServletContext();
                    File file = getFile(page);
                    if (file == null) {
                        throw new IllegalArgumentException();
                    }

                    String servletPath = file.toString().substring(context.getRealPath("/").length() - 1).replace(File.separatorChar, '/');
                    String draft = "/WEB-INF/_draft" + servletPath;

                    int dotAt = draft.indexOf('.');
                    String extension;
                    if (dotAt < 0) {
                        extension = "";
                    } else {
                        extension = draft.substring(dotAt);
                        draft = draft.substring(0, dotAt);
                    }

                    synchronized (draftIndexes) {
                        Integer draftIndex = draftIndexes.get(draft);
                        if (draftIndex == null) {
                            draftIndex = 0;
                            draftIndexes.put(draft, draftIndex);
                        }
                        IoUtils.delete(new File(context.getRealPath(draft + draftIndex + extension)));
                        ++ draftIndex;
                        draftIndexes.put(draft, draftIndex);
                        draft = draft + draftIndex + extension;
                    }

                    String realDraft = context.getRealPath(draft);
                    File realDraftFile = new File(realDraft);
                    IoUtils.createParentDirectories(realDraftFile);
                    FileOutputStream realDraftOutput = new FileOutputStream(realDraftFile);
                    try {
                        realDraftOutput.write(page.paramOrDefault(String.class, "code", "").getBytes("UTF-8"));
                    } finally {
                        realDraftOutput.close();
                    }

                    page.getResponse().sendRedirect(
                            StringUtils.addQueryParameters(
                                    page.param(String.class, "jspPreviewUrl"),
                                    "_jsp", servletPath,
                                    "_draft", draft));
                } };
            }
        };

        private final String displayName;

        private Type(String displayName) {
            this.displayName = displayName;
        }

        public abstract void run(WebPageContext page) throws IOException, ServletException;

        // --- Object support ---

        @Override
        public String toString() {
            return displayName;
        }
    }

    /** {@link CodeDebugServlet} utility methods. */
    public static final class Static {

        @SuppressWarnings("unchecked")
        public static List<Object> getInputs(ServletContext context) {
            return (List<Object>) context.getAttribute(INPUTS_ATTRIBUTE);
        }

        public static void setInputs(ServletContext context, List<Object> inputs) {
            context.setAttribute(INPUTS_ATTRIBUTE, inputs);
        }
    }
}
