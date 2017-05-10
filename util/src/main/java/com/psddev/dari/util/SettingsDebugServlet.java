package com.psddev.dari.util;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import com.psddev.dari.util.sa.Jvm;
import com.psddev.dari.util.sa.JvmMethodListener;
import com.psddev.dari.util.sa.JvmObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Debug servlet for inspecting {@linkplain Settings global settings}. */
public class SettingsDebugServlet extends DebugServlet {

    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SettingsDebugServlet.class);

    @Override
    public String getName() {
        return "Settings";
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("settings");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Settings");

            writeStart("h2");
                writeHtml("Values");
            writeEnd();

            writeStart("table", "class", "table table-condensed table-striped");
                writeStart("thead");
                    writeStart("tr");
                        writeStart("th").writeHtml("Key").writeEnd();
                        writeStart("th").writeHtml("Value").writeEnd();
                        writeStart("th").writeHtml("Class").writeEnd();
                    writeEnd();
                writeEnd();

                writeStart("tbody");
                    for (Map.Entry<String, Object> entry : flatten(Settings.asMap()).entrySet()) {
                        String key = entry.getKey();
                        String keyLowered = key.toLowerCase(Locale.ENGLISH);
                        Object value = entry.getValue();

                        writeStart("tr");
                            writeStart("td").writeHtml(key).writeEnd();

                            writeStart("td");
                                if (keyLowered.contains("password")
                                        || keyLowered.contains("secret")) {
                                    writeStart("span", "class", "label label-warning").writeHtml("Hidden").writeEnd();
                                } else {
                                    writeHtml(value);
                                }
                            writeEnd();

                            writeStart("td");
                                writeHtml(value != null ? value.getClass().getName() : "N/A");
                            writeEnd();
                        writeEnd();
                    }
                writeEnd();
            writeEnd();

            Jvm jvm = new Jvm();
            List<Usage> usages = new ArrayList<>();
            SettingsMethodListener listener = new SettingsMethodListener(usages);

            Stream.of(Settings.class.getMethods())
                    .filter(m -> Modifier.isStatic(m.getModifiers()) && m.getName().startsWith("get"))
                    .forEach(m -> jvm.addMethodListener(m, listener));

            for (Class<?> objectClass : ClassFinder.findClasses(Object.class)) {
                if (!Settings.class.isAssignableFrom(objectClass)) {
                    try {
                        jvm.analyze(objectClass);

                    } catch (Exception error) {
                        // Ignore errors.
                    }
                }
            }

            if (!usages.isEmpty()) {
                Collections.sort(usages);

                writeStart("h2");
                    writeHtml("Usages");
                writeEnd();

                writeStart("table", "class", "table table-condensed table-striped");
                    writeStart("thead");
                        writeStart("tr");
                            writeStart("th").writeHtml("Key").writeEnd();
                            writeStart("th").writeHtml("Default").writeEnd();
                            writeStart("th").writeHtml("Method").writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("tbody");
                        for (Usage usage : usages) {
                            usage.writeRowHtml(this);
                        }
                    writeEnd();
                writeEnd();
            }

            endPage();
        } };
    }

    private Map<String, Object> flatten(Map<String, Object> map) {
        Map<String, Object> flattened = new TreeMap<String, Object>(String.CASE_INSENSITIVE_ORDER);
        collectFlattenedValues(flattened, null, map);
        return flattened;
    }

    private void collectFlattenedValues(Map<String, Object> flattened, String key, Object value) {
        if (value instanceof Map) {
            String prefix = key == null ? "" : key + "/";
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                collectFlattenedValues(flattened, prefix + entry.getKey(), entry.getValue());
            }

        } else if (value instanceof DataSource) {
            try {
                Map<String, Object> map = new TreeMap<String, Object>();

                for (PropertyDescriptor desc : Introspector.getBeanInfo(value.getClass()).getPropertyDescriptors()) {
                    String name = desc.getName();
                    Throwable error = null;

                    try {
                        Method getter = desc.getReadMethod();
                        Method setter = desc.getWriteMethod();

                        if (getter != null && setter != null) {
                            getter.setAccessible(true);
                            map.put(name, getter.invoke(value));
                        }

                    } catch (IllegalAccessException e) {
                        error = e;
                    } catch (InvocationTargetException e) {
                        error = e.getCause();
                    } catch (RuntimeException e) {
                        error = e;
                    }

                    if (error != null) {
                        LOGGER.debug(String.format(
                                "Can't read [%s] from an instance of [%s] stored in [%s]!",
                                name, value.getClass(), key),
                                error);
                    }
                }

                collectFlattenedValues(flattened, key, map);

            } catch (IntrospectionException error) {
                flattened.put(key, value);
            }

        } else {
            flattened.put(key, value);
        }
    }

    private static class SettingsMethodListener extends JvmMethodListener {

        private final List<Usage> usages;

        public SettingsMethodListener(List<Usage> usages) {
            this.usages = usages;
        }

        @Override
        public void onInvocation(
                Method callingMethod,
                int callingLine,
                Method calledMethod,
                int calledLine,
                JvmObject calledObject,
                List<JvmObject> calledArguments,
                JvmObject returnedObject) {

            Class<?>[] parameterClasses = calledMethod.getParameterTypes();
            String key = null;
            String defaultValue = null;

            for (int i = 0, length = parameterClasses.length; i < length; ++ i) {
                Class<?> parameterClass = parameterClasses[i];

                if (String.class.equals(parameterClass)) {
                    if (key == null) {
                        key = calledArguments.get(i).toString();
                    }

                } else if (Object.class.equals(parameterClass)) {
                    defaultValue = calledArguments.get(i).toString();
                }
            }

            usages.add(new Usage(
                    callingMethod.getDeclaringClass().getName(),
                    callingMethod.getName(),
                    callingLine,
                    key,
                    defaultValue));
        }
    }

    private static class Usage implements Comparable<Usage> {

        private final String className;
        private final String methodName;
        private final int lineNumber;
        private final String key;
        private final Object defaultValue;

        public Usage(String className, String methodName, int lineNumber, String key, Object defaultValue) {
            this.className = className;
            this.methodName = methodName;
            this.lineNumber = lineNumber;
            this.key = key;
            this.defaultValue = defaultValue;
        }

        public void writeRowHtml(DebugFilter.PageWriter writer) throws IOException {
            File source = CodeUtils.getSource(className);

            writer.writeStart("tr");
                writer.writeStart("td", "style", "word-break:break-all;");
                    writer.writeHtml(key);
                writer.writeEnd();

                writer.writeStart("td", "style", "word-break:break-all;");
                    writer.writeHtml(defaultValue);
                writer.writeEnd();

                writer.writeStart("td");
                    if (source != null) {
                        writer.writeStart("a",
                                "target", "_blank",
                                "href", DebugFilter.Static.getServletPath(writer.page.getRequest(), "code",
                                        "file", source,
                                        "line", lineNumber));
                            writer.writeHtml(className);
                            writer.writeHtml('.');
                            writer.writeHtml(methodName);
                            writer.writeHtml(':');
                            writer.writeHtml(lineNumber);
                        writer.writeEnd();

                    } else {
                        writer.writeHtml(className);
                        writer.writeHtml('.');
                        writer.writeHtml(methodName);
                        writer.writeHtml(':');
                        writer.writeHtml(lineNumber);
                    }
                writer.writeEnd();
            writer.writeEnd();
        }

        @Override
        public int compareTo(Usage other) {
            return ObjectUtils.compare(key, other.key, true);
        }
    }
}
