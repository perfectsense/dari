package com.psddev.dari.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Function for {@link HtmlWriter#object formatting an object} to be
 * written to an {@link HtmlWriter}.
 */
public interface HtmlFormatter<T> {

    /**
     * Formats the given {@code objects} and writes the output to the
     * given {@code writer}.
     */
    public void format(HtmlWriter writer, T object) throws IOException;

    // --- Standard formatters ---

    /** Standard HTML object formatter for classes. */
    @SuppressWarnings("rawtypes")
    public static final HtmlFormatter<Class> CLASS = new HtmlFormatter<Class>() {

        @Override
        public void format(HtmlWriter writer, Class rawClassInstance) throws IOException {
            Class<?> classInstance = rawClassInstance;
            writer.writeStart("p");
                writer.writeStart("code").writeHtml(classInstance).writeEnd();
            writer.writeEnd();

            Field[] fields = classInstance.getDeclaredFields();
            Arrays.sort(fields, new Comparator<Field>() {
                @Override
                public int compare(Field x, Field y) {
                    return x.getName().compareTo(y.getName());
                }
            });

            Method[] methods = classInstance.getDeclaredMethods();
            Arrays.sort(methods, new Comparator<Method>() {
                @Override
                public int compare(Method x, Method y) {
                    return x.getName().compareTo(y.getName());
                }
            });

            writer.writeStart("ul");
                if (fields.length > 0) {
                    writer.writeStart("li").writeHtml("Fields:").writeStart("ul");
                        for (Field field : fields) {
                            writer.writeStart("li").writeHtml(field.getName()).writeEnd();
                        }
                    writer.writeEnd().writeEnd();
                }
                if (methods.length > 0) {
                    writer.writeStart("li").writeHtml("Methods:").writeStart("ul");
                        for (Method method : methods) {
                            writer.writeStart("li").writeHtml(method.getName()).writeEnd();
                        }
                    writer.writeEnd().writeEnd();
                }
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for collections. */
    @SuppressWarnings("rawtypes")
    public static final HtmlFormatter<Collection> COLLECTION = new HtmlFormatter<Collection>() {

        @Override
        public void format(HtmlWriter writer, Collection collection) throws IOException {
            writer.writeStart("p");
                writer.writeStart("code").writeHtml(collection.getClass().getName()).writeEnd().writeHtml(" containing ");
                writer.writeStart("strong").writeHtml(collection.size()).writeEnd().writeHtml(" items:");
            writer.writeEnd();

            writer.writeStart(collection instanceof List ? "ol" : "ul");
                for (Object item : collection) {
                    writer.writeStart("li").writeObject(item).writeEnd();
                }
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for dates. */
    public static final HtmlFormatter<Date> DATE = new HtmlFormatter<Date>() {
        @Override
        public void format(HtmlWriter writer, Date date) throws IOException {
            writer.writeHtml(date);
        }
    };

    /** Standard HTML object formatter for enums. */
    @SuppressWarnings("rawtypes")
    public static final HtmlFormatter<Enum> ENUM = new HtmlFormatter<Enum>() {
        @Override
        public void format(HtmlWriter writer, Enum enumObject) throws IOException {
            writer.writeStart("code");
                writer.writeHtml(enumObject.getClass().getName());
                writer.writeHtml('.');
                writer.writeHtml(((Enum<?>) enumObject).name());
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for floating point numbers. */
    public static final HtmlFormatter<Number> FLOATING_POINT = new HtmlFormatter<Number>() {
        @Override
        public void format(HtmlWriter writer, Number number) throws IOException {
            writer.writeStart("abbr", "title", number);
                writer.writeHtml(String.format("%,.2f", number));
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for maps. */
    @SuppressWarnings("rawtypes")
    public static final HtmlFormatter<Map> MAP = new HtmlFormatter<Map>() {

        @Override
        public void format(HtmlWriter writer, Map map) throws IOException {
            writer.writeStart("p");
                writer.writeStart("code").writeHtml(map.getClass().getName()).writeEnd().writeHtml(" containing ");
                writer.writeStart("strong").writeHtml(map.size()).writeEnd().writeHtml(" items:");
            writer.writeEnd();

            writer.writeStart("dl");
                for (Map.Entry<?, ?> entry : ((Map<?, ?>) map).entrySet()) {
                    writer.writeStart("dt").writeObject(entry.getKey()).writeEnd();
                    writer.writeStart("dd").writeObject(entry.getValue()).writeEnd();
                }
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for a {@code null}. */
    public static final HtmlFormatter<Void> NULL = new HtmlFormatter<Void>() {
        @Override
        public void format(HtmlWriter writer, Void nullObject) throws IOException {
            writer.writeStart("code").writeHtml("null").writeEnd();
        }
    };

    /** Standard HTML object formatter for numbers. */
    public static final HtmlFormatter<Number> NUMBER = new HtmlFormatter<Number>() {
        @Override
        public void format(HtmlWriter writer, Number number) throws IOException {
            if (number instanceof BigDecimal) {
                FLOATING_POINT.format(writer, number);
            } else {
                writer.writeHtml(String.format("%,d", number));
            }
        }
    };

    /** Standard HTML object formatter for paginated results. */
    @SuppressWarnings("rawtypes")
    public static final HtmlFormatter<PaginatedResult> PAGINATED_RESULT = new HtmlFormatter<PaginatedResult>() {

        @Override
        public void format(HtmlWriter writer, PaginatedResult rawResult) throws IOException {
            PaginatedResult<?> result = rawResult;

            writer.writeStart("p");
                writer.writeStart("code").writeHtml(result.getClass().getName()).writeEnd();
                writer.writeHtml(' ');
                writer.writeStart("strong").writeHtml(result.getFirstItemIndex()).writeEnd();
                writer.writeHtml(" to ");
                writer.writeStart("strong").writeHtml(result.getLastItemIndex()).writeEnd();
                writer.writeHtml(" of ");
                writer.writeStart("strong").writeHtml(result.getCount()).writeEnd();
            writer.writeEnd();

            writer.writeStart("ol");
                for (Object item : result.getItems()) {
                    writer.writeStart("li").writeObject(item).writeEnd();
                }
            writer.writeEnd();
        }
    };

    /** Standard HTML object formatter for stack trace elements. */
    public static final HtmlFormatter<StackTraceElement> STACK_TRACE_ELEMENT = new HtmlFormatter<StackTraceElement>() {

        @Override
        public void format(HtmlWriter writer, StackTraceElement element) throws IOException {
            String className = element.getClassName();
            int lineNumber = element.getLineNumber();
            String cssClass = className != null
                    && (className.startsWith("com.psddev.dari.util.AbstractFilter")
                    || className.startsWith("org.apache.catalina.core.ApplicationFilterChain"))
                    ? "muted" : null;

            String jspServletPath = CodeUtils.getJspServletPath(className);
            if (jspServletPath != null) {
                jspServletPath = StringUtils.ensureStart(jspServletPath, "/");
                lineNumber = CodeUtils.getJspLineNumber(className, lineNumber);
                writer.writeHtml(jspServletPath);
                writer.writeHtml(':');
                writer.writeHtml(lineNumber);

            } else {
                File source = CodeUtils.getSource(className);
                if (source == null) {
                    writer.writeStart("span", "class", cssClass);
                        writer.writeHtml(element);
                    writer.writeEnd();

                } else {
                    int dotAt = className.lastIndexOf('.');
                    if (dotAt > -1) {
                        className = className.substring(dotAt + 1);
                    }
                    writer.writeHtml(className);
                    writer.writeHtml('.');
                    writer.writeHtml(element.getMethodName());
                    writer.writeHtml(':');
                    writer.writeHtml(lineNumber);
                }
            }
        }
    };

    /** Standard HTML object formatter for throwables. */
    public static final HtmlFormatter<Throwable> THROWABLE = new HtmlFormatter<Throwable>() {

        @Override
        public void format(HtmlWriter writer, Throwable throwable) throws IOException {
            Throwable cause = throwable.getCause();
            if (cause != null) {
                writer.writeObject(cause);
            }

            writer.writeStart("div", "class", "dari-throwable");
                String message = throwable.getMessage();

                if (ObjectUtils.isBlank(message)) {
                    writer.writeHtml(throwable.getClass().getName());

                } else {
                    writer.writeHtml(message);
                    writer.writeHtml(" (");
                    writer.writeHtml(throwable.getClass().getName());
                    writer.writeHtml(")");
                }

                writer.writeStart("ul", "class", "dari-stack-trace");
                    for (StackTraceElement element : throwable.getStackTrace()) {
                        writer.writeStart("li").writeObject(element).writeEnd();
                    }
                writer.writeEnd();
            writer.writeEnd();
        }
    };

    // --- Optional ---

    public static final Class<?> JASPER_EXCEPTION_CLASS = ObjectUtils.getClassByName("org.apache.jasper.JasperException");

    public static final HtmlFormatter<Object> JASPER_EXCEPTION = new HtmlFormatter<Object>() {

        private final Pattern errorPattern = Pattern.compile("(line: (\\d+) in the jsp(?s:.)*?)(\\2:(?-s:.)*(?m:$))");
        private final Pattern stackTracePattern = Pattern.compile("\\s*Stacktrace:\\s*$");

        @Override
        public void format(HtmlWriter writer, Object object) throws IOException {
            Throwable throwable = (Throwable) object;
            Throwable cause = throwable.getCause();
            String message = throwable.getMessage();

            if (cause != null) {
                writer.writeObject(cause);
                if (message != null && message.startsWith(cause.getClass().getName())) {
                    return;
                }
            }

            writer.writeStart("div", "class", "dari-throwable");
                message = StringUtils.escapeHtml(message);

                // Highlight the line mentioned in the error message.
                StringBuilder messageBuilder = new StringBuilder();
                Matcher errorMatcher = errorPattern.matcher(message);
                int lastEnd = 0;
                for (; errorMatcher.find(); lastEnd = errorMatcher.end()) {
                    messageBuilder.append(message.substring(lastEnd, errorMatcher.start()));
                    messageBuilder.append(errorMatcher.group(1));
                    messageBuilder.append("<span style=\"background: #B94A48;");
                    messageBuilder.append(" color: white;");
                    messageBuilder.append(" padding: 2px 0;");
                    messageBuilder.append(" text-shadow: none;\">");
                    messageBuilder.append(errorMatcher.group(3));
                    messageBuilder.append("</span>");
                }
                messageBuilder.append(message.substring(lastEnd));

                message = messageBuilder.toString();
                message = stackTracePattern.matcher(message).replaceFirst("");
                writer.write(message);
            writer.writeEnd();
        }
    };
}
