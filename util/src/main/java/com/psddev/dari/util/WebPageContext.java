package com.psddev.dari.util;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.UUID;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.jsp.PageContext;

import org.joda.time.DateTime;

/** Utility methods for working with a web page. */
public class WebPageContext extends HtmlWriter {

    private final Object page;
    private final ServletContext servletContext;
    private final HttpServletRequest request;
    private final HttpServletResponse response;
    private Converter converter;

    /**
     * Creates an instance based on the given {@code pageContext}.
     *
     * @param pageContext Can't be {@code null}.
     */
    public WebPageContext(PageContext pageContext) {
        ErrorUtils.errorIfNull(pageContext, "pageContext");

        this.page = pageContext.getPage();
        this.servletContext = pageContext.getServletContext();
        this.request = (HttpServletRequest) pageContext.getRequest();
        this.response = (HttpServletResponse) pageContext.getResponse();
        setDelegate(pageContext.getOut());
    }

    /**
     * Creates an instance based on the given {@code servlet},
     * {@code request}, {@code response}.
     *
     * @param servlet Can't be {@code null}.
     */
    public WebPageContext(
            Servlet servlet,
            HttpServletRequest request,
            HttpServletResponse response) {

        ErrorUtils.errorIfNull(servlet, "servlet");

        this.page = servlet;
        this.servletContext = servlet.getServletConfig().getServletContext();
        this.request = request;
        this.response = response;
    }

    /**
     * Creates an instance based on the given {@code servletContext},
     * {@code request}, and {@code response}.
     */
    public WebPageContext(
            ServletContext servletContext,
            HttpServletRequest request,
            HttpServletResponse response) {

        this.page = null;
        this.servletContext = servletContext;
        this.request = request;
        this.response = response;
    }

    /**
     * Returns the original servlet context.
     *
     * @return Never {@code null}.
     */
    public ServletContext getServletContext() {
        return servletContext;
    }

    /**
     * Returns the original request.
     *
     * @return Never {@code null}.
     */
    public HttpServletRequest getRequest() {
        return request;
    }

    /**
     * Returns the original response.
     *
     * @return Never {@code null}.
     */
    public HttpServletResponse getResponse() {
        return response;
    }

    @Override
    public Writer getDelegate() {
        Writer delegate = super.getDelegate();

        if (delegate != null) {
            return delegate;

        } else {
            try {
                HttpServletResponse response = getResponse();
                Writer writer;

                try {
                    writer = response.getWriter();

                } catch (IllegalStateException error) {
                    writer = new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8);
                }

                setDelegate(writer);
                return super.getDelegate();

            } catch (IOException error) {
                throw new IllegalStateException(error);
            }
        }
    }

    /**
     * Returns the original output writer.
     *
     * @return Never {@code null}.
     */
    public HtmlWriter getWriter() throws IOException {
        return this;
    }

    /**
     * Returns the converter.
     *
     * @return Never {@code null}.
     */
    public Converter getConverter() {
        if (converter == null) {
            converter = new Converter();
            converter.putAllStandardFunctions();
        }
        return converter;
    }

    /**
     * Sets the converter.
     *
     * @param converter If {@code null}, restores the default converter.
     */
    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    /**
     * Stringifies and writes all given {@code objects} to output writer.
     *
     * @param objects May be {@code null}.
     */
    public void write(Object... objects) throws IOException {
        if (objects != null) {
            for (Object object : objects) {
                getWriter().write(String.valueOf(object));
            }
        }
    }

    /**
     * Escapes the given {@code input}, or if it's {@code null} the given
     * {@code defaultValue}, so that it's safe to use in an HTML page.
     *
     * @param input Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     */
    public String h(Object input, Object defaultValue) {
        return StringUtils.escapeHtml(input != null
                ? input.toString()
                : String.valueOf(defaultValue));
    }

    /**
     * Escapes the given {@code input} so that it's safe to use in
     * an HTML page.
     *
     * @param input Can be {@code null}.
     * @return Empty string if the given {@code input} is {@code null}.
     */
    public String h(Object input) {
        return h(input, "");
    }

    /**
     * Escapes the given {@code input} so that it's safe to use in
     * JavaScript code.
     *
     * @param input Can be {@code null}.
     * @return Empty string if the given {@code input} is {@code null}.
     */
    public String js(Object input) {
        return input == null ? "" : StringUtils.escapeJavaScript(input.toString());
    }

    /**
     * Returns all request parameter names.
     *
     * @return Never {@code null}. Mutable.
     */
    public List<String> paramNamesList() {
        List<String> names = new ArrayList<String>();
        @SuppressWarnings("unchecked")
        Enumeration<String> namesEnumeration = getRequest().getParameterNames();

        if (namesEnumeration != null) {
            while (namesEnumeration.hasMoreElements()) {
                Object value = namesEnumeration.nextElement();
                names.add(value != null ? value.toString() : null);
            }
        }

        return names;
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the given {@code returnType}, or if not found or
     * is blank, the given {@code defaultValue}.
     *
     * @param returnType Can't be {@code null}.
     * @param name Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     * @return May be {@code null}.
     */
    public Object paramOrDefault(Type returnType, String name, Object defaultValue) {
        Converter converter = getConverter();
        String value = getRequest().getParameter(name);

        if (!ObjectUtils.isBlank(value)) {
            Object convertedValue = converter.convert(returnType, value);
            if (convertedValue != null) {
                return convertedValue;
            }
        }

        return converter.convert(returnType, defaultValue);
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the given {@code returnClass}, or if not found or
     * is blank, the given {@code defaultValue}.
     *
     * @param returnClass Can't be {@code null}.
     * @param name Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     * @return May be {@code null}.
     */
    @SuppressWarnings("unchecked")
    public <T> T paramOrDefault(Class<T> returnClass, String name, T defaultValue) {
        return (T) paramOrDefault((Type) returnClass, name, defaultValue);
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the type referenced by the given
     * {@code returnTypeReference}, or if not found or is blank,
     * the given {@code defaultValue}.
     *
     * @param returnTypeReference Can't be {@code null}.
     * @param name Can be {@code null}.
     * @param defaultValue Can be {@code null}.
     * @return May be {@code null}.
     */
    @SuppressWarnings("unchecked")
    public <T> T paramOrDefault(TypeReference<T> returnTypeReference, String name, T defaultValue) {
        return (T) paramOrDefault(returnTypeReference.getType(), name, defaultValue);
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the given {@code returnType}, or if not found or
     * is blank, {@code null}.
     *
     * @param returnType Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return May be {@code null}.
     */
    public Object param(Type returnType, String name) {
        return paramOrDefault(returnType, name, null);
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the given {@code returnClass}, or if not found or
     * is blank, {@code null}.
     *
     * @param returnClass Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return May be {@code null}.
     */
    public <T> T param(Class<T> returnClass, String name) {
        return paramOrDefault(returnClass, name, null);
    }

    /**
     * Returns the parameter associated with the given {@code name}
     * as an instance of the type referenced by the given
     * {@code returnTypeReference}, or if not found or is blank,
     * {@code null}.
     *
     * @param returnTypeReference Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return May be {@code null}.
     */
    public <T> T param(TypeReference<T> returnTypeReference, String name) {
        return paramOrDefault(returnTypeReference, name, null);
    }

    /**
     * Returns the parameters associated with the given {@code name}
     * as instances of the given {@code itemType}.
     *
     * @param itemType Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return Never {@code null}. Mutable. Empty list if the parameter
     * with the given {@code name} doesn't exist.
     */
    public List<Object> params(Type itemType, String name) {
        List<Object> convertedValues;
        String[] values = getRequest().getParameterValues(name);

        if (values == null) {
            convertedValues = new ArrayList<Object>(1);

        } else {
            Converter converter = getConverter();
            int length = values.length;
            convertedValues = new ArrayList<Object>(length);

            for (int i = 0; i < length; ++ i) {
                convertedValues.add(converter.convert(itemType, values[i]));
            }
        }

        return convertedValues;
    }

    /**
     * Returns the parameters associated with the given {@code name}
     * as instances of the given {@code itemClass}.
     *
     * @param itemClass Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return Never {@code null}. Mutable. Empty list if the parameter
     * with the given {@code name} doesn't exist.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> params(Class<T> itemClass, String name) {
        return (List<T>) params((Type) itemClass, name);
    }

    /**
     * Returns the parameters associated with the given {@code name}
     * as instances of the type referenced by the given
     * {@code itemTypeReference}.
     *
     * @param itemTypeReference Can't be {@code null}.
     * @param name Can be {@code null}.
     * @return Never {@code null}. Mutable. Empty list if the parameter
     * with the given {@code name} doesn't exist.
     */
    @SuppressWarnings("unchecked")
    public <T> List<T> params(TypeReference<T> itemTypeReference, String name) {
        return (List<T>) params(itemTypeReference.getType(), name);
    }

    // --- JspUtils bridge ---

    /** @see JspUtils#createId */
    public String createId() {
        return JspUtils.createId(getRequest());
    }

    /** @see JspUtils#finish */
    public void finish() {
        JspUtils.finish(getRequest());
    }

    /** @see JspUtils#forward */
    public void forward(String path, Object... parameters) throws IOException, ServletException {
        JspUtils.forward(getRequest(), getResponse(), path, parameters);
    }

    /** @see JspUtils#getAbsolutePath */
    public String absoluteUrl(Object path, Object... parameters) {
        String pathString = path == null ? null : path.toString();
        return JspUtils.getAbsolutePath(getRequest(), pathString, parameters);
    }

    /** @see JspUtils#getCookie */
    public Cookie getCookie(String name) {
        return JspUtils.getCookie(getRequest(), name);
    }

    /** @see JspUtils#getEmbeddedAbsolutePath */
    public String url(Object path, Object... parameters) {
        String pathString = path == null ? null : path.toString();
        return JspUtils.getEmbeddedAbsolutePath(getServletContext(), getRequest(), pathString, parameters);
    }

    /** @see JspUtils#getId */
    public String getId() {
        return JspUtils.getId(getRequest());
    }

    /** @see JspUtils#getOriginalContextPath */
    public String getOriginalContextPath() {
        return JspUtils.getOriginalContextPath(getRequest());
    }

    /** @see JspUtils#getOriginalServletPath */
    public String getOriginalServletPath() {
        return JspUtils.getOriginalServletPath(getRequest());
    }

    /** @see JspUtils#getOriginalPathInfo */
    public String getOriginalPathInfo() {
        return JspUtils.getOriginalPathInfo(getRequest());
    }

    /** @see JspUtils#getOriginalQueryString */
    public String getOriginalQueryString() {
        return JspUtils.getOriginalQueryString(getRequest());
    }

    /** @see JspUtils#includeEmbedded */
    public boolean include(String path, Object... attributes) throws IOException, ServletException {
        return JspUtils.includeEmbedded(
                getServletContext(),
                getRequest(),
                getResponse(),
                getWriter(),
                path,
                attributes);
    }

    /** @see JspUtils#isAjaxRequest */
    public boolean isAjaxRequest() {
        return JspUtils.isAjaxRequest(getRequest());
    }

    /** @see JspUtils#isFinished */
    public boolean isFinished() {
        return JspUtils.isFinished(getRequest(), getResponse());
    }

    /** @see JspUtils#isFormPost */
    public boolean isFormPost() {
        return JspUtils.isFormPost(getRequest());
    }

    /** @see JspUtils#isForwarded */
    public boolean isForwarded() {
        return JspUtils.isForwarded(getRequest());
    }

    /** @see JspUtils#isIncluded */
    public boolean isIncluded() {
        return JspUtils.isIncluded(getRequest());
    }

    /** @see JspUtils#proxy */
    public void proxy(Object url, Object... parameters) throws IOException {
        JspUtils.proxy(getRequest(), getResponse(), getWriter(), url, parameters);
    }

    /** @see JspUtils#redirectEmbedded */
    public void redirect(Object path, Object... parameters) throws IOException {
        JspUtils.redirectEmbedded(getServletContext(), getRequest(), getResponse(), path, parameters);
    }

    // --- Deprecated ---

    /**
     * Returns the original page object (instance of {@link Servlet}
     * in a servlet environment).
     *
     * @return May be {@code null}.
     * @deprecated No replacement.
     */
    @Deprecated
    public Object getPage() {
        return this.page;
    }

    /** @deprecated Use {@link DateUtils#toString} instead. */
    @Deprecated
    public String dt(Date date, String format) {
        return new DateTime(date).toString(format);
    }

    /** @deprecated No replacement. */
    @Deprecated
    public String hb(Object input, Object defaultValue) {
        return input != null
                ? StringUtils.escapeHtmlAndBreak(input.toString())
                : defaultValue.toString();
    }

    /** @deprecated No replacement. */
    @Deprecated
    public String hb(Object input) {
        return hb(input, "");
    }

    /** @deprecated Use {@link #paramNamesList} instead. */
    @Deprecated
    public String[] paramNames(String prefix) {
        List<String> names = new ArrayList<String>();
        for (
                @SuppressWarnings("unchecked")
                Enumeration<String> e = getRequest().getParameterNames();
                e.hasMoreElements();
                ) {
            String name = e.nextElement();
            if (prefix == null || name.startsWith(prefix)) {
                names.add(name);
            }
        }
        return names.toArray(new String[names.size()]);
    }

    /** @deprecated Use {@link #paramNamesList} instead. */
    @Deprecated
    public String[] paramNames() {
        return paramNames(null);
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public boolean boolParam(String name) {
        return Boolean.parseBoolean(getRequest().getParameter(name));
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public boolean[] boolParams(String name) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                boolean[] values = new boolean[len];
                for (int i = 0; i < len; i ++) {
                    values[i] = Boolean.parseBoolean(stringValues[i]);
                }
                return values;
            }
        }
        return new boolean[0];
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public Date dateParam(String name, Date defaultValue) {
        Date value = ObjectUtils.to(Date.class, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public Date dateParam(String name) {
        return dateParam(name, null);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Date[] dateParams(String name, Date defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                Date[] values = new Date[len];
                for (int i = 0; i < len; i ++) {
                    Date value = ObjectUtils.to(Date.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new Date[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Date[] dateParams(String name) {
        return dateParams(name, null);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public <T extends Enum<T>> T enumParam(Class<T> type, String name, T defaultValue) {
        T value = ObjectUtils.to(type, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public <T extends Enum<T>> T[] enumParams(Class<T> type, String name, T defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        int len = stringValues != null ? stringValues.length : 0;
        @SuppressWarnings("unchecked")
        T[] values = (T[]) Array.newInstance(type, len);
        for (int i = 0; i < len; i ++) {
            T value = ObjectUtils.to(type, stringValues[i]);
            values[i] = value != null ? value : defaultValue;
        }
        return values;
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public Double doubleParam(String name, Double defaultValue) {
        Double value = ObjectUtils.to(Double.class, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public Double doubleParam(String name) {
        return doubleParam(name, 0.0);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Double[] doubleParams(String name, Double defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                Double[] values = new Double[len];
                for (int i = 0; i < len; i ++) {
                    Double value = ObjectUtils.to(Double.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new Double[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Double[] doubleParams(String name) {
        return doubleParams(name, 0.0);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public Float floatParam(String name, Float defaultValue) {
        Float value = ObjectUtils.to(Float.class, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public Float floatParam(String name) {
        return floatParam(name, 0.0f);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Float[] floatParams(String name, Float defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                Float[] values = new Float[len];
                for (int i = 0; i < len; i ++) {
                    Float value = ObjectUtils.to(Float.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new Float[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Float[] floatParams(String name) {
        return floatParams(name, 0.0f);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public Integer intParam(String name, Integer defaultValue) {
        Integer value = ObjectUtils.to(Integer.class, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public Integer intParam(String name) {
        return intParam(name, 0);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Integer[] intParams(String name, Integer defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                Integer[] values = new Integer[len];
                for (int i = 0; i < len; i ++) {
                    Integer value = ObjectUtils.to(Integer.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new Integer[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Integer[] intParams(String name) {
        return intParams(name, 0);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public Long longParam(String name, Long defaultValue) {
        Long value = ObjectUtils.to(Long.class, getRequest().getParameter(name));
        return value != null ? value : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public Long longParam(String name) {
        return longParam(name, 0L);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Long[] longParams(String name, Long defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                Long[] values = new Long[len];
                for (int i = 0; i < len; i ++) {
                    Long value = ObjectUtils.to(Long.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new Long[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public Long[] longParams(String name) {
        return longParams(name, 0L);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public String param(String name, String defaultValue) {
        String value = getRequest().getParameter(name);
        return value == null || value.length() == 0 ? defaultValue : value;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public String param(String name) {
        return param(name, null);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public String[] params(String name, String defaultValue) {
        String[] values = getRequest().getParameterValues(name);
        if (values != null) {
            int len = values.length;
            if (len > 0) {
                for (int i = 0; i < len; i ++) {
                    String value = values[i];
                    values[i] = value == null || value.length() == 0 ? defaultValue : value;
                }
                return values;
            }
        }
        return new String[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public String[] params(String name) {
        return params(name, null);
    }

    /** @deprecated Use {@link #paramOrDefault} instead. */
    @Deprecated
    public UUID uuidParam(String name, UUID defaultValue) {
        UUID uuid = ObjectUtils.to(UUID.class, getRequest().getParameter(name));
        return uuid != null ? uuid : defaultValue;
    }

    /** @deprecated Use {@link #param} instead. */
    @Deprecated
    public UUID uuidParam(String name) {
        return uuidParam(name, null);
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public UUID[] uuidParams(String name, UUID defaultValue) {
        String[] stringValues = getRequest().getParameterValues(name);
        if (stringValues != null) {
            int len = stringValues.length;
            if (len > 0) {
                UUID[] values = new UUID[len];
                for (int i = 0; i < len; i ++) {
                    UUID value = ObjectUtils.to(UUID.class, stringValues[i]);
                    values[i] = value != null ? value : defaultValue;
                }
                return values;
            }
        }
        return new UUID[0];
    }

    /** @deprecated Use {@link #params} instead. */
    @Deprecated
    public UUID[] uuidParams(String name) {
        return uuidParams(name, null);
    }
}
