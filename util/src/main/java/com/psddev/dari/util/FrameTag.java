package com.psddev.dari.util;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.BodyTagSupport;
import javax.servlet.jsp.tagext.DynamicAttributes;

/**
 * @deprecated 2015-07-23 - No replacement.
 */
@Deprecated
@SuppressWarnings("serial")
public class FrameTag extends BodyTagSupport implements DynamicAttributes {

    protected static final String ATTRIBUTE_PREFIX = FrameTag.class.getName() + ".";
    protected static final String CURRENT_NAME_PREFIX = ATTRIBUTE_PREFIX + "currentName";
    protected static final String JS_INCLUDED_PREFIX = ATTRIBUTE_PREFIX + "jsIncluded";

    public enum InsertionMode {
        replace, // default
        append,
        prepend;
    }

    private String tagName;
    private String name;
    private boolean lazy;
    private InsertionMode mode;
    private final Map<String, Object> attributes = new CompactMap<String, Object>();
    private transient String oldName;

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setLazy(boolean lazy) {
        this.lazy = lazy;
    }

    public void setMode(InsertionMode mode) {
        this.mode = mode;
    }

    // --- DynamicAttributes support ---

    @Override
    public void setDynamicAttribute(String uri, String localName, Object value) {
        if (value != null) {
            attributes.put(localName, value);
        }
    }

    // --- TagSupport support ---

    private boolean isRenderingFrame(HttpServletRequest request) {
        return request.getParameter(FrameFilter.PATH_PARAMETER) != null
                && name.equals(request.getParameter(FrameFilter.NAME_PARAMETER));
    }

    private void startFrame(HttpServletRequest request, HtmlWriter writer, String... classNames) throws IOException {
        StringBuilder fullClassName = new StringBuilder("dari-frame");

        if (classNames != null) {
            int length = classNames.length;

            if (length > 0) {
                for (int i = 0; i < length; ++ i) {
                    fullClassName.append(" dari-frame-");
                    fullClassName.append(classNames[i]);
                }
            }
        }

        writer.writeStart(ObjectUtils.firstNonNull(tagName, "div"),
                attributes,
                "class", fullClassName.toString(),
                "name", name,
                "data-insertion-mode", mode != null ? mode : InsertionMode.replace,
                "data-extra-form-data",
                        FrameFilter.PATH_PARAMETER + "=" + StringUtils.encodeUri(FrameFilter.encodePath(JspUtils.getCurrentServletPath(request))) + "&"
                                + FrameFilter.NAME_PARAMETER + "=" + StringUtils.encodeUri(name));
    }

    @Override
    public int doStartTag() throws JspException {
        HttpServletRequest request = (HttpServletRequest) pageContext.getRequest();
        oldName = Static.getCurrentName(request);

        request.setAttribute(CURRENT_NAME_PREFIX, name);

        try {
            if (!Boolean.TRUE.equals(request.getAttribute(JS_INCLUDED_PREFIX))) {
                request.setAttribute(JS_INCLUDED_PREFIX, Boolean.TRUE);

                @SuppressWarnings("all")
                HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                final String[][] plugins = new String[][] {
                        //name          path
                        {"$.plugin2",  "/_resource/jquery2/jquery.extra.js"},
                        {"$.fn.popup", "/_resource/jquery2/jquery.popup.js"},
                        {"$.fn.frame", "/_resource/jquery2/jquery.frame.js"},
                };

                writer.writeStart("script", "type", "text/javascript");
                    writer.write("(function($, win, undef) {");
                        writer.write("var done = function() {");
                            writer.write("$(window.document).frame({");
                                writer.write("'setBody': function(body) {");
                                    writer.write("var insertionMode = $(this).attr('data-insertion-mode');");
                                    writer.write("if ('append' === insertionMode) {");
                                        writer.write("$(this).append(body);");
                                    writer.write("} else if ('prepend' === insertionMode) {");
                                        writer.write("$(this).prepend(body);");
                                    writer.write("} else {"); // 'replace' === insertionMode
                                        writer.write("$(this).html(body);");
                                    writer.write("}");
                                writer.write("}");
                            writer.write("}).ready(function() {");
                                writer.write("$(this).trigger('create');");
                            writer.write("});");
                        writer.write("};");
                        writer.write("var deferreds = [];");

                        for (String[] plugin : plugins) {
                            writer.write("if (!$.isFunction(" + plugin[0] + ")) {");
                                writer.write("deferreds.push($.getScript('" + JspUtils.getAbsolutePath(request, plugin[1]) + "'));");
                            writer.write("}");
                        }

                        writer.write("if (deferreds.length > 0) {");
                            writer.write("deferreds.push($.Deferred(function(deferred) {");
                                writer.write("$(deferred.resolve);");
                            writer.write("}));");
                            writer.write("$.when.apply($, deferreds).done(function() {");
                                writer.write("done();");
                            writer.write("});");
                        writer.write("} else {");
                            writer.write("done();");
                        writer.write("}");
                    writer.write("}(jQuery, window));");
                writer.writeEnd();
            }

            if (!lazy
                    || isRenderingFrame(request)
                    || Boolean.FALSE.equals(ObjectUtils.to(Boolean.class, request.getParameter(FrameFilter.LAZY_PARAMETER)))) {
                return EVAL_BODY_BUFFERED;

            } else {
                @SuppressWarnings("all")
                HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                startFrame(request, writer);
                    writer.writeStart("a", "href", JspUtils.getAbsolutePath(request, "", FrameFilter.LAZY_PARAMETER, false));
                        writer.writeHtml("");
                    writer.writeEnd();
                writer.writeEnd();

                return SKIP_BODY;
            }

        } catch (IOException error) {
            throw new JspException(error);
        }
    }

    @Override
    public int doEndTag() throws JspException {
        HttpServletRequest request = (HttpServletRequest) pageContext.getRequest();

        request.setAttribute(CURRENT_NAME_PREFIX, oldName);

        if (bodyContent != null) {
            String body = bodyContent.getString();

            if (body != null) {
                if (isRenderingFrame(request)) {
                    request.setAttribute(FrameFilter.BODY_ATTRIBUTE, body);

                } else {
                    try {
                        @SuppressWarnings("all")
                        HtmlWriter writer = new HtmlWriter(pageContext.getOut());

                        startFrame(request, writer, "loaded");
                            writer.write(body);
                        writer.writeEnd();

                    } catch (IOException error) {
                        throw new JspException(error);
                    }
                }
            }
        }

        return EVAL_PAGE;
    }

    /**
     * {@link FrameTag} utility methods.
     */
    public static final class Static {

        /**
         * Returns the name of the current frame that's rendering.
         *
         * @param request Can't be {@code null}.
         * @return May be {@code null}.
         */
        public static String getCurrentName(HttpServletRequest request) {
            return (String) request.getAttribute(CURRENT_NAME_PREFIX);
        }
    }
}
