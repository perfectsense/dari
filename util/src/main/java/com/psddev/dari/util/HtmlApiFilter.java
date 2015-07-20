package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URL;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter that automatically captures the HTML output from the response
 * and formats it so that it's suitable for use as an API.
 *
 * <p>You can use it on any paths with the following query parameters:</p>
 *
 * <ul>
 * <li>{@code ?_format=js}</li>
 * <li>{@code ?_format=json}</li>
 * <li>{@code ?_format=json&amp;_result=html}</li>
 * <li>{@code ?_format=jsonp&amp;_callback=}</li>
 * <li>{@code ?_format=jsonp&amp;_callback=&amp;_result=html}</li>
 * </ul>
 *
 * @see HtmlMicrodata
 */
public class HtmlApiFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String format = request.getParameter("_format");

        if (ObjectUtils.isBlank(format)) {
            chain.doFilter(request, response);
            return;
        }

        Writer writer = response.getWriter();

        if ("js".equals(format)) {
            response.setContentType("text/javascript");

            writer.write("(function(window, undefined) {");
                writer.write("var d = window.document,");
                writer.write("ss = d.getElementsByTagName('SCRIPT'),");
                writer.write("s = ss[ss.length - 1],");
                writer.write("f = d.createElement('IFRAME'),");
                writer.write("h;");

                writer.write("f.scrolling = 'no';");
                writer.write("f.style.border = 'none';");
                writer.write("f.style.width = '100%';");
                writer.write("f.src = '");
                writer.write(StringUtils.escapeJavaScript(JspUtils.getAbsoluteUrl(request, "", "_format", "_frame")));
                writer.write("';");

                writer.write("s.parentNode.insertBefore(f, s);");
                writer.write("s.parentNode.removeChild(s);");

                writer.write("window.addEventListener('message', function(event) {");
                    writer.write("var nh = parseInt(event.data, 10);");

                    writer.write("if (h !== nh) {");
                        writer.write("f.style.height = nh + 'px';");
                        writer.write("h = nh;");
                    writer.write("}");
                writer.write("}, false);");
            writer.write("})(window);");
            return;

        } else if ("_frame".equals(format)) {
            @SuppressWarnings("resource")
            HtmlWriter html = new HtmlWriter(writer);

            response.setContentType("text/html");
            html.writeTag("!doctype html");
            html.writeStart("html");
                html.writeStart("head");
                html.writeEnd();

                html.writeStart("body", "style", html.cssString(
                        "margin", 0,
                        "padding", 0));
                    html.writeStart("iframe",
                            "scrolling", "no",
                            "src", JspUtils.getAbsoluteUrl(request, "", "_format", null),
                            "style", html.cssString(
                                    "border", "none",
                                    "width", "100%"));
                    html.writeEnd();

                    html.writeStart("script", "type", "text/javascript");
                        html.writeRaw("(function(window, undefined) {");
                            html.writeRaw("setInterval(function() {");
                                html.writeRaw("var f = document.getElementsByTagName('iframe')[0], h = f.contentDocument.body.scrollHeight;");

                                html.writeRaw("f.height = h + 'px';");
                                html.writeRaw("window.parent.postMessage('' + h, '*');");
                            html.writeRaw("}, 500);");
                        html.writeRaw("})(window);");
                    html.writeEnd();
                html.writeEnd();
            html.writeEnd();

            return;
        }

        CapturingResponse capturing = new CapturingResponse(response);
        Object output;

        try {
            chain.doFilter(request, capturing);
            output = capturing.getOutput();

        } catch (RuntimeException error) {
            output = error;
        }

        if ("json".equals(format)) {
            response.setContentType("application/json");
            writeJson(request, writer, output);

        } else if ("jsonp".equals(format)) {
            String callback = request.getParameter("_callback");

            // Legacy parameter support.
            if (ObjectUtils.isBlank(callback)) {
                callback = request.getParameter("callback");
            }

            Preconditions.checkArgument(!ObjectUtils.isBlank(callback));

            response.setContentType("application/javascript");
            writer.write(callback);
            writer.write("(");
            writeJson(request, writer, output);
            writer.write(");");

        } else if ("oembed".equals(format)) {
            response.setContentType("application/json+oembed");

            Map<String, Object> json = new CompactMap<String, Object>();
            StringWriter string = new StringWriter();
            @SuppressWarnings("resource")
            HtmlWriter html = new HtmlWriter(string);

            html.writeStart("script",
                    "type", "text/javascript",
                    "src", JspUtils.getAbsoluteUrl(request, "", "_format", "js"));
            html.writeEnd();

            json.put("version", "1.0");
            json.put("type", "rich");
            json.put("html", string.toString());
            writer.write(ObjectUtils.toJson(json));

        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid API response format!", format));
        }
    }

    private static void writeJson(HttpServletRequest request, Writer writer, Object output) throws IOException {
        Map<String, Object> json = new CompactMap<String, Object>();

        if (output instanceof Throwable) {
            Throwable error = (Throwable) output;

            json.put("status", "error");
            json.put("errorClass", error.getClass().getName());
            json.put("errorMessage", error.getMessage());

        } else {
            if (!"html".equals(request.getParameter("_result"))) {
                output = HtmlMicrodata.Static.parseString(
                        new URL(JspUtils.getAbsoluteUrl(request, "")),
                        (String) output);
            }

            json.put("status", "ok");
            json.put("result", output);
        }

        writer.write(ObjectUtils.toJson(json));
    }
}
