package com.psddev.dari.util;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Automatically uses {@link Profiler} to keep track of all events
 * that occur during a HTTP request.
 */
public class ProfilerFilter extends AbstractFilter {

    private static final String ATTRIBUTE_PREFIX = ProfilerFilter.class.getName() + ".";
    private static final String PROFILER_ATTRIBUTE = ATTRIBUTE_PREFIX + "profiler";

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        if (Settings.isDebug()) {
            super.doDispatch(request, response, chain);

        } else {
            chain.doFilter(request, response);
        }
    }

    private MarkingProfiler getOrCreateMarkingProfiler(HttpServletRequest request) {
        MarkingProfiler profiler = (MarkingProfiler) request.getAttribute(PROFILER_ATTRIBUTE);

        if (profiler == null) {
            profiler = new MarkingProfiler();

            request.setAttribute(PROFILER_ATTRIBUTE, profiler);
        }

        return profiler;
    }

    @Override
    protected void doInclude(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        MarkingProfiler profiler = getOrCreateMarkingProfiler(request);
        LazyWriterResponse oldResponse = profiler.getResponse();
        LazyWriterResponse newResponse = new LazyWriterResponse(request, response);
        String path = JspUtils.getCurrentServletPath(request);
        LazyWriter writer = newResponse.getLazyWriter();

        try {
            profiler.setResponse(newResponse);
            Profiler.Static.startThreadEvent("JSP Include", new JspInclude(request, path));

            writer.writeLazily("<span class=\"_profile-jspStart\" data-jsp=\"");
            writer.writeLazily(StringUtils.escapeHtml(path));
            writer.writeLazily("\" style=\"display: none;\"></span>");

            chain.doFilter(request, newResponse);

        } finally {
            Profiler.Static.stopThreadEvent();
            profiler.setResponse(oldResponse);

            writer.writeLazily("<span class=\"_profile-jspStop\" data-jsp=\"");
            writer.writeLazily(StringUtils.escapeHtml(path));
            writer.writeLazily("\" style=\"display: none;\"></span>");
            writer.writePending();
        }
    }

    @Override
    protected void doError(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    @Override
    protected void doForward(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        doRequest(request, response, chain);
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        try {
            JspBufferFilter.Static.overrideBuffer(0);

            MarkingProfiler profiler = getOrCreateMarkingProfiler(request);
            LazyWriterResponse oldResponse = profiler.getResponse();
            LazyWriterResponse newResponse = new LazyWriterResponse(request, response);

            try {
                profiler.setResponse(newResponse);
                Profiler.Static.setThreadProfiler(profiler);
                profiler.startEvent("Request", request.getServletPath());

                chain.doFilter(request, newResponse);

            } finally {
                profiler.stopEvent();
                Profiler.Static.setThreadProfiler(null);
                profiler.setResponse(oldResponse);
                newResponse.getLazyWriter().writePending();
                writeResult(request, Static.getResultWriter(request, newResponse), profiler);
            }

        } finally {
            JspBufferFilter.Static.restoreBuffer();
        }
    }

    private void writeResult(HttpServletRequest request, HtmlWriter writer, MarkingProfiler profiler) throws IOException {
        Map<String, String> nameColors = new HashMap<String, String>();
        Map<String, String> nameClasses = new HashMap<String, String>();
        double goldenRatio = 0.618033988749895;
        double hue = Math.random();
        int index = 1;

        for (String name : profiler.getEventStats().keySet()) {
            hue += goldenRatio;
            hue %= 1.0;
            nameColors.put(name, "hsl(" + (hue * 360) + ",50%,50%)");
            nameClasses.put(name, "event" + index);
            ++ index;
        }

        writer.writeStart("div", "id", "_profile-result");

            writer.writeStart("div", "class", "navbar navbar-fixed-top");
                writer.writeStart("div", "class", "navbar-inner");
                    writer.writeStart("div", "class", "container-fluid");
                        writer.writeStart("span", "class", "brand").writeHtml("Dari \u2192 Profile Result").writeEnd();
                    writer.writeEnd();
                writer.writeEnd();
            writer.writeEnd();

            writer.writeStart("div", "class", "container-fluid", "style", "padding-top: 54px;");

                writer.writeStart("div", "id", "_profile-overview");
                    writer.writeStart("div", "class", "row");

                        // Overview of event stats.
                        writer.writeStart("div", "class", "span6");
                            writer.writeStart("h2").writeHtml("Overview").writeEnd();
                            writer.writeStart("table", "class", "table table-condensed");

                                writer.writeStart("thead");
                                    writer.writeStart("tr");
                                        writer.writeStart("th", "colspan", 2).writeHtml("Event").writeEnd();
                                        writer.writeStart("th").writeHtml("Count").writeEnd();
                                        writer.writeStart("th").writeHtml("Own Total").writeEnd();
                                        writer.writeStart("th").writeHtml("Own Average").writeEnd();
                                    writer.writeEnd();
                                writer.writeEnd();

                                writer.writeStart("tbody");
                                    for (Map.Entry<String, Profiler.EventStats> entry : profiler.getEventStats().entrySet()) {
                                        String name = entry.getKey();
                                        Profiler.EventStats stats = entry.getValue();
                                        int count = stats.getCount();
                                        double ownDuration = stats.getOwnDuration() / 1e6;

                                        writer.writeStart("tr");
                                            writer.writeStart("td");
                                                writer.writeElement("input",
                                                        "type", "checkbox",
                                                        "checked", "checked",
                                                        "value", nameClasses.get(name));
                                            writer.writeEnd();
                                            writer.writeStart("td");
                                                writer.writeStart("span",
                                                        "class", "label",
                                                        "style", "background: " + nameColors.get(name) + ";");
                                                    writer.writeHtml(name);
                                                writer.writeEnd();
                                            writer.writeEnd();
                                            writer.writeStart("td").writeObject(count).writeEnd();
                                            writer.writeStart("td").writeObject(ownDuration).writeEnd();
                                            writer.writeStart("td").writeObject(ownDuration / count).writeEnd();
                                        writer.writeEnd();
                                    }
                                writer.writeEnd();

                            writer.writeEnd();
                        writer.writeEnd();
                    writer.writeEnd();
                writer.writeEnd();

                // All events.
                writer.writeStart("div", "id", "_profile-eventTimeline");
                    writer.writeStart("h2").writeHtml("Event Timeline").writeEnd();
                    writer.writeStart("table", "class", "table table-condensed");

                        writer.writeStart("thead");
                            writer.writeStart("tr");
                                writer.writeStart("th", "style", "width: 30px;").writeEnd();
                                writer.writeStart("th").writeHtml("#").writeEnd();
                                writer.writeStart("th").writeHtml("Start").writeEnd();
                                writer.writeStart("th").writeHtml("Total").writeEnd();
                                writer.writeStart("th").writeHtml("Own").writeEnd();
                                writer.writeStart("th", "class", "objects").writeHtml("Objects").writeEnd();
                            writer.writeEnd();
                        writer.writeEnd();

                        writer.writeStart("tbody");
                            for (Profiler.Event rootEvent : profiler.getRootEvents()) {
                                writeEvent(writer, profiler, nameColors, nameClasses, 0, rootEvent);
                            }
                        writer.writeEnd();

                    writer.writeEnd();
                writer.writeEnd();

            writer.writeEnd();

        writer.writeEnd();

        // Script for formatting and adding interactivity
        // to the profile result.
        writer.writeStart("script", "type", "text/javascript");
            writer.write("(function() {");
                writer.write("var profileScript = document.createElement('script');");
                writer.write("profileScript.setAttribute('data-java-context-path', '");
                writer.write(request.getContextPath());
                writer.write("');");
                writer.write("profileScript.src = '");
                writer.write(JspUtils.getAbsolutePath(request, "/_resource/dari/profiler.js"));
                writer.write("';");
                writer.write("document.body.appendChild(profileScript);");
            writer.write("})();");
        writer.writeEnd();
    }

    @SuppressWarnings("unchecked")
    private void writeEvent(
            HtmlWriter writer,
            MarkingProfiler profiler,
            Map<String, String> nameColors,
            Map<String, String> nameClasses,
            int depth,
            Profiler.Event event)
            throws IOException {

        String name = event.getName();

        writer.writeStart("tr", "class", nameClasses.get(name), "data-depth", depth);

            writer.writeStart("td");
                writer.writeStart("i", "class", "tree icon icon-chevron-down");
                writer.writeEnd();
            writer.writeEnd();

            writer.writeStart("td").writeHtml(profiler.getEventIndexes().get(event)).writeEnd();
            writer.writeStart("td").writeObject((event.getStart() - profiler.getStart()) / 1e6).writeEnd();
            writer.writeStart("td").writeObject(event.getTotalDuration() / 1e6).writeEnd();
            writer.writeStart("td").writeObject(event.getOwnDuration() / 1e6).writeEnd();

            writer.writeStart("td", "class", "objects", "style", "padding-left: " + (depth * 30 + 5) + "px;");

                writer.writeStart("span",
                        "class", "label",
                        "style", "background: " + nameColors.get(name) + ";");
                    writer.writeHtml(name);
                writer.writeEnd();

                for (Object item : event.getObjects()) {
                    if (item instanceof Supplier) {
                        item = ((Supplier<Object>) item).get();
                    }

                    writer.writeHtml(" \u2192 ");
                    writer.writeObject(item);
                }

            writer.writeEnd();

            for (Profiler.Event child : event.getChildren()) {
                writeEvent(writer, profiler, nameColors, nameClasses, depth + 1, child);
            }

        writer.writeEnd();
    }

    /** {@link ProfilerFilter} utility methods. */
    public static final class Static {

        private static final String ATTRIBUTE_PREFIX = ProfilerFilter.class.getName() + ".";
        private static final String RESULT_WRITER_ATTRIBUTE = ATTRIBUTE_PREFIX + ".resultWriter";

        /**
         * Returns the HTML writer used to render the result.
         *
         * @return Never {@code null}.
         */
        public static HtmlWriter getResultWriter(HttpServletRequest request, HttpServletResponse response) throws IOException {
            Preconditions.checkNotNull(request);

            HtmlWriter writer = (HtmlWriter) request.getAttribute(RESULT_WRITER_ATTRIBUTE);

            if (writer == null) {
                writer = new HtmlWriter(response.getWriter());
                writer.putAllStandardDefaults();
                writer.putDefault(Collection.class, COLLECTION_FORMATTER);
                writer.putDefault(StackTraceElement.class, STACK_TRACE_ELEMENT_FORMATTER);
                setResultWriter(request, writer);
            }

            return writer;
        }

        /**
         * Sets the HTML writer used to render the result.
         *
         * @param request Can't be {@code null}.
         * @param writer {@code null} unsets the result writer.
         */
        public static void setResultWriter(HttpServletRequest request, HtmlWriter writer) {
            Preconditions.checkNotNull(request);

            request.setAttribute(RESULT_WRITER_ATTRIBUTE, writer);
        }
    }

    // Profiler that marks the start of an event in the HTML.
    private static class MarkingProfiler extends Profiler {

        private final long start = System.nanoTime();
        private final Map<Event, Integer> eventIndexes = new HashMap<Event, Integer>();
        private LazyWriterResponse response;
        private int eventIndex;

        public long getStart() {
            return start;
        }

        public Map<Profiler.Event, Integer> getEventIndexes() {
            return eventIndexes;
        }

        public LazyWriterResponse getResponse() {
            return response;
        }

        public void setResponse(LazyWriterResponse response) {
            this.response = response;
        }

        @Override
        public Event startEvent(String name, Object... objects) {
            Event event = super.startEvent(name, objects);

            ++ eventIndex;
            eventIndexes.put(event, eventIndex);

            try {
                LazyWriter writer = getResponse().getLazyWriter();

                writer.writeLazily("<span class=\"_profile-eventStart\" data-index=\"");
                writer.writeLazily(String.valueOf(eventIndex));
                writer.writeLazily("\"></span>");

            } catch (IOException error) {
                // Writing the marker to the output isn't strictly necessary
                // for this filter to function, so ignore the error.
            }

            return event;
        }
    }

    private static class JspInclude implements HtmlObject {

        private final HttpServletRequest request;
        private final String path;

        public JspInclude(HttpServletRequest request, String path) {
            this.request = request;
            this.path = path;
        }

        @Override
        public void format(HtmlWriter writer) throws IOException {
            writer.writeStart("a",
                    "target", "_blank",
                    "href", JspUtils.getAbsolutePath(
                            request, "/_debug/code",
                            "action", "edit",
                            "type", "JSP",
                            "servletPath", path));
                writer.writeHtml(path);
            writer.writeEnd();
        }
    }

    @SuppressWarnings("all")
    private static final HtmlFormatter<Collection> COLLECTION_FORMATTER = new HtmlFormatter<Collection>() {
        @Override
        public void format(HtmlWriter writer, Collection collection) throws IOException {
            writer.writeHtml(collection.getClass().getSimpleName());
            writer.writeHtml(": [");
            for (Iterator<Object> i = collection.iterator(); i.hasNext();) {
                writer.writeObject(i.next());
                if (i.hasNext()) {
                    writer.write(", ");
                }
            }
            writer.writeHtml("]");
        }
    };

    private static final HtmlFormatter<StackTraceElement> STACK_TRACE_ELEMENT_FORMATTER = new HtmlFormatter<StackTraceElement>() {
        @Override
        public void format(HtmlWriter writer, StackTraceElement element) throws IOException {
            writer.writeHtml("Called from ");
            HtmlFormatter.STACK_TRACE_ELEMENT.format(writer, element);
        }
    };
}
