package com.psddev.dari.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/** Debug servlet that reports application {@link Stats}. */
@SuppressWarnings("serial")
public class StatsDebugServlet extends DebugServlet {

    private enum Type {
        COUNT,
        DURATION
    }

    @Override
    public String getName() {
        return "Stats";
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("stats");
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("all")
        WebPageContext page = new WebPageContext(getServletContext(), request, response);
        Type type = page.param(Type.class, "type");

        if (type != null) {
            String statsName = page.param(String.class, "stats");

            if (statsName != null) {
                for (Stats stats : Stats.Static.getAll()) {
                    if (statsName.equals(stats.getName())) {
                        Stats.Measurement measurement = stats.getMeasurements().get(page.param(String.class, "operation"));

                        if (measurement != null) {
                            response.setContentType("text/javascript");

                            PrintWriter writer = response.getWriter();
                            String callback = page.param(String.class, "callback");

                            if (!ObjectUtils.isBlank(callback)) {
                                writer.write(callback);
                                writer.write("(");
                            }

                            long begin = page.param(long.class, "begin");
                            long end = page.paramOrDefault(long.class, "end", System.currentTimeMillis());

                            writer.write("[");

                            if (Type.COUNT.equals(type)) {
                                for (Iterator<Double> i = measurement.getCountAverages(page.param(int.class, "interval"), begin, end).iterator(); i.hasNext();) {
                                    double average = i.next();
                                    writer.write(Double.isNaN(average) ? "null" : String.valueOf(average));
                                    if (i.hasNext()) {
                                        writer.write(",");
                                    }
                                }

                            } else {
                                for (Iterator<Double> i = measurement.getDurationAverages(0, begin, end).iterator(); i.hasNext();) {
                                    double average = i.next();
                                    writer.write(Double.isNaN(average) ? "null" : String.valueOf(average * 5e3));
                                    if (i.hasNext()) {
                                        writer.write(",");
                                    }
                                }
                            }

                            writer.write("]");

                            if (!ObjectUtils.isBlank(callback)) {
                                writer.write(");");
                            }

                            return;
                        }
                    }
                }
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Stats");

                writeStart("style", "type", "text/css");
                    write(".chart { float: left; margin: 0 20px 10px 0; }");
                    write("hr { border-color: black; border-top: none; margin-left: -20px; margin-right: -20px; }");

                    write(".axis {");
                        write("font: 10px sans-serif;");
                    write("}");

                    write(".axis text {");
                        write("-webkit-transition: fill-opacity 50ms linear;");
                    write("}");

                    write(".axis path {");
                        write("display: none;");
                    write("}");

                    write(".axis line {");
                        write("stroke: #000;");
                        write("shape-rendering: crispEdges;");
                    write("}");

                    write(".horizon {");
                        write("border-color: black;");
                        write("border-style: solid;");
                        write("border-width: 1px 0;");
                        write("overflow: hidden;");
                        write("position: relative;");
                        write("width: 400px;");
                    write("}");

                    write(".horizon canvas {");
                        write("display: block;");
                    write("}");

                    write(".horizon .title,");
                    write(".horizon .value {");
                        write("bottom: 0;");
                        write("line-height: 30px;");
                        write("margin: 0 6px;");
                        write("position: absolute;");
                        write("text-shadow: 0 1px 0 rgba(255, 255, 255, .5);");
                        write("white-space: nowrap;");
                    write("}");

                    write(".horizon .title {");
                        write("left: 0;");
                        write("top: 0;");
                    write("}");

                    write(".horizon .value {");
                        write("right: 0;");
                    write("}");

                    write(".line {");
                        write("background: #000;");
                        write("opacity: .2;");
                        write("z-index: 2;");
                    write("}");
                writeEnd();

                writeStart("script", "type", "text/javascript", "src", JspUtils.getAbsolutePath(page.getRequest(), "/_resource/d3/d3.v2.min.js")).writeEnd();
                writeStart("script", "type", "text/javascript", "src", JspUtils.getAbsolutePath(page.getRequest(), "/_resource/d3/cubism.v1.min.js")).writeEnd();
                writeStart("script", "type", "text/javascript");
                    write("var maxDataSize = 400;");
                    write("var context = cubism.context().serverDelay(0).clientDelay(0).step(5e3).size(maxDataSize);");
                writeEnd();

                for (Iterator<Stats> i = Stats.Static.getAll().iterator(); i.hasNext();) {
                    Stats stats = i.next();
                    String statsName = stats.getName();
                    String operation = page.paramOrDefault(String.class, statsName + "/operation", "Total");
                    int intervalIndex = page.param(int.class, statsName + "/interval");

                    writeStart("h2").writeHtml(statsName).writeEnd();

                    for (Type type : Type.values()) {
                        String divId = JspUtils.createId(page.getRequest());

                        writeStart("div", "class", "chart");
                            writeStart("h3").writeHtml(Type.COUNT.equals(type) ? "Throughput (/s)" : "Latency (ms)").writeEnd();
                            writeStart("div", "id", divId).writeEnd();
                        writeEnd();

                        writeStart("script", "type", "text/javascript");
                            write("d3.select('#");
                            write(divId);
                            write("').call(function(div) {");

                                write("div.datum(function() {");
                                    write("var last;");
                                    write("return context.metric(function(begin, end, step, callback) {");
                                        write("begin = +begin, end= +end;");
                                        write("var first = isNaN(last);");
                                        write("if (first) last = begin;");

                                        write("$.getJSON('"); write(page.js(JspUtils.getAbsolutePath(page.getRequest(), "/_debug/stats"))); write("', {");
                                            write("'stats': '"); write(page.js(statsName)); write("',");
                                            write("'operation': '"); write(page.js(operation)); write("',");
                                            write("'interval': '"); write(page.js(intervalIndex)); write("',");
                                            write("'type': '"); write(page.js(type)); write("',");
                                            write("'begin': begin,");
                                            write("'end': end");

                                        write("}, function(data) {");
                                            write("if (first) {");
                                                write("var padding = maxDataSize - data.length;");
                                                write("if (padding > 0) {");
                                                    write("var newData = [ ];");
                                                    write("for (var i = 0; i < padding; ++ i) newData.push(0.0);");
                                                    write("data = newData.concat(data);");
                                                write("}");
                                            write("}");
                                            write("callback(null, data);");
                                        write("});");

                                    write("}, '");
                                    write(page.js(operation + " over " + stats.getAverageIntervals().get(intervalIndex).intValue() + "s"));
                                    write("');");
                                write("});");

                                write("div.append('div')");
                                    write(".attr('class', 'axis')");
                                    write(".call(context.axis().orient('top'));");

                                write("div.append('div')");
                                    write(".attr('class', 'horizon')");
                                    write(".call(context.horizon().height(60));");
                            write("});");
                        writeEnd();
                    }

                    writeStart("h3", "style", "clear: left;").writeHtml("Averages").writeEnd();
                    writeStart("table", "class", "table table-condensed");
                        writeStart("thead");
                            writeStart("tr");
                                writeStart("th").writeHtml("Operation").writeEnd();
                                writeStart("th").writeHtml("Total").writeEnd();
                                for (double averageInterval : stats.getAverageIntervals()) {
                                    writeStart("th", "colspan", 2).writeHtml("Over ").writeObject((int) averageInterval).writeHtml("s").writeEnd();
                                }
                                writeStart("th", "colspan", 2).writeHtml("Over All").writeEnd();
                            writeEnd();
                        writeEnd();
                        writeStart("tbody");
                            for (Map.Entry<String, Stats.Measurement> entry : stats.getMeasurements().entrySet()) {
                                writeStatsMeasurement(stats, entry.getKey(), entry.getValue());
                            }
                        writeEnd();
                    writeEnd();

                    if (i.hasNext()) {
                        writeElement("hr");
                    }
                }

                writeStart("script", "type", "text/javascript");
                    write("context.on('focus', function(i) {");
                        write("d3.selectAll('.value').style('right', i == null ? null : context.size() - i + 'px');");
                    write("});");
                writeEnd();
            endPage();
        }

            // Writes individual stats measurement.
            private void writeStatsMeasurement(Stats stats, String operation, Stats.Measurement measurement) throws IOException {
                writeStart("tr");
                    writeStart("th").writeHtml(operation).writeEnd();
                    writeStart("td").writeObject(measurement.getOverallTotalCount()).writeEnd();
                    for (int i = 0, size = stats.getAverageIntervals().size(); i < size; ++ i) {
                        writeCountAndDuration(stats, operation, i, measurement.getCurrentCountAverage(i), measurement.getCurrentDurationAverage(i));
                    }
                    writeCountAndDuration(stats, operation, -1, measurement.getOverallCountAverage(), measurement.getOverallDurationAverage());
                writeEnd();
            }

            private void writeCountAndDuration(Stats stats, String operation, int intervalIndex, double count, double duration) throws IOException {
                boolean link = intervalIndex >= 0;
                String statsName = stats.getName();
                String href = page.url(null, statsName + "/operation", operation, statsName + "/interval", intervalIndex);

                writeStart("td");
                    if (link) {
                        writeStart("a", "href", href);
                    }
                    writeObject(count).writeHtml("/s");
                    if (link) {
                        writeEnd();
                    }
                writeEnd();

                writeStart("td");
                    if (link) {
                        writeStart("a", "href", href);
                    }
                    if (Double.isNaN(duration)) {
                        writeStart("span", "class", "label").writeHtml("N/A").writeEnd();
                    } else {
                        writeObject(duration * 1e3).writeHtml("ms");
                    }
                    if (link) {
                        writeEnd();
                    }
                writeEnd();
            }
        };
    }
}
