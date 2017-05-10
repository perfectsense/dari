package com.psddev.dari.db;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.CodeUtils;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.Profiler;
import com.psddev.dari.util.ProfilerFilter;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Enables {@link ProfilingDatabase} if {@link Profiler} is active
 * on the current HTTP request.
 */
public class ProfilingDatabaseFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws Exception {

        Profiler profiler = Profiler.Static.getThreadProfiler();

        if (profiler == null) {
            super.doRequest(request, response, chain);

        } else {
            ProfilingDatabase profiling = new ProfilingDatabase();
            profiling.setDelegate(Database.Static.getDefault());

            HtmlWriter resultWriter = ProfilerFilter.Static.getResultWriter(request, response);

            resultWriter.putOverride(Recordable.class, (writer, recordable) -> {
                if (recordable instanceof Query) {
                    formatQuery(request, resultWriter, (Query<?>) recordable);
                    return;
                }

                State recordableState = recordable.getState();
                ObjectType type = recordableState.getType();

                if (type != null) {
                    writer.writeHtml(type.getLabel());
                    writer.writeHtml(": ");
                }

                writer.writeStart("a", "href", JspUtils.getAbsolutePath(
                        request, "/_debug/query",
                        "where", "id = " + recordableState.getId(),
                        "event", "Run"), "target", "query");
                writer.writeHtml(recordableState.getLabel());
                writer.writeEnd();
            });

            resultWriter.putOverride(State.class, (writer, state) -> {
                ObjectType type = state.getType();

                if (type != null) {
                    writer.writeHtml(type.getLabel());
                    writer.writeHtml(": ");
                }

                writer.writeStart("a", "href", JspUtils.getAbsolutePath(
                        request, "/_debug/query",
                        "where", "id = " + state.getId(),
                        "event", "Run"), "target", "query");
                writer.writeHtml(state.getLabel());
                writer.writeEnd();
            });

            resultWriter.putOverride(SolrPaginatedResult.class, (writer, result) -> {
                writer.writeStart("p");
                writer.writeStart("code").writeHtml(result.getClass().getName()).writeEnd();
                writer.writeHtml(' ');
                writer.writeStart("strong").writeHtml(result.getFirstItemIndex()).writeEnd();
                writer.writeHtml(" to ");
                writer.writeStart("strong").writeHtml(result.getLastItemIndex()).writeEnd();
                writer.writeHtml(" of ");
                writer.writeStart("strong").writeHtml(result.getCount()).writeEnd();
                writer.writeEnd();

                if (Settings.isDebug() && result.getSolrQuery() != null) {
                    String solrFullQuery = result.getSolrQuery().toString();
                    String solrQuery = result.getSolrQuery().getQuery();
                    String solrSort = StringUtils.join(result.getSolrQuery().getSortFields(), ",");

                    // Use a form instead of a link if the URL will be too long.
                    if (solrFullQuery.length() > 2000) {
                        writer.writeStart("span", "class", "solr-query");
                        writer.writeHtml("Solr Query: ");
                        writer.writeHtml(StringUtils.decodeUri(solrFullQuery));

                        writer.writeStart("form",
                                "class", "solrQueryDebugForm",
                                "method", "post",
                                "action", JspUtils.getAbsolutePath(request, "/_debug/db-solr"),
                                "target", "query");
                        writer.writeElement("input", "type", "hidden", "name", "query", "value", StringUtils.decodeUri(solrQuery));
                        writer.writeElement("input", "type", "hidden", "name", "sort", "value", StringUtils.decodeUri(solrSort));
                        writer.writeElement("input", "class", "btn", "type", "submit", "value", "Execute");
                        writer.writeEnd();
                        writer.writeEnd();

                    } else {
                        writer.writeHtml("Solr Query: ");
                        writer.writeHtml(StringUtils.decodeUri(solrFullQuery));
                        writer.writeHtml(" (");
                        writer.writeStart("a",
                                "href", JspUtils.getAbsolutePath(request, "/_debug/db-solr", "query", solrQuery, "sort", solrSort),
                                "target", "query");
                        writer.writeHtml("Execute");
                        writer.writeEnd();
                        writer.writeHtml(")");
                    }
                }

                writer.writeStart("ol");
                for (Object item : result.getItems()) {
                    writer.writeStart("li").writeObject(item).writeHtml(" Solr Score: " + SolrDatabase.Static.getScore(item)).writeEnd();
                }
                writer.writeEnd();
            });

            resultWriter.putOverride(StackTraceElement.class, (writer, element) -> {
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
                    writer.writeStart("a",
                            "class", cssClass,
                            "target", "_blank",
                            "href", JspUtils.getAbsolutePath(
                                    request, "/_debug/code",
                                    "action", "edit",
                                    "type", "JSP",
                                    "servletPath", jspServletPath,
                                    "line", lineNumber));
                    writer.writeHtml(jspServletPath);
                    writer.writeHtml(':');
                    writer.writeHtml(lineNumber);
                    writer.writeEnd();

                } else {
                    File source = CodeUtils.getSource(className);
                    if (source == null) {
                        writer.writeStart("span", "class", cssClass);
                        writer.writeHtml(element);
                        writer.writeEnd();

                    } else {
                        writer.writeStart("a",
                                "class", cssClass,
                                "target", "_blank",
                                "href", JspUtils.getAbsolutePath(
                                        request, "/_debug/code",
                                        "action", "edit",
                                        "file", source,
                                        "line", lineNumber));
                        int dotAt = className.lastIndexOf('.');
                        if (dotAt > -1) {
                            className = className.substring(dotAt + 1);
                        }
                        writer.writeHtml(className);
                        writer.writeHtml('.');
                        writer.writeHtml(element.getMethodName());
                        writer.writeHtml(':');
                        writer.writeHtml(lineNumber);
                        writer.writeEnd();
                    }
                }
            });

            try {
                Database.Static.overrideDefault(profiling);
                super.doRequest(request, response, chain);

            } finally {
                Database.Static.restoreDefault();
            }
        }
    }

    private void formatQuery(HttpServletRequest request, HtmlWriter writer, Query<?> query) throws IOException {
        String objectClass = query.getObjectClass() != null ? query.getObjectClass().getName() : null;

        StringBuilder codeBuilder = new StringBuilder();
        codeBuilder.append("Query");

        if (query.isFromAll()) {
            codeBuilder.append(".fromAll()");

        } else {
            if (objectClass != null) {
                codeBuilder.append(".from(");
                codeBuilder.append(objectClass);
                codeBuilder.append(".class)");

            } else {
                codeBuilder.append(".fromGroup(\"");
                codeBuilder.append(query.getGroup());
                codeBuilder.append("\")");
            }
        }

        Predicate predicate = query.getPredicate();
        if (predicate != null) {
            codeBuilder.append(".where(\"");
            codeBuilder.append(predicate);
            codeBuilder.append("\")");
        }

        for (Sorter sorter : query.getSorters()) {
            codeBuilder.append(".sort(\"");
            codeBuilder.append(sorter.getOperator());
            codeBuilder.append('"');
            for (Object option : sorter.getOptions()) {
                codeBuilder.append(", ");
                if (option instanceof String) {
                    codeBuilder.append('"');
                    codeBuilder.append(((String) option).replaceAll("\"", "\\\""));
                    codeBuilder.append('"');
                } else {
                    codeBuilder.append(option);
                }
            }
            codeBuilder.append(')');
        }

        List<String> fields = query.getFields();
        if (fields != null) {
            codeBuilder.append(".fields(");
            for (String field : fields) {
                codeBuilder.append('"');
                codeBuilder.append(field);
                codeBuilder.append("\", ");
            }
            if (!fields.isEmpty()) {
                codeBuilder.setLength(codeBuilder.length() - 2);
            }
            codeBuilder.append(')');
        }

        writer.writeStart("span", "class", "dari-query");
            String code = codeBuilder.toString();
            writer.writeHtml(code);

            // Use a form instead of a link if the URL will be too long.
            if (code.length() > 2000) {
                writer.writeStart("form",
                        "method", "post",
                        "action", JspUtils.getAbsolutePath(request, "/_debug/code"),
                        "target", "query");
                    writer.writeElement("input", "type", "hidden", "name", "query", "value", code);
                    writer.writeElement("input", "type", "hidden", "name", "objectClass", "value", objectClass);
                    writer.writeElement("input", "class", "btn", "type", "submit", "value", "Execute");
                writer.writeEnd();

            } else {
                writer.writeHtml(" (");
                    writer.writeStart("a",
                            "href", JspUtils.getAbsolutePath(request, "/_debug/code", "query", code, "objectClass", objectClass),
                            "target", "query");
                        writer.writeHtml("Execute");
                    writer.writeEnd();
                writer.writeHtml(")");
            }

        writer.writeEnd();
    }
}
