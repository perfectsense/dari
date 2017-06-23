package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Database;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.DebugServlet;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.json.JSONObject;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortOrder.DESC;

@SuppressWarnings("serial")
public class ElasticDebugServlet extends DebugServlet {

    @Override
    public String getName() {
        return Database.Static.getFirst(ElasticsearchDatabase.class) != null
                ? "Database: Elasticsearch Query"
                : null;
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-elastic");
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "Elasticsearch");

                ElasticsearchDatabase database = null;
                List<ElasticsearchDatabase> databases = Database.Static.getByClass(ElasticsearchDatabase.class);
                for (ElasticsearchDatabase db : databases) {
                    if (db.getName().equals(page.param(String.class, "db"))) {
                        database = db;
                        break;
                    }
                }
                if (database == null) {
                    database = Database.Static.getFirst(ElasticsearchDatabase.class);
                }

                String query = page.param(String.class, "query");
                String sort = page.param(String.class, "sort");
                String index = page.param(String.class, "index");
                String type = page.param(String.class, "type");
                String from = page.param(String.class, "from");
                String size = page.param(String.class, "size");

                writeStart("h2").writeHtml("Query").writeEnd();
                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "post");

                    writeStart("select", "class", "span6", "name", "db");
                        for (ElasticsearchDatabase db : Database.Static.getByClass(ElasticsearchDatabase.class)) {
                            String dbName = db.getName();
                            writeStart("option",
                                    "selected", db.equals(database) ? "selected" : null,
                                    "value", dbName);
                                writeHtml(dbName);
                            writeEnd();
                        }
                    writeEnd();

                    writeStart("textarea",
                            "class", "span6",
                            "name", "index",
                            "placeholder", "Index (can be blank)",
                            "rows", 2,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                    writeHtml(index);
                    writeEnd();

                    writeStart("textarea",
                            "class", "span6",
                            "name", "type",
                            "placeholder", "Type (can be blank)",
                            "rows", 2,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                    writeHtml(type);
                    writeEnd();

                    writeStart("textarea",
                            "class", "span6",
                            "name", "from",
                            "placeholder", "From (0)",
                            "rows", 1,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                    writeHtml(from);
                    writeEnd();

                    writeStart("textarea",
                            "class", "span6",
                            "name", "size",
                            "placeholder", "Size (10)",
                            "rows", 1,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                    writeHtml(size);
                    writeEnd();

                    writeStart("textarea",
                            "class", "span6",
                            "name", "query",
                            "placeholder", "Query (without 'query:')",
                            "rows", 4,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                    writeHtml(query);
                    writeEnd();

                    writeStart("h3").writeHtml("Sort").writeEnd();
                    writeStart("textarea",
                            "class", "span6",
                            "name", "sort",
                            "placeholder", "Sort (field asc|desc)",
                            "rows", 2,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                        writeHtml(sort);
                    writeEnd();

                    writeElement("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                writeEnd();

                if (!ObjectUtils.isBlank(query)) {
                    writeStart("h2").writeHtml("Result").writeEnd();
                    if (database != null) {
                        TransportClient client = database.openConnection();
                        SearchResponse response;
                        SearchRequestBuilder srb;

                        String[] indexIdStrings = ObjectUtils.isBlank(index) ? new String[]{database.getAllElasticIndexName()} : index.split(",");
                        srb = client.prepareSearch(indexIdStrings)
                                .setTimeout(TimeValue.timeValueMillis(ElasticsearchDatabase.TIMEOUT));
                        if (!ObjectUtils.isBlank(type)) {
                            String[] typeIdStrings = type.split(",");
                            srb.setTypes(typeIdStrings);
                        }
                        srb.setQuery(QueryBuilders.wrapperQuery(query));
                        srb.setTrackScores(true);

                        if (!StringUtils.isBlank(sort)) {
                            for (String sortField : sort.split(",")) {
                                String[] parameters = sortField.split(" ");
                                if (parameters[0].toLowerCase().equals("_score")) {
                                    srb.addSort(new ScoreSortBuilder());
                                } else {
                                    srb.addSort(new FieldSortBuilder(parameters[0]).order(parameters[1].toLowerCase().equals("asc") ? ASC : DESC));
                                }
                            }
                        }
                        srb.setExplain(true);
                        if (ObjectUtils.isBlank(from)) {
                            srb.setFrom(0);
                        } else {
                            srb.setFrom(Integer.parseInt(from));
                        }
                        if (ObjectUtils.isBlank(size)) {
                            srb.setSize(10);
                        } else {
                            srb.setSize(Integer.parseInt(size));
                        }

                        Throwable error = null;

                        try {
                            long startTime = System.nanoTime();
                            response = srb.execute().actionGet();
                            SearchHits hits = response.getHits();

                            writeStart("p");
                            writeHtml("Took ");
                            writeStart("strong").writeObject((System.nanoTime() - startTime) / 1e6).writeEnd();
                            writeHtml(" milliseconds to find ");
                            writeStart("strong").writeObject(hits.getTotalHits()).writeEnd();
                            writeHtml(" documents.");
                            writeEnd();

                            writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                            writeStart("tr");
                            writeStart("th").writeHtml("id").writeEnd();
                            writeStart("th").writeHtml("typeId").writeEnd();
                            writeStart("th").writeHtml("object").writeEnd();
                            writeStart("th").writeHtml("score").writeEnd();
                            writeEnd();
                            writeEnd();
                            writeStart("tbody");
                            for (SearchHit document : hits.getHits()) {
                                writeStart("tr");
                                writeStart("td");
                                writeObject(document.id());
                                writeEnd();
                                writeStart("td");
                                writeObject(document.type());
                                writeEnd();
                                writeStart("td", "width", "450");
                                writeStart("pre");
                                Map<String, Object> m = document.getSource();
                                if (m.get(ElasticsearchDatabase.DATA_FIELD) != null) {
                                    String data = (String) m.get(ElasticsearchDatabase.DATA_FIELD);
                                    @SuppressWarnings("unchecked")
                                    Map<String, Object> values = (Map<String, Object>) ObjectUtils.fromJson(data);
                                    m.remove(ElasticsearchDatabase.DATA_FIELD);
                                    m.put(ElasticsearchDatabase.DATA_FIELD, values);
                                }
                                JSONObject json = new JSONObject(m);
                                write(json.toString(4));
                                writeEnd();
                                writeEnd();
                                writeStart("td");
                                writeStart("pre");
                                write(document.getScore() + "<br>"
                                        + document.getExplanation().toString().replaceAll("\n", "<br>").replaceAll(" ", "&nbsp;"));
                                writeEnd();
                                writeEnd();
                                writeEnd();
                            }
                            writeEnd();
                            writeEnd();

                        } catch (Exception e) {
                            error = e;
                        }

                        if (error != null) {
                            writeStart("div", "class", "alert alert-error");
                            write(srb.toString() + "<br>" + error);
                            writeEnd();
                        }
                    }
                }

            endPage();
        } };
    }
}
