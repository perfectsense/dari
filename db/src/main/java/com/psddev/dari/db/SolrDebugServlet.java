package com.psddev.dari.db;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugServlet;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

@SuppressWarnings("serial")
public class SolrDebugServlet extends DebugServlet {

    @Override
    public String getName() {
        return Database.Static.getFirst(SolrDatabase.class) != null
                ? "Database: Solr Query"
                : null;
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-solr");
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "Solr");

                SolrDatabase database = null;
                List<SolrDatabase> databases = Database.Static.getByClass(SolrDatabase.class);
                for (SolrDatabase db : databases) {
                    if (db.getName().equals(page.param(String.class, "db"))) {
                        database = db;
                        break;
                    }
                }
                if (database == null) {
                    database = Database.Static.getFirst(SolrDatabase.class);
                }

                String query = page.param(String.class, "query");
                String sort = page.param(String.class, "sort");

                writeStart("h2").writeHtml("Query").writeEnd();
                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "post");

                    writeStart("select", "class", "span6", "name", "db");
                        for (SolrDatabase db : Database.Static.getByClass(SolrDatabase.class)) {
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
                            "name", "query",
                            "placeholder", "Query",
                            "rows", 4,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                        writeHtml(query);
                    writeEnd();

                    writeStart("h3").writeHtml("Sort").writeEnd();
                    writeStart("textarea",
                            "class", "span6",
                            "name", "sort",
                            "placeholder", "Sort",
                            "rows", 2,
                            "style", "font-family:monospace; margin: 4px 0; width: 100%;");
                        writeHtml(sort);
                    writeEnd();

                    writeElement("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                writeEnd();

                if (!ObjectUtils.isBlank(query)) {
                    writeStart("h2").writeHtml("Result").writeEnd();
                    SolrServer server = database.openConnection();
                    SolrQuery solrQuery = new SolrQuery(query);
                    if (!StringUtils.isBlank(sort)) {
                        for (String sortField : sort.split(",")) {
                            String[] parameters = sortField.split(" ");
                            solrQuery.addSortField(parameters[0], SolrQuery.ORDER.valueOf(parameters[1]));
                        }
                    }
                    solrQuery.setParam("debugQuery", true);

                    Throwable error = null;

                    try {
                        long startTime = System.nanoTime();
                        QueryResponse response = server.query(solrQuery, SolrRequest.METHOD.POST);
                        Map<String, String> explainMap = response.getExplainMap();
                        SolrDocumentList documents = response.getResults();

                        writeStart("p");
                            writeHtml("Took ");
                            writeStart("strong").writeObject((System.nanoTime() - startTime) / 1e6).writeEnd();
                            writeHtml(" milliseconds to find ");
                            writeStart("strong").writeObject(documents.getNumFound()).writeEnd();
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
                                for (SolrDocument document : response.getResults()) {
                                    writeStart("tr");
                                        for (Map.Entry<String, Object> entry : document.entrySet()) {
                                            writeStart("td");
                                                writeObject(entry.getValue());
                                            writeEnd();
                                        }
                                        writeStart("td");
                                            Object id = document.get("id");
                                            if (explainMap.containsKey(id)) {
                                                write(explainMap.get(id).replaceAll("\n", "<br>").replaceAll(" ", "&nbsp;"));
                                            }
                                        writeEnd();
                                    writeEnd();
                                }
                            writeEnd();
                        writeEnd();

                    } catch (IOException e) {
                        error = e;
                    } catch (SolrServerException e) {
                        error = e;
                    }

                    if (error != null) {
                        writeStart("div", "class", "alert alert-error");
                            writeObject(error);
                        writeEnd();
                    }
                }

            endPage();
        } };
    }
}
