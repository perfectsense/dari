package com.psddev.dari.db;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.ObjectUtils;

@DebugFilter.Path("db-sql")
@SuppressWarnings("serial")
public class SqlDebugServlet extends HttpServlet {

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "SQL");

                SqlDatabase database = null;
                List<SqlDatabase> databases = Database.Static.getByClass(SqlDatabase.class);
                for (SqlDatabase db : databases) {
                    if (db.getName().equals(page.param(String.class, "db"))) {
                        database = db;
                        break;
                    }
                }
                if (database == null) {
                    database = Database.Static.getFirst(SqlDatabase.class);
                }

                String sql = page.param(String.class, "sql");

                writeStart("h2").writeHtml("Query").writeEnd();
                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "post");

                    writeStart("select", "class", "span6", "name", "db");
                        for (SqlDatabase db : Database.Static.getByClass(SqlDatabase.class)) {
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
                            "name", "sql",
                            "placeholder", "SQL Statement",
                            "rows", 4,
                            "style", "font-family:monospace; min-height: 300px; margin: 4px 0; width: 100%;");
                        writeHtml(sql);
                    writeEnd();

                    writeElement("input", "class", "btn btn-primary", "type", "submit", "value", "Run");
                writeEnd();

                if (!ObjectUtils.isBlank(sql)) {
                    writeStart("h2").writeHtml("Result").writeEnd();
                    Connection connection = database.openConnection();

                    try {
                        Statement statement = connection.createStatement();

                        try {
                            long startTime = System.nanoTime();
                            statement.execute(sql);
                            writeStart("p").writeHtml("Took ").writeStart("strong").writeObject((System.nanoTime() - startTime) / 1e6).writeEnd().writeHtml(" milliseconds.");

                            for (; true; statement.getMoreResults()) {
                                int updateCount = statement.getUpdateCount();

                                if (updateCount > -1) {
                                    writeStart("p", "class", "alert alert-info");
                                        writeStart("strong").writeObject(updateCount).writeEnd().writeHtml(" items updated!");
                                    writeEnd();

                                } else {
                                    ResultSet result = statement.getResultSet();
                                    if (result == null) {
                                        break;
                                    }

                                    ResultSetMetaData meta = result.getMetaData();
                                    int count = meta.getColumnCount();

                                    writeStart("table", "class", "table table-condensed");
                                        writeStart("thead");
                                            writeStart("tr");
                                                for (int i = 1; i <= count; ++ i) {
                                                    writeStart("th");
                                                        writeHtml(meta.getColumnLabel(i));
                                                    writeEnd();
                                                }
                                            writeEnd();
                                        writeEnd();
                                        writeStart("tbody");
                                            while (result.next()) {
                                                writeStart("tr");
                                                    for (int i = 1; i <= count; ++ i) {
                                                        Object value = result.getObject(i);
                                                        if (value instanceof byte[]) {
                                                            value = new String(result.getBytes(i), StandardCharsets.UTF_8);
                                                        }
                                                        writeStart("td");
                                                            writeObject(value);
                                                        writeEnd();
                                                    }
                                                writeEnd();
                                            }
                                        writeEnd();
                                    writeEnd();
                                }
                            }

                        } finally {
                            statement.close();
                        }

                    } catch (SQLException error) {
                        writeStart("div", "class", "alert alert-error");
                            writeObject(error);
                        writeEnd();

                    } finally {
                        database.closeConnection(connection);
                    }
                }

            endPage();
        } };
    }
}
