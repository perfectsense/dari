package com.psddev.dari.sql;

import java.io.IOException;
import java.sql.SQLException;

import com.psddev.dari.db.DatabaseException;
import com.psddev.dari.db.Query;
import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.StringUtils;

/**
 * Thrown when there's an error while executing any of the
 * {@link AbstractSqlDatabase} methods.
 */
@SuppressWarnings("serial")
public class SqlDatabaseException extends DatabaseException {

    private final String sqlQuery;
    private final Query<?> query;

    public SqlDatabaseException(
            AbstractSqlDatabase database,
            SQLException cause,
            String sqlQuery,
            Query<?> query) {

        super(database, cause);
        this.sqlQuery = sqlQuery;
        this.query = query;
    }

    public SqlDatabaseException(AbstractSqlDatabase database, String message, SQLException cause) {
        super(database, message, cause);
        this.sqlQuery = null;
        this.query = null;
    }

    public SqlDatabaseException(AbstractSqlDatabase database, String message) {
        super(database, message);
        this.sqlQuery = null;
        this.query = null;
    }

    public String getSqlQuery() {
        return sqlQuery;
    }

    public Query<?> getQuery() {
        return query;
    }

    @Override
    public String getMessage() {
        StringBuilder messageBuilder = new StringBuilder();

        String message = super.getMessage();
        messageBuilder.append(message != null ? message : "Can't execute SQL!");

        String sqlQuery = getSqlQuery();
        if (sqlQuery != null) {
            messageBuilder.append(" (");
            messageBuilder.append(sqlQuery);
            messageBuilder.append(')');
        }

        return messageBuilder.toString();
    }

    public static class ReadTimeout
            extends SqlDatabaseException
            implements DatabaseException.ReadTimeout, HtmlObject {

        public ReadTimeout(
                AbstractSqlDatabase database,
                SQLException cause,
                String sqlQuery,
                Query<?> query) {

            super(database, cause, sqlQuery, query);
        }

        // --- HtmlObject support ---

        @Override
        public void format(HtmlWriter writer) throws IOException {
            writer.writeStart("p").writeHtml("Query took too long to execute!").writeEnd();

            writer.writeStart("p");
                writer.writeHtml("Query: ");
                writer.writeObject(getQuery());
            writer.writeEnd();

            String sql = getSqlQuery();
            writer.writeStart("p");
                writer.writeHtml("SQL: ");
                writer.writeHtml(sql);

                writer.writeHtml(" (");
                    writer.writeStart("a",
                            "target", "_blank",
                            "href", StringUtils.addQueryParameters(
                                    "/_debug/db-sql",
                                    "sql", "EXPLAIN " + sql));
                        writer.writeHtml("View execution plan");
                    writer.writeEnd();
                writer.writeHtml(")");
            writer.writeEnd();

            writer.writeStart("ul", "class", "dari-stack-trace");
                for (StackTraceElement element : getStackTrace()) {
                    writer.writeStart("li").writeObject(element).writeEnd();
                }
            writer.writeEnd();
        }
    }
}
