package com.psddev.dari.sql;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.Query;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.NoSuchElementException;

final class SqlIterator<T> implements Closeable, Iterator<T> {

    private AbstractSqlDatabase database;
    private final String sqlQuery;
    private final Query<T> query;

    private final Connection connection;
    private final Statement statement;
    private final ResultSet result;
    private boolean hasNext = true;

    /**
     * @param database Nonnull.
     * @param sqlQuery Nonnull.
     * @param fetchSize Number of objects to fetch at a time. {@code 0} or
     *                  less to use the default.
     * @param query Nullable.
     */
    public SqlIterator(AbstractSqlDatabase database, String sqlQuery, int fetchSize, Query<T> query) {
        Preconditions.checkNotNull(database);
        Preconditions.checkNotNull(sqlQuery);

        this.database = database;
        this.sqlQuery = sqlQuery;
        this.query = query;

        try {
            connection = database.openQueryConnection(query);
            statement = connection.createStatement();
            statement.setFetchSize(fetchSize <= 0 ? 200 : fetchSize);
            result = statement.executeQuery(sqlQuery);

            moveToNext();

        } catch (SQLException error) {
            close();
            throw database.createSelectError(sqlQuery, query, error);
        }
    }

    private void moveToNext() throws SQLException {
        if (hasNext) {
            hasNext = result.next();

            if (!hasNext) {
                close();
            }
        }
    }

    @Override
    public void close() {
        hasNext = false;

        database.closeResources(query, connection, statement, result);
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public T next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        try {
            T object = database.createSavedObjectUsingResultSet(result, query);

            moveToNext();
            return object;

        } catch (SQLException error) {
            close();
            throw database.createSelectError(sqlQuery, query, error);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
