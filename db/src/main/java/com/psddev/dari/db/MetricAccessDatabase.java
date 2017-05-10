package com.psddev.dari.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public interface MetricAccessDatabase extends Database {

    SqlVendor getMetricVendor();

    String getMetricCatalog();

    int getSymbolId(String symbol);

    Connection openConnection();

    Connection openReadConnection();

    ResultSet executeQueryBeforeTimeout(
            Statement statement,
            String sqlQuery,
            int timeout)
            throws SQLException;

    void closeConnection(Connection connection);
}
