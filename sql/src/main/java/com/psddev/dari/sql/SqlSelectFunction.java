package com.psddev.dari.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

@FunctionalInterface
public interface SqlSelectFunction<R> {

    R apply(ResultSet result) throws SQLException;
}
