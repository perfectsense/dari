package com.psddev.dari.h2;

import com.psddev.dari.db.Query;
import com.psddev.dari.util.ObjectUtils;
import org.h2.api.Trigger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SearchUpdateTrigger implements Trigger {

    protected static final Table<Record> TABLE = DSL.table(DSL.name("RecordSearch"));

    protected static final Field<UUID> ID_FIELD = DSL.field(DSL.name("id"), UUID.class);

    protected static final Field<String> FIELD_NAME_FIELD = DSL.field(DSL.name("fieldName"), String.class);

    protected static final Field<String> VALUE_FIELD = DSL.field(DSL.name("value"), String.class);

    @Override
    public void init(Connection connection, String schemaName, String triggerName, String tableName, boolean before, int type) {
    }

    @Override
    public void fire(Connection connection, Object[] oldRow, Object[] newRow) throws SQLException {
        try (DSLContext context = DSL.using(connection, SQLDialect.H2)) {

            // DELETE?
            if (newRow == null) {
                context.deleteFrom(TABLE)
                        .where(ID_FIELD.eq((UUID) oldRow[0]))
                        .execute();

            // INSERT or UPDATE.
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> data = (Map<String, Object>) ObjectUtils.fromJson(new String((byte[]) newRow[2], StandardCharsets.UTF_8));
                StringBuilder any = new StringBuilder();

                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    StringBuilder search = new StringBuilder();

                    appendToSearch(search, entry.getValue());
                    any.append(search);

                    context.mergeInto(TABLE)
                            .columns(ID_FIELD, FIELD_NAME_FIELD, VALUE_FIELD)
                            .key(ID_FIELD, FIELD_NAME_FIELD)
                            .values((UUID) newRow[0], entry.getKey(), search.toString())
                            .execute();
                }

                context.mergeInto(TABLE)
                        .columns(ID_FIELD, FIELD_NAME_FIELD, VALUE_FIELD)
                        .key(ID_FIELD, FIELD_NAME_FIELD)
                        .values((UUID) newRow[0], Query.ANY_KEY, any.toString())
                        .execute();
            }
        }
    }

    private void appendToSearch(StringBuilder search, Object value) {
        if (value != null) {
            if (value instanceof List) {
                ((List<?>) value).forEach(v -> appendToSearch(search, v));

            } else if (value instanceof Map) {
                ((Map<?, ?>) value).values().forEach(v -> appendToSearch(search, v));

            } else {
                search.append(value);
                search.append(' ');
            }
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void remove() {
    }
}
