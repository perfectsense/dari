package com.psddev.dari.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;
import java.util.UUID;

class UuidSqlIndex extends AbstractSqlIndex {

    private final Param<UUID> valueParam;

    public UuidSqlIndex(AbstractSqlDatabase database, String namePrefix, int version) {
        super(database, namePrefix, version);

        this.valueParam = DSL.param("value", database.uuidType());
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        UUID valueUuid = ObjectUtils.to(UUID.class, value);

        if (valueUuid == null) {
            return null;

        } else {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(valueParam.getName(), valueUuid);
            return bindValues;
        }
    }

    @Override
    public Object valueInline(ObjectIndex index, Object value) {
        return DSL.inline(ObjectUtils.to(UUID.class, value), database.uuidType());
    }
}
