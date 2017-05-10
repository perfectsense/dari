package com.psddev.dari.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;

class NumberSqlIndex extends AbstractSqlIndex {

    private final Param<Double> valueParam;

    public NumberSqlIndex(AbstractSqlDatabase database, String namePrefix, int version) {
        super(database, namePrefix, version);

        this.valueParam = DSL.param("value", database.doubleType());
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        Double valueDouble = ObjectUtils.to(Double.class, value);

        if (valueDouble == null) {
            return null;

        } else {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(valueParam.getName(), valueDouble);
            return bindValues;
        }
    }

    @Override
    public Object valueInline(ObjectIndex index, Object value) {
        return DSL.inline(ObjectUtils.to(Double.class, value), database.doubleType());
    }
}
