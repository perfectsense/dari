package com.psddev.dari.sql;

import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.db.Region;
import com.psddev.dari.util.CompactMap;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;

class RegionSqlIndex extends AbstractSqlIndex {

    private final Param<String> regionParam;
    private final Object valueParam;

    public RegionSqlIndex(AbstractSqlDatabase database, String namePrefix, int version) {
        super(database, namePrefix, version);

        this.regionParam = DSL.param("region", String.class);
        this.valueParam = database.stGeomFromText(regionParam);
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        if (value instanceof Region) {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(regionParam.getName(), ((Region) value).toWkt());
            return bindValues;

        } else {
            return null;
        }
    }

    @Override
    public Object valueInline(ObjectIndex index, Object value) {
        return value instanceof Region
                ? database.stGeomFromText(DSL.inline(((Region) value).toWkt(), String.class))
                : null;
    }
}
