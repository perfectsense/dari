package com.psddev.dari.sql;

import com.psddev.dari.db.Location;
import com.psddev.dari.db.ObjectIndex;
import com.psddev.dari.util.CompactMap;
import org.jooq.Param;
import org.jooq.impl.DSL;

import java.util.Map;

class LocationSqlIndex extends AbstractSqlIndex {

    private final Param<String> locationParam;
    private final Object valueParam;

    public LocationSqlIndex(AbstractSqlDatabase database, String namePrefix, int version) {
        super(database, namePrefix, version);

        this.locationParam = DSL.param("location", String.class);
        this.valueParam = database.stGeomFromText(locationParam);
    }

    @Override
    public Object valueParam() {
        return valueParam;
    }

    @Override
    public Map<String, Object> valueBindValues(ObjectIndex index, Object value) {
        if (value instanceof Location) {
            Map<String, Object> bindValues = new CompactMap<>();
            bindValues.put(locationParam.getName(), ((Location) value).toWkt());
            return bindValues;

        } else {
            return null;
        }
    }

    @Override
    public Object valueInline(ObjectIndex index, Object value) {
        return value instanceof Location
                ? database.stGeomFromText(DSL.inline(((Location) value).toWkt(), String.class))
                : null;
    }
}
