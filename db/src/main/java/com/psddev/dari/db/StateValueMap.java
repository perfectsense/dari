package com.psddev.dari.db;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.util.CompactMap;

/**
 * Internal map implementation that automatically resolves object
 * references and converts values based on the internal field type.
 */
class StateValueMap extends AbstractMap<String, Object> {

    private final Database database;
    private final Object object;
    private final ObjectField field;
    private final String valueType;
    private final Map<String, Object> map;
    private boolean hasConvertedAll;

    public StateValueMap(
            Database database,
            Object object,
            ObjectField field,
            String valueType,
            Map<String, Object> map) {

        this.database = database;
        this.object = object;
        this.field = field;
        this.valueType = valueType;

        if (map == null) {
            this.map = new CompactMap<String, Object>();
            hasConvertedAll = true;

        } else {
            this.map = map;
            hasConvertedAll = field != null && field.isRaw();
        }
    }

    private Object convertValue(Object value) {
        return StateValueUtils.toJavaValue(database, object, field, valueType, value);
    }

    private void convertAll() {
        if (hasConvertedAll) {
            return;
        }

        Map<UUID, Object> references = StateValueUtils.resolveReferences(database, object, map.values());
        for (Iterator<Map.Entry<String, Object>> i = map.entrySet().iterator(); i.hasNext();) {
            Map.Entry<String, Object> e = i.next();
            Object value = e.getValue();
            UUID id = StateValueUtils.toIdIfReference(value);

            if (id == null) {
                e.setValue(convertValue(value));

            } else {
                Object object = references.get(id);
                if (object != null) {
                    e.setValue(object);
                } else {
                    i.remove();
                }
            }
        }

        hasConvertedAll = true;
    }

    // --- AbstractMap support ---

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        convertAll();
        return map.entrySet();
    }

    @Override
    public Object put(String key, Object value) {
        convertAll();
        return map.put(key, convertValue(value));
    }
}
