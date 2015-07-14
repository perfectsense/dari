package com.psddev.dari.db;

import java.util.Map;
import java.util.Set;

@Reference.Embedded
public class Reference extends Record {

    public static final String OBJECT_KEY = "_object";

    @InternalName("record")
    private Record object;

    public Object getObject() {
        Object value = getState().get(OBJECT_KEY);

        if (value == null) {
            value = object;
        }

        return value instanceof Record
                ? value
                : Query.findById(Object.class, StateValueUtils.toIdIfReference(value));
    }

    public void setObject(Object object) {
        if (object instanceof Record) {
            this.object = (Record) object;

        } else {
            getState().put(OBJECT_KEY, object);
        }
    }

    /**
     * @deprecated No replacement.
     */
    @Deprecated
    public Set<Map.Entry<String, Object>> entrySet() {
        return getState().entrySet();
    }
}
