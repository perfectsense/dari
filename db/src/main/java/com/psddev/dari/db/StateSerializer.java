package com.psddev.dari.db;

import com.google.common.base.Preconditions;
import com.psddev.dari.util.ObjectUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface StateSerializer {

    /**
     * Key for state's unique ID.
     */
    String ID_KEY = "_id";

    /**
     * Key for state's type ID.
     */
    String TYPE_KEY = "_type";

    /**
     * Key for a reference to an object.
     */
    String REFERENCE_KEY = "_ref";

    /**
     * Serializes the given {@code values} map from a {@link State} instance
     * into a byte array that's suitable for storing in a database.
     *
     * @param values Nonnull.
     * @return Nonnull.
     */
    static byte[] serialize(Map<String, Object> values) {
        Preconditions.checkNotNull(values);
        return ObjectUtils.toJson(values).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Deserializes the given {@code data} byte array from a database into
     * a map that's suitable for use by a {@link State} instance.
     *
     * @param data Nonnull.
     * @return Nonnull.
     */
    static Map<String, Object> deserialize(byte[] data) {
        Preconditions.checkNotNull(data);
        @SuppressWarnings("unchecked")
        Map<String, Object> dataMap = (Map<String, Object>) ObjectUtils.fromJson(data);
        return dataMap;
    }

    static Object toJavaValue(Database database, Object object, ObjectField field, String type, Object value) {
        return StateValueUtils.toJavaValue(database, object, field, type, value);
    }
}
