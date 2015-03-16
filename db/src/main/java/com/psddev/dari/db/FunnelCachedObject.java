package com.psddev.dari.db;

import java.util.Map;
import java.util.UUID;

/**
 * See: {@link FunnelCache}.
 */
public class FunnelCachedObject {

    private final UUID id;
    private final UUID typeId;
    private final Map<String, Object> values;
    private final Map<String, Object> extras;

    FunnelCachedObject(UUID id, UUID typeId, Map<String, Object> values, Map<String, Object> extras) {
        this.id = id;
        this.typeId = typeId;
        this.values = values;
        this.extras = extras;
    }

    public UUID getId() {
        return id;
    }

    public UUID getTypeId() {
        return typeId;
    }

    public Map<String, Object> getValues() {
        return values;
    }

    public Map<String, Object> getExtras() {
        return extras;
    }

    @Override
    public String toString() {
        return String.format("ID: %s, Type: %s", id, typeId);
    }
}
