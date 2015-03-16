package com.psddev.dari.db;

import java.util.List;

/**
 * See: {@link FunnelCache}.
 */
public interface FunnelCachedObjectProducer<T extends Database> {
    List<FunnelCachedObject> produce(T database);
}
