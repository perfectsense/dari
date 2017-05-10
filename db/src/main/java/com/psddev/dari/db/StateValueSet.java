package com.psddev.dari.db;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Internal set implementation that automatically resolves object
 * references and converts items based on the internal field type.
 */
class StateValueSet extends AbstractSet<Object> {

    private final Database database;
    private final Object object;
    private final ObjectField field;
    private final String itemType;
    private Set<Object> set;
    private boolean hasConvertedAll;

    @SuppressWarnings("unchecked")
    public StateValueSet(
            Database database,
            Object object,
            ObjectField field,
            String itemType,
            Iterable<?> items) {

        this.database = database;
        this.object = object;
        this.field = field;
        this.itemType = itemType;

        if (items == null) {
            set = new LinkedHashSet<Object>();
            hasConvertedAll = true;

        } else if (items instanceof Set) {
            set = (Set<Object>) items;
            hasConvertedAll = field != null && field.isRaw();

        } else {
            set = new LinkedHashSet<Object>();
            hasConvertedAll = field != null && field.isRaw();

            for (Object item : items) {
                set.add(item);
            }
        }
    }

    private Object convertItem(Object item) {
        return StateValueUtils.toJavaValue(database, object, field, itemType, item);
    }

    private void convertAll() {
        if (hasConvertedAll) {
            return;
        }

        Set<Object> newSet = new LinkedHashSet<Object>();
        Map<UUID, Object> references = StateValueUtils.resolveReferences(database, object, set);
        for (Object item : set) {
            UUID id = StateValueUtils.toIdIfReference(item);

            if (id == null) {
                newSet.add(convertItem(item));

            } else {
                Object object = references.get(id);
                if (object != null) {
                    newSet.add(object);
                }
            }
        }

        set = newSet;
        hasConvertedAll = true;
    }

    // --- AbstractSet support ---

    @Override
    public boolean add(Object item) {
        convertAll();
        return set.add(convertItem(item));
    }

    @Override
    public Iterator<Object> iterator() {
        convertAll();
        return set.iterator();
    }

    @Override
    public int size() {
        convertAll();
        return set.size();
    }
}
