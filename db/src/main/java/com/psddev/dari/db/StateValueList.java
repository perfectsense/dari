package com.psddev.dari.db;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.UUID;

/**
 * Internal list implementation that automatically resolves object
 * references and converts items based on the internal field type.
 */
class StateValueList extends AbstractList<Object> {

    private final Database database;
    private final Object object;
    private final ObjectField field;
    private final String itemType;
    private final List<Object> list;
    private boolean hasConvertedAll;

    @SuppressWarnings("unchecked")
    public StateValueList(
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
            list = new ArrayList<Object>();
            hasConvertedAll = true;

        } else if (items instanceof List) {
            list = (List<Object>) items;
            hasConvertedAll = field != null && field.isRaw();

        } else {
            list = new ArrayList<Object>();
            hasConvertedAll = field != null && field.isRaw();

            for (Object item : items) {
                list.add(item);
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

        Map<UUID, Object> references = StateValueUtils.resolveReferences(database, object, list);
        for (ListIterator<Object> i = list.listIterator(); i.hasNext();) {
            Object item = i.next();
            UUID id = StateValueUtils.toIdIfReference(item);

            if (id == null) {
                i.set(convertItem(item));

            } else {
                Object object = references.get(id);
                if (object != null) {
                    i.set(object);
                } else {
                    i.remove();
                }
            }
        }

        hasConvertedAll = true;
    }

    // --- AbstractList support ---

    @Override
    public void add(int index, Object item) {
        convertAll();
        list.add(index, convertItem(item));
    }

    @Override
    public Object get(int index) {
        convertAll();
        return list.get(index);
    }

    @Override
    public Object remove(int index) {
        convertAll();
        return list.remove(index);
    }

    @Override
    public Object set(int index, Object item) {
        convertAll();
        return list.set(index, convertItem(item));
    }

    @Override
    public int size() {
        convertAll();
        return list.size();
    }
}
