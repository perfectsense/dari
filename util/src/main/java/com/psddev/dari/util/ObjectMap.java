package com.psddev.dari.util;

import java.lang.reflect.Field;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@code Map<String,Object>} backed by an object, optionally with a {@code Converter}
 * to convert the keys.
 *
 * Allows the getting/settings of fields of the object via the {@code Map} interface.
 *
 * If a {@code Converter} is defined, keys are passed through it to get the actual key used
 * when interacting with the object.
 *
 * Additionally, fields that do not belong to the backing object are stored in a
 * secondary store by the ObjectMap, pretending they are part of the object.
 *
 * @author rhseeger
 *
 */
public class ObjectMap extends AbstractMap<String, Object> {

    private static final Converter DEFAULT_CONVERTER; static {
        DEFAULT_CONVERTER = new Converter();
        DEFAULT_CONVERTER.putAllStandardFunctions();
    }

    private Object object;
    private TypeDefinition<?> definition;
    private final Map<String, Object> extras = new CompactMap<String, Object>();
    private Converter converter;

    /** Creates a blank instance. */
    public ObjectMap() {
    }

    /** Creates an instance backed by the given {@code object}. */
    public ObjectMap(Object object) {
        setObject(object);
    }

    /** Returns the object. */
    public Object getObject() {
        return object;
    }

    /** Sets the object. */
    public void setObject(Object object) {
        this.object = object;
        definition = TypeDefinition.getInstance(object.getClass());
        extras.clear();
    }

    /** Returns the converter. */
    public Converter getConverter() {
        return converter != null ? converter : DEFAULT_CONVERTER;
    }

    /** Sets the converter. */
    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    // --- AbstractMap support ---

    @Override
    public void clear() {
        Object object = getObject();
        for (List<Field> fields : definition.getAllSerializableFields().values()) {
            for (Field field : fields) {
                try {
                    field.set(object, getConverter().convert(field.getGenericType(), null));
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
        extras.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        return (key instanceof String && definition.getField((String) key) != null) || extras.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        for (Object e : values()) {
            if (ObjectUtils.equals(value, e)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet() {
        return new AbstractSet<Map.Entry<String, Object>>() {

            @Override
            public Iterator<Map.Entry<String, Object>> iterator() {
                return new KeyIterator<Map.Entry<String, Object>>() {

                    @Override
                    public Map.Entry<String, Object> makeValue(String key) {
                        return new Entry(key);
                    }
                };
            }

            @Override
            public int size() {
                return ObjectMap.this.size();
            }
        };
    }

    @Override
    public Object get(Object key) {
        if (key instanceof String) {
            Field field = definition.getField((String) key);
            if (field != null) {
                try {
                    return field.get(getObject());
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
        return extras.get(key);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Set<String> keySet() {
        return new AbstractSet<String>() {

            @Override
            public Iterator<String> iterator() {
                return new KeyIterator<String>() {

                    @Override
                    public String makeValue(String key) {
                        return key;
                    }
                };
            }

            @Override
            public int size() {
                return ObjectMap.this.size();
            }
        };
    }

    @Override
    public Object put(String key, Object value) {
        Field field = definition.getField(key);
        if (field != null) {
            Object object = getObject();
            try {
                Object oldValue = field.get(object);
                field.set(object, getConverter().convert(field.getGenericType(), value));
                return oldValue;
            } catch (IllegalAccessException ex) {
                throw new IllegalStateException(ex);
            }
        } else {
            return extras.put(key, value);
        }
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> map) {
        for (Map.Entry<? extends String, ? extends Object> e : map.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    @Override
    public Object remove(Object key) {
        if (key instanceof String) {
            Field field = definition.getField((String) key);
            if (field != null) {
                Object object = getObject();
                try {
                    Object oldValue = field.get(object);
                    field.set(object, getConverter().convert(field.getGenericType(), null));
                    return oldValue;
                } catch (IllegalAccessException ex) {
                    throw new IllegalStateException(ex);
                }
            }
        }
        return extras.remove(key);
    }

    @Override
    public int size() {
        return definition.getAllSerializableFields().size() + extras.size();
    }

    @Override
    public Collection<Object> values() {
        return new AbstractCollection<Object>() {

            @Override
            public Iterator<Object> iterator() {
                return new KeyIterator<Object>() {

                    @Override
                    public Object makeValue(String key) {
                        return get(key);
                    }
                };
            }

            @Override
            public int size() {
                return ObjectMap.this.size();
            }
        };
    }

    // --- Nested ---

    private class Entry implements Map.Entry<String, Object> {

        private final String key;

        public Entry(String key) {
            this.key = key;
        }

        // --- Map.Entry support ---

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public Object getValue() {
            return get(getKey());
        }

        @Override
        public Object setValue(Object value) {
            return put(getKey(), value);
        }

        // --- Object support ---

        @Override
        public boolean equals(Object object) {
            return this == object || (object instanceof Entry
                    && ObjectUtils.equals(getKey(), ((Entry) object).getKey()));
        }

        @Override
        public int hashCode() {
            return ObjectUtils.hashCode(getKey());
        }

        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }
    }

    private abstract class KeyIterator<T> implements Iterator<T> {

        private Iterator<String> keys = definition.getAllSerializableFields().keySet().iterator();
        private boolean isInObject = keys.hasNext();
        private String currentKey;

        @Override
        public boolean hasNext() {
            if (isInObject && !keys.hasNext()) {
                isInObject = false;
                keys = extras.keySet().iterator();
            }
            return keys.hasNext();
        }

        public abstract T makeValue(String key);

        @Override
        public T next() {
            currentKey = keys.next();
            return makeValue(currentKey);
        }

        @Override
        public void remove() {
            ObjectMap.this.remove(currentKey);
        }
    }
}
