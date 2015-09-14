package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * Simple JSON processor backed by the
 * <a href="http://jackson.codehaus.org/">Jackon JSON Processor</a>.
 */
public class JsonProcessor {

    private static final JsonFactory DEFAULT_JSON_FACTORY;
    private static final Converter DEFAULT_CONVERTER;

    static {
        DEFAULT_JSON_FACTORY = new JsonFactory();
        DEFAULT_JSON_FACTORY.enable(JsonParser.Feature.ALLOW_COMMENTS);
        DEFAULT_JSON_FACTORY.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
        DEFAULT_JSON_FACTORY.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        DEFAULT_CONVERTER = new Converter();
        DEFAULT_CONVERTER.putAllStandardFunctions();
    }

    private JsonFactory jsonFactory;
    private Converter converter;

    @Deprecated
    private Transformer transformer;

    private boolean isIndentOutput;

    /** Returns the internal Jackson JSON factory. */
    public JsonFactory getJsonFactory() {
        return jsonFactory != null ? jsonFactory : DEFAULT_JSON_FACTORY;
    }

    /** Set the internal Jackson JSON factory. */
    public void setJsonFactory(JsonFactory factory) {
        this.jsonFactory = factory;
    }

    /**
     * Returns the converter used to convert an object into an iterable
     * or a map as necesasry.
     */
    public Converter getConverter() {
        return converter != null ? converter : DEFAULT_CONVERTER;
    }

    /**
     * Sets the converter used to convert an object into an iterable
     * or a map as necessary.
     */
    public void setConverter(Converter converter) {
        this.converter = converter;
    }

    /**
     * Returns the transformer used to transform an object before it's
     * converted to a string.
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public Transformer getTransformer() {
        return transformer;
    }

    /**
     * Sets the transformer used to transform an object before it's
     * converted to a string.
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public void setTransformer(Transformer transformer) {
        this.transformer = transformer;
    }

    /** Returns {@code true} if the generated output should be indented. */
    public boolean isIndentOutput() {
        return isIndentOutput;
    }

    /** Sets whether the generated output should be indented. */
    public void setIndentOutput(boolean isIndentOutput) {
        this.isIndentOutput = isIndentOutput;
    }

    /** Parses the JSON string from the given {@code stream} into an object. */
    public Object parse(InputStream stream) throws IOException {
        try {
            return parseAny(stream);
        } catch (JsonParseException ex) {
            throw new JsonParsingException("Can't parse JSON string!", ex);
        }
    }

    /** Parses the JSON string from the given {@code reader} into an object. */
    public Object parse(Reader reader) throws IOException {
        try {
            return parseAny(reader);
        } catch (JsonParseException ex) {
            throw new JsonParsingException("Can't parse JSON string!", ex);
        }
    }

    /** Parses the given JSON {@code string} into an object. */
    public Object parse(String string) {
        try {
            return parseAny(string);
        } catch (JsonParseException ex) {
            throw new JsonParsingException(String.format(
                    "Can't parse JSON string! [%s]", string), ex);
        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Parses the given JSON {@code bytes} into an object.
     *
     * @param bytes If {@code null}, returns {@code null}.
     */
    public Object parse(byte[] bytes) {
        try {
            return parseAny(bytes);
        } catch (JsonParseException error) {
            throw new JsonParsingException("Can't parse JSON bytes!", error);
        } catch (IOException error) {
            throw new IllegalStateException(error);
        }
    }

    // Parses the given source.
    private Object parseAny(Object source) throws IOException {
        if (source != null) {
            JsonParser parser = null;

            try {
                JsonFactory factory = getJsonFactory();

                if (source instanceof InputStream) {
                    parser = factory.createJsonParser((InputStream) source);
                } else if (source instanceof Reader) {
                    parser = factory.createJsonParser((Reader) source);
                } else if (source instanceof String) {
                    parser = factory.createJsonParser(new StringReader((String) source));
                } else if (source instanceof byte[]) {
                    parser = factory.createJsonParser((byte[]) source);
                } else {
                    throw new IllegalStateException();
                }

                if (parser.nextToken() != null) {
                    return readAny(parser);
                }

            } finally {
                if (parser != null) {
                    parser.close();
                }
            }
        }

        return null;
    }

    /**
     * Reads the current JSON token from the given {@code parser} and
     * converts it to an object.
     */
    private Object readAny(JsonParser parser) throws IOException {
        JsonToken token = parser.getCurrentToken();

        if (token == null) {
            return null;
        }

        switch (token) {
            case VALUE_STRING:
                return parser.getText();

            case VALUE_NUMBER_INT:
                return parser.getLongValue();

            case VALUE_NUMBER_FLOAT:
                return parser.getDoubleValue();

            case START_OBJECT:
                Map<String, Object> map = new CompactMap<String, Object>();
                while (parser.nextToken() != JsonToken.END_OBJECT) {
                    String name = parser.getCurrentName();
                    parser.nextToken();
                    map.put(name, readAny(parser));
                }
                return map;

            case START_ARRAY:
                List<Object> list = new ArrayList<Object>();
                while (parser.nextToken() != JsonToken.END_ARRAY) {
                    list.add(readAny(parser));
                }
                return list;

            case VALUE_NULL:
                return null;

            case VALUE_TRUE:
                return Boolean.TRUE;

            case VALUE_FALSE:
                return Boolean.FALSE;

            default:
                throw new IllegalStateException(String.format(
                        "Can't parse [%s] JSON token!", token));
        }
    }

    /** Generates a JSON string from the given {@code object}. */
    public String generate(Object object) {
        StringWriter writer = new StringWriter();
        JsonGenerator generator = null;

        try {
            try {
                generator = getJsonFactory().createJsonGenerator(writer);
                if (isIndentOutput()) {
                    generator.useDefaultPrettyPrinter();
                }
                writeAny(generator, new IdentityHashMap<Object, Object>(), object);

            } finally {
                if (generator != null) {
                    generator.close();
                }
            }

        } catch (IOException ex) {
            throw new IllegalStateException(ex);
        }

        return writer.toString();
    }

    /**
     * Writes the given object, optionally transforming it using the
     * transformer, to the given generator.
     */
    private void writeAny(
            JsonGenerator generator,
            IdentityHashMap<Object, Object> transformedCache,
            Object object)
            throws IOException {

        boolean isDuplicate = false;
        Object transformed;

        if (transformedCache.containsKey(object)) {
            isDuplicate = true;
            transformed = transformedCache.get(object);

        } else {
            @SuppressWarnings("deprecation")
            Transformer transformer = getTransformer();
            transformed = transformer != null ? transformer.transform(object) : object;
            transformedCache.put(object, transformed);
        }

        if (transformed == null) {
            generator.writeNull();

        } else if (transformed instanceof String
                || transformed instanceof Character
                || transformed instanceof CharSequence) {
            generator.writeString(transformed.toString());

        } else if (transformed instanceof Boolean) {
            generator.writeBoolean((Boolean) transformed);

        } else if (transformed instanceof Double) {
            generator.writeNumber((Double) transformed);

        } else if (transformed instanceof Number) {
            if (transformed instanceof Long
                    || transformed instanceof Integer
                    || transformed instanceof Short
                    || transformed instanceof Byte) {
                generator.writeNumber(((Number) transformed).longValue());

            } else if (transformed instanceof Double
                    || transformed instanceof Float) {
                generator.writeNumber(((Number) transformed).doubleValue());

            } else {
                generator.writeString(transformed.toString());
            }

        } else if (isDuplicate) {
            generator.writeNull();

        } else if (transformed instanceof Map) {
            writeMap(generator, transformedCache, (Map<?, ?>) transformed);

        } else {
            Converter converter = getConverter();
            Iterator<?> transformedIterator = converter.convert(Iterable.class, transformed).iterator();
            if (transformedIterator.hasNext()) {

                Object item = transformedIterator.next();
                if (!transformedIterator.hasNext() && transformed == item) {
                    if (!(transformed instanceof Map)) {
                        Map<?, ?> transformedMap = converter.convert(Map.class, transformed);
                        transformedCache.put(transformed, transformedMap);
                        transformed = transformedMap;
                    }
                    writeMap(generator, transformedCache, (Map<?, ?>) transformed);

                } else {
                    generator.writeStartArray();
                    while (true) {
                        writeAny(generator, new IdentityHashMap<Object, Object>(transformedCache), item);
                        if (transformedIterator.hasNext()) {
                            item = transformedIterator.next();
                        } else {
                            break;
                        }
                    }
                    generator.writeEndArray();
                }

            } else {
                generator.writeStartArray();
                generator.writeEndArray();
            }
        }
    }

    /** Writes the given {@code map} to the given {@code generator}. */
    private void writeMap(
            JsonGenerator generator,
            IdentityHashMap<Object, Object> transformedCache,
            Map<?, ?> map)
            throws IOException {

        generator.writeStartObject();

        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Object key = entry.getKey();

            if (key != null) {
                generator.writeFieldName(key.toString());
                writeAny(generator, transformedCache, entry.getValue());
            }
        }

        generator.writeEndObject();
    }
}
