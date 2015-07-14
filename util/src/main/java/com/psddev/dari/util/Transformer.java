package com.psddev.dari.util;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Map;

/**
 * Transforms an arbitrary object into another object.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class Transformer {

    private final Map<Class<?>, TransformationFunction<?>> functions = new HashMap<Class<?>, TransformationFunction<?>>();

    /**
     * Returns the function used to transform an instance of the given
     * {@code fromClass}.
     */
    @SuppressWarnings("unchecked")
    public <F> TransformationFunction<F> getFunction(Class<F> fromClass) {

        TransformationFunction<?> function = null;
        if (fromClass == null) {
            function = functions.get(null);

        } else {
            for (Class<?> assignable : TypeDefinition.getInstance(fromClass).getAssignableClassesAndInterfaces()) {
                function = functions.get(assignable);
                if (function != null) {
                    break;
                }
            }
        }

        return (TransformationFunction<F>) function;
    }

    /**
     * Puts the given {@code function} to be used to transform an instance
     * of the given {@code fromClass}.
     */
    public <F> void putFunction(Class<F> fromClass, TransformationFunction<F> function) {
        functions.put(fromClass, function);
    }

    /** Transforms the given {@code object} into another object. */
    public Object transform(Object object) {

        Class<?> objectClass = object != null ? object.getClass() : null;
        if (objectClass != null && objectClass.isArray()) {
            int length = Array.getLength(object);
            Object returnArray = Array.newInstance(Object.class, length);
            for (int i = 0; i < length; ++ i) {
                Array.set(returnArray, i, transform(Array.get(object, i)));
            }
            return returnArray;

        } else {
            @SuppressWarnings("unchecked")
            TransformationFunction<Object> function = (TransformationFunction<Object>) getFunction(objectClass);
            return function != null ? function.transform(object) : object;
        }
    }
}
