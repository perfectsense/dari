package com.psddev.dari.db;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Modifies the {@linkplain ObjectType object type} definitions that are
 * compatible with {@code T}. This is most often used to apply a common
 * set of fields and behaviors to a group of classes. It can also be used
 * to do that to an existing class that you can't change.
 *
 * <p>For example, let's say you have a news site with articles, galleries,
 * and videos:
 *
 * <p><blockquote><pre>
class Article extends Record { ... }
class Gallery extends Record { ... }
class Video extends Record { ... }</pre></blockquote>
 *
 * <p>You can add ratings to all of them and any future types:
 *
 * <p><blockquote><pre>
class Rating extends Modification&lt;Object&gt; {
    private int likes;
    private int dislikes;

    // Getters &amp; Setters.
}</pre></blockquote>
 *
 * <p>And access the ratings data through {@link State#as}:
 *
 * <p><blockquote><pre>
Article article = Query.from(Article.class).first();
article.as(Rating.class).setLikes(10);
article.save();

// Some time later...
article.as(Rating.class).getLikes(); // Returns 10.</pre></blockquote>
 *
 * <p>If you don't want to apply the modification to everything, You can
 * limit the effect through either {@code T} or {@link Modification.Classes}:
 *
 * <p><blockquote><pre>
// Only modifies the {@code Article} class.
class Rating extends Modification&lt;Article&gt; { ... }

// Modifies the {@code Gallery} and {@code Video} classes.
{@literal @}Modification.classes({ Gallery.class, Video.class })
class Rating extends Modification&lt;Object&gt; { ... }</pre></blockquote>
 */
public abstract class Modification<T> extends Record {

    /** Returns the original object. */
    @SuppressWarnings("unchecked")
    public final T getOriginalObject() {
        return (T) getState().getOriginalObject();
    }

    /**
     * Specifies an array of classes that the target type should modify.
     * This takes precedence over the {@code T} type argument in
     * {@link Modification}, so in the following, {@code Bar}
     * would only apply to {@code Foo}, not {@code Object}:
     *
     * <p><blockquote><pre>
{@literal @}Modification.classes({ Foo.class })
class Bar extends Modification&lt;Object&gt; { ... }</pre></blockquote>
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface Classes {
        Class<?>[] value();
    }

    /** {@linkplain Modification Modification} utility methods. */
    public static final class Static {

        /**
         * Returns a set of classes that should be modified by the given
         * {@code modificationClass}.
         */
        public static Set<Class<?>> getModifiedClasses(Class<? extends Modification<?>> modificationClass) {
            Set<Class<?>> modified = new LinkedHashSet<Class<?>>();
            Classes classes = modificationClass.getAnnotation(Classes.class);

            if (classes != null) {
                for (Class<?> c : classes.value()) {
                    if (c != null) {
                        modified.add(c);
                    }
                }

                if (!modified.isEmpty()) {
                    return modified;
                }
            }

            Type superClass = modificationClass.getGenericSuperclass();

            if (superClass instanceof ParameterizedType) {
                Type[] typeArguments = ((ParameterizedType) superClass).getActualTypeArguments();

                if (typeArguments != null && typeArguments.length > 0) {
                    Type type = typeArguments[0];

                    while (true) {
                        if (type instanceof Class) {
                            modified.add((Class<?>) type);
                        } else if (type instanceof ParameterizedType) {
                            type = ((ParameterizedType) type).getRawType();
                            continue;
                        }
                        break;
                    }
                }
            }

            if (modified.isEmpty()) {
                modified.add(Object.class);
            }

            return modified;
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link State#getValue} instead. */
    @Deprecated
    public static Object getValue(Object object, String field) {
        return State.getInstance(object).getValue(field);
    }

    /** @deprecated Use {@link State#putValue} instead. */
    @Deprecated
    public static void setValue(Object object, String field, Object value) {
        State.getInstance(object).putValue(field, value);
    }

    /** @deprecated Use {@link Class#isAssignableFrom} instead. */
    @Deprecated
    public static boolean isModification(Class<?> objectClass) {
        return Modification.class.isAssignableFrom(objectClass);
    }

    /** @deprecated Use {@link Static#getModifiedClasses} instead. */
    @Deprecated
    @SuppressWarnings("unchecked")
    public static Set<Class<?>> getModifiableClasses(Class<?> modificationClass) {
        return Static.getModifiedClasses((Class<? extends Modification<?>>) modificationClass);
    }
}
