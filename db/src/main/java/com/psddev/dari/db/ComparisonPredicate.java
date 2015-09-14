package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;

import org.joda.time.DateTime;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;

/**
 * Predicate for comparing object field values that are represented by
 * a key against other values. This class is immutable as long as the
 * initial values passed to the constructor are immutable as well.
 */
public final class ComparisonPredicate extends Predicate {

    private final boolean isIgnoreCase;
    private final String key;
    private final List<Object> values;

    /**
     * Creates an instance with the given {@code operator}, {@code key},
     * and {@code values}.
     *
     * @throws IllegalArgumentException If the given {@code operator} or
     *         {@code key} is blank.
     */
    public ComparisonPredicate(String operator, boolean isIgnoreCase, String key, Iterable<?> values) {
        super(operator);

        if (ObjectUtils.isBlank(key)) {
            throw new IllegalArgumentException("Key can't be blank!");
        }

        this.isIgnoreCase = isIgnoreCase;
        this.key = key;

        if (values == null) {
            values = Collections.singletonList(null);
        }

        boolean isNullAliasForMissing = Settings.get(boolean.class, "dari/isNullAliasForMissing");
        List<Object> mutableValues = new ArrayList<Object>();
        this.values = Collections.unmodifiableList(mutableValues);
        for (Object value : values) {

            if (value instanceof Recordable
                    && !(value instanceof Query)) {
                mutableValues.add(((Recordable) value).getState().getId());

            } else if (value instanceof State
                    && !((State) value).getType().getGroups().contains(Query.class.getName())) {
                mutableValues.add(((State) value).getId());

            } else if (value instanceof Date) {
                mutableValues.add(((Date) value).getTime());

            } else if (value instanceof DateTime) {
                mutableValues.add(((DateTime) value).getMillis());

            } else if (value instanceof Enum) {
                mutableValues.add(((Enum<?>) value).name());

            } else if (value instanceof Locale) {
                mutableValues.add(((Locale) value).toLanguageTag());

            } else if (value instanceof Class) {
                mutableValues.add(ObjectType.getInstance((Class<?>) value).getId());

            } else if (value == null && isNullAliasForMissing) {
                mutableValues.add(Query.MISSING_VALUE);

            } else {
                mutableValues.add(value);
            }
        }
    }

    @SuppressWarnings("all")
    protected ComparisonPredicate() {
        isIgnoreCase = false;
        key = null;
        values = null;
    }

    /** Returns {@code true} if the comparison should ignore case. */
    public boolean isIgnoreCase() {
        return isIgnoreCase;
    }

    /** Returns the key that represents the object field values. */
    public String getKey() {
        return key;
    }

    /** Returns the other values to be compared. */
    public List<Object> getValues() {
        return values;
    }

    /**
     * Resolves the other values to be compared using the given
     * {@code database}.
     */
    public List<Object> resolveValues(Database database) {
        DatabaseEnvironment environment = database.getEnvironment();
        List<Object> resolved = new ArrayList<Object>();
        for (Object value : values) {

            if (value instanceof Query) {
                for (Object item : database
                        .readPartial((Query<?>) value, 0, Settings.getOrDefault(int.class, "dari/subQueryResolveLimit", 100))
                        .getItems()) {
                    resolved.add(State.getInstance(item).getId());
                }

            } else if (value instanceof Class) {
                ObjectType type = environment.getTypeByClass((Class<?>) value);
                resolved.add(type != null ? type.getId() : null);

            } else {
                resolved.add(value);
            }
        }

        return resolved;
    }

    /**
     * Finds a query that'd return a result equivalent to the other
     * values to be compared. If no such query exists, this method returns
     * a {@code null}.
     */
    public Query<?> findValueQuery() {
        if (values.size() == 1) {
            Object value = values.get(0);
            if (value instanceof Query) {
                return (Query<?>) value;
            }
        }
        return null;
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof ComparisonPredicate) {
            ComparisonPredicate otherPredicate = (ComparisonPredicate) other;
            return getOperator().equals(otherPredicate.getOperator())
                    && key.equals(otherPredicate.key)
                    && isIgnoreCase == otherPredicate.isIgnoreCase
                    && values.equals(otherPredicate.values);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(getOperator(), isIgnoreCase, key, values);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(key);
        sb.append(' ');
        sb.append(getOperator());

        if (isIgnoreCase) {
            sb.append("[c]");
        }

        sb.append(' ');

        if (values.isEmpty()) {
            sb.append("[ ]");

        } else if (values.size() == 1) {
            quoteValue(sb, values.get(0));

        } else {
            sb.append("[ ");
            for (Object value : values) {
                quoteValue(sb, value);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(" ]");
        }

        return sb.toString();
    }

    /** Quotes the given {@code value} for use in a predicate string. */
    private void quoteValue(StringBuilder sb, Object value) {
        if (value instanceof String) {
            sb.append('\'');
            sb.append(StringUtils.replaceAll((String) value, "'", "\\\\'"));
            sb.append('\'');
        } else {
            sb.append(value);
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link PredicateParser#EQUALS_ANY_OPERATOR} instead. */
    @Deprecated
    public static final String EQUALS_ANY_OPERATOR = PredicateParser.EQUALS_ANY_OPERATOR;

    /** @deprecated Use {@link PredicateParser#NOT_EQUALS_ALL_OPERATOR} instead. */
    @Deprecated
    public static final String NOT_EQUALS_ALL_OPERATOR = PredicateParser.NOT_EQUALS_ALL_OPERATOR;

    /** @deprecated Use {@link PredicateParser#LESS_THAN_OPERATOR} instead. */
    @Deprecated
    public static final String LESS_THAN_OPERATOR = PredicateParser.LESS_THAN_OPERATOR;

    /** @deprecated Use {@link PredicateParser#LESS_THAN_OR_EQUALS_OPERATOR} instead. */
    @Deprecated
    public static final String LESS_THAN_OR_EQUALS_OPERATOR = PredicateParser.LESS_THAN_OR_EQUALS_OPERATOR;

    /** @deprecated Use {@link PredicateParser#GREATER_THAN_OPERATOR} instead. */
    @Deprecated
    public static final String GREATER_THAN_OPERATOR = PredicateParser.GREATER_THAN_OPERATOR;

    /** @deprecated Use {@link PredicateParser#GREATER_THAN_OR_EQUALS_OPERATOR} instead. */
    @Deprecated
    public static final String GREATER_THAN_OR_EQUALS_OPERATOR = PredicateParser.GREATER_THAN_OR_EQUALS_OPERATOR;

    /** @deprecated Use {@link PredicateParser#STARTS_WITH_OPERATOR} instead. */
    @Deprecated
    public static final String STARTS_WITH_OPERATOR = PredicateParser.STARTS_WITH_OPERATOR;

    /** @deprecated Use {@link PredicateParser#MATCHES_ANY_OPERATOR} instead. */
    @Deprecated
    public static final String MATCHES_ANY_OPERATOR = PredicateParser.MATCHES_ANY_OPERATOR;

    /** @deprecated Use {@link PredicateParser#MATCHES_ALL_OPERATOR} instead. */
    @Deprecated
    public static final String MATCHES_ALL_OPERATOR = PredicateParser.MATCHES_ALL_OPERATOR;
}
