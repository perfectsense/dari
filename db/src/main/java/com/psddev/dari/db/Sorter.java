package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.List;

import com.psddev.dari.util.StringUtils;

/**
 * Sorter used to prioritize items returned from a {@linkplain Query query}.
 *
 * <p>Following operators are available:
 *
 * <ul>
 * <li>{@value ASCENDING_OPERATOR}
 * <li>{@value CLOSEST_OPERATOR}
 * <li>{@value DESCENDING_OPERATOR}
 * <li>{@value FARTHEST_OPERATOR}
 * <li>{@value RELEVANT_OPERATOR}
 * </ul>
 */
@Sorter.Embedded
public class Sorter extends Record {

    public static final String ASCENDING_OPERATOR = "ascending";
    public static final String DESCENDING_OPERATOR = "descending";

    public static final String CLOSEST_OPERATOR = "closest";
    public static final String FARTHEST_OPERATOR = "farthest";

    public static final String RELEVANT_OPERATOR = "relevant";

    private final String operator;
    private final List<Object> options;

    /**
     * Creates an instance with the given {@code operator}, {@code key},
     * and {@code options}.
     */
    public Sorter(String operator, Iterable<?> options) {
        this.operator = operator;
        this.options = new ArrayList<Object>();
        if (options != null) {
            for (Object option : options) {
                this.options.add(option);
            }
        }
    }

    @SuppressWarnings("all")
    protected Sorter() {
        this.operator = null;
        this.options = null;
    }

    /** Returns the operator. */
    public String getOperator() {
        return operator;
    }

    /** Returns the list of options. */
    public List<Object> getOptions() {
        return options;
    }

    // --- Cloneable support ---

    @Override
    public Sorter clone() {
        return new Sorter(getOperator(), getOptions());
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof Sorter) {
            Sorter otherSorter = (Sorter) other;
            return getOperator().equals(otherSorter.getOperator())
                    && getOptions().equals(otherSorter.getOptions());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(getOperator());

        List<Object> options = getOptions();
        if (!options.isEmpty()) {
            sb.append(" { ");
            for (Object option : options) {
                quoteValue(sb, option);
                sb.append(", ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(" }");
        }

        return sb.toString();
    }

    /** Quotes the given {@code option} for use in a predicate string. */
    private void quoteValue(StringBuilder sb, Object option) {
        if (option instanceof String) {
            sb.append('\'');
            sb.append(StringUtils.replaceAll((String) option, "'", "\\\\'"));
            sb.append('\'');
        } else {
            sb.append(option);
        }
    }
}
