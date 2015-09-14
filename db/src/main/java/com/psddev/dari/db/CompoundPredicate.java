package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.psddev.dari.util.ObjectUtils;

/**
 * Predicate whose evaluation depends on its child predicates. This class
 * is immutable as long as all child predicates are immutable as well.
 */
public final class CompoundPredicate extends Predicate {

    private final List<Predicate> children;

    /**
     * Combines the given {@code left} and {@code right} predicates with
     * the given {@code operator}.
     */
    public static Predicate combine(String operator, Predicate left, Predicate right) {
        if (PredicateParser.NOT_OPERATOR.equalsIgnoreCase(operator)) {
            if (left == null) {
                return new CompoundPredicate(PredicateParser.NOT_OPERATOR, Arrays.asList(right));

            } else if (right == null) {
                return new CompoundPredicate(PredicateParser.NOT_OPERATOR, Arrays.asList(left));
            }

        } else if (left == null) {
            return right;

        } else if (right == null) {
            return left;
        }

        if (left instanceof CompoundPredicate) {
            CompoundPredicate leftCompound = (CompoundPredicate) left;
            if (leftCompound.getOperator().equals(operator)) {
                List<Predicate> leftChildren = leftCompound.getChildren();
                List<Predicate> children = new ArrayList<Predicate>(leftChildren.size() + 1);
                children.addAll(leftChildren);
                children.add(right);
                return new CompoundPredicate(operator, children);
            }
        }

        return new CompoundPredicate(operator, Arrays.asList(left, right));
    }

    /**
     * Creates an instance based on the given {@code operator} and
     * {@code children}.
     *
     * @throws IllegalArgumentException If the given {@code operator} is
     * blank.
     */
    public CompoundPredicate(String operator, Iterable<? extends Predicate> children) {
        super(operator);

        if (children == null) {
            this.children = Collections.emptyList();

        } else {
            List<Predicate> mutableChildren = new ArrayList<Predicate>();
            this.children = Collections.unmodifiableList(mutableChildren);
            for (Predicate child : children) {
                mutableChildren.add(child);
            }
        }
    }

    @SuppressWarnings("all")
    protected CompoundPredicate() {
        this.children = null;
    }

    /** Returns the child predicates to be evaluated. */
    public List<Predicate> getChildren() {
        return children;
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof CompoundPredicate) {
            CompoundPredicate otherPredicate = (CompoundPredicate) other;
            return getOperator().equals(otherPredicate.getOperator())
                    && children.equals(otherPredicate.children);

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(getOperator(), children);
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (!children.isEmpty()) {

            String joinOperator = getOperator();
            if (NOT_OPERATOR.equals(joinOperator)) {
                joinOperator = OR_OPERATOR;
                stringBuilder.append(NOT_OPERATOR);
                stringBuilder.append(' ');
            }

            stringBuilder.append('(');
            for (Predicate child : children) {
                stringBuilder.append(child);
                stringBuilder.append(' ');
                stringBuilder.append(joinOperator);
                stringBuilder.append(' ');
            }

            stringBuilder.setLength(stringBuilder.length() - joinOperator.length() - 2);
            stringBuilder.append(')');
        }

        return stringBuilder.toString();
    }

    // --- Deprecated ---

    /** @deprecated Use {@link PredicateParser#AND_OPERATOR} instead. */
    @Deprecated
    public static final String AND_OPERATOR = PredicateParser.AND_OPERATOR;

    /** @deprecated Use {@link PredicateParser#OR_OPERATOR} instead. */
    @Deprecated
    public static final String OR_OPERATOR = PredicateParser.OR_OPERATOR;

    /** @deprecated Use {@link PredicateParser#NOT_OPERATOR} instead. */
    @Deprecated
    public static final String NOT_OPERATOR = PredicateParser.NOT_OPERATOR;
}
