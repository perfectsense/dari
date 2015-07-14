package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import com.psddev.dari.util.CollectionUtils;
import com.psddev.dari.util.ObjectUtils;

/**
 * Parser for translating a predicate string into a {@linkplain Predicate
 * predicate object} that can be used in a {@linkplain Query query}.
 *
 * <p>Typical string looks like:
 *
 * <p><blockquote><pre>
 * key1 eq 'value' or (key2 eq 0 or key3 eq 100)
 * </pre></blockquote>
 *
 * <p>Following operators are standard:
 *
 * <ul>
 * <li>{@value AND_OPERATOR}
 * <li>{@value OR_OPERATOR}
 * <li>{@value NOT_OPERATOR}
 *
 * <li>{@value EQUALS_ANY_OPERATOR}
 * <li>{@value NOT_EQUALS_ALL_OPERATOR}
 * <li>{@value LESS_THAN_OPERATOR}
 * <li>{@value LESS_THAN_OR_EQUALS_OPERATOR}
 * <li>{@value GREATER_THAN_OPERATOR}
 * <li>{@value GREATER_THAN_OR_EQUALS_OPERATOR}
 * <li>{@value STARTS_WITH_OPERATOR}
 * <li>{@value CONTAINS_OPERATOR}
 * <li>{@value MATCHES_ANY_OPERATOR}
 * <li>{@value MATCHES_ALL_OPERATOR}
 * </ul>
 */
public class PredicateParser {

    public static final String AND_OPERATOR = "and";
    public static final String OR_OPERATOR = "or";
    public static final String NOT_OPERATOR = "not";

    public static final String EQUALS_ANY_OPERATOR = "equalsany";
    public static final String NOT_EQUALS_ALL_OPERATOR = "notequalsall";
    public static final String LESS_THAN_OPERATOR = "lessthan";
    public static final String LESS_THAN_OR_EQUALS_OPERATOR = "lessthanorequals";
    public static final String GREATER_THAN_OPERATOR = "greaterthan";
    public static final String GREATER_THAN_OR_EQUALS_OPERATOR = "greaterthanorequals";
    public static final String STARTS_WITH_OPERATOR = "startswith";
    public static final String CONTAINS_OPERATOR = "contains";
    public static final String MATCHES_ANY_OPERATOR = "matchesany";
    public static final String MATCHES_ALL_OPERATOR = "matchesall";
    public static final String MATCHES_EXACT_ANY_OPERATOR = "matchesexactany";
    public static final String MATCHES_EXACT_ALL_OPERATOR = "matchesexactall";

    private final Map<String, String> compoundOperators; {
        Map<String, String> m = new ConcurrentHashMap<String, String>();

        m.put(AND_OPERATOR, AND_OPERATOR);
        m.put("&&", AND_OPERATOR);

        m.put(OR_OPERATOR, OR_OPERATOR);
        m.put("||", OR_OPERATOR);

        m.put(NOT_OPERATOR, NOT_OPERATOR);
        m.put("!", NOT_OPERATOR);

        compoundOperators = m;
    }

    private final Map<String, String> comparisonOperators; {
        Map<String, String> m = new ConcurrentHashMap<String, String>();

        m.put(EQUALS_ANY_OPERATOR, EQUALS_ANY_OPERATOR);
        m.put("equals", EQUALS_ANY_OPERATOR);
        m.put("eq", EQUALS_ANY_OPERATOR);
        m.put("in", EQUALS_ANY_OPERATOR);
        m.put("is", EQUALS_ANY_OPERATOR);
        m.put("=", EQUALS_ANY_OPERATOR);

        m.put(NOT_EQUALS_ALL_OPERATOR, NOT_EQUALS_ALL_OPERATOR);
        m.put("notequals", NOT_EQUALS_ALL_OPERATOR);
        m.put("ne", NOT_EQUALS_ALL_OPERATOR);
        m.put("notin", NOT_EQUALS_ALL_OPERATOR);
        m.put("isn't", NOT_EQUALS_ALL_OPERATOR);
        m.put("isnt", NOT_EQUALS_ALL_OPERATOR);
        m.put("!=", NOT_EQUALS_ALL_OPERATOR);
        m.put("=!", NOT_EQUALS_ALL_OPERATOR);
        m.put("<>", NOT_EQUALS_ALL_OPERATOR);
        m.put("><", NOT_EQUALS_ALL_OPERATOR);

        m.put(LESS_THAN_OPERATOR, LESS_THAN_OPERATOR);
        m.put("lt", LESS_THAN_OPERATOR);
        m.put("<", LESS_THAN_OPERATOR);

        m.put(LESS_THAN_OR_EQUALS_OPERATOR, LESS_THAN_OR_EQUALS_OPERATOR);
        m.put("le", LESS_THAN_OR_EQUALS_OPERATOR);
        m.put("<=", LESS_THAN_OR_EQUALS_OPERATOR);
        m.put("=<", LESS_THAN_OR_EQUALS_OPERATOR);

        m.put(GREATER_THAN_OPERATOR, GREATER_THAN_OPERATOR);
        m.put("gt", GREATER_THAN_OPERATOR);
        m.put(">", GREATER_THAN_OPERATOR);

        m.put(GREATER_THAN_OR_EQUALS_OPERATOR, GREATER_THAN_OR_EQUALS_OPERATOR);
        m.put("ge", GREATER_THAN_OR_EQUALS_OPERATOR);
        m.put(">=", GREATER_THAN_OR_EQUALS_OPERATOR);
        m.put("=>", GREATER_THAN_OR_EQUALS_OPERATOR);

        m.put(STARTS_WITH_OPERATOR, STARTS_WITH_OPERATOR);
        m.put("startswith", STARTS_WITH_OPERATOR);
        m.put("sw", STARTS_WITH_OPERATOR);
        m.put("^=", STARTS_WITH_OPERATOR);

        m.put(CONTAINS_OPERATOR, CONTAINS_OPERATOR);

        m.put(MATCHES_ANY_OPERATOR, MATCHES_ANY_OPERATOR);
        m.put("matchesany", MATCHES_ANY_OPERATOR);
        m.put("matches", MATCHES_ANY_OPERATOR);
        m.put("~=", MATCHES_ANY_OPERATOR);

        m.put(MATCHES_ALL_OPERATOR, MATCHES_ALL_OPERATOR);

        comparisonOperators = m;
    }

    private final Map<String, Evaluator> evaluators; {
        Map<String, Evaluator> m = new ConcurrentHashMap<String, Evaluator>();

        m.put(AND_OPERATOR, new AndEvaluator());
        m.put(OR_OPERATOR, new OrEvaluator());
        m.put(NOT_OPERATOR, new NotEvaluator());

        m.put(EQUALS_ANY_OPERATOR, new EqualsAnyEvaluator());
        m.put(NOT_EQUALS_ALL_OPERATOR, new NotEqualsAllEvaluator());
        m.put(LESS_THAN_OPERATOR, new LessThanEvaluator());
        m.put(LESS_THAN_OR_EQUALS_OPERATOR, new LessThanOrEqualsEvaluator());
        m.put(GREATER_THAN_OPERATOR, new GreaterThanEvaluator());
        m.put(GREATER_THAN_OR_EQUALS_OPERATOR, new GreaterThanOrEqualsEvaluator());
        m.put(STARTS_WITH_OPERATOR, new StartsWithEvaluator());
        m.put(CONTAINS_OPERATOR, new ContainsEvaluator());
        m.put(MATCHES_ANY_OPERATOR, new MatchesAnyEvaluator());
        m.put(MATCHES_ALL_OPERATOR, new MatchesAllEvaluator());

        evaluators = m;
    }

    /** Returns the map of compound operators. */
    public Map<String, String> getCompoundOperators() {
        return compoundOperators;
    }

    /** Returns the map of comparison operators. */
    public Map<String, String> getComparisonOperators() {
        return comparisonOperators;
    }

    /** Returns the map of evaluators. */
    public Map<String, Evaluator> getEvaluators() {
        return evaluators;
    }

    /**
     * Parses the given {@code predicateString} along with the given
     * {@code parameters}.
     *
     * @param predicateString If {@code null}, returns {@code null}.
     * @param parameters May be {@code null}.
     * @return May be {@code null}.
     */
    public Predicate parse(String predicateString, Object... parameters) {
        if (predicateString == null) {
            return null;
        }

        Queue<String> tokens = new LinkedList<String>();

        char[] predicateChars = predicateString.toCharArray();
        for (int i = 0, length = predicateChars.length; i < length; ++ i) {

            char c = predicateChars[i];
            if (Character.isWhitespace(c)) {
                continue;
            }

            StringBuilder tokenBuilder = new StringBuilder();
            if ("()[],".indexOf(c) > -1) {
                tokenBuilder.append(c);

            } else if ("'\"".indexOf(c) > -1) {
                char quote = c;
                for (++ i; i < length; ++ i) {
                    c = predicateChars[i];
                    if (c == '\\') {
                        ++ i;
                        if (i < length) {
                            tokenBuilder.append(predicateChars[i]);
                        }
                    } else if (c == quote) {
                        break;
                    } else {
                        tokenBuilder.append(c);
                    }
                }

            } else {
                tokenBuilder.append(c);
                for (++ i; i < length; ++ i) {
                    c = predicateChars[i];
                    if ("),".indexOf(c) > -1) {
                        -- i;
                        break;
                    } else if (Character.isWhitespace(c)) {
                        break;
                    } else {
                        tokenBuilder.append(c);
                    }
                }
            }

            tokens.add(tokenBuilder.toString());
        }

        return readPredicate(tokens, new ParameterList(parameters));
    }

    @SuppressWarnings("serial")
    private static class ParameterList extends ArrayList<Object> {

        private int next;

        public ParameterList(Object... parameters) {
            if (parameters != null && parameters.length > 0) {
                Collections.addAll(this, parameters);
            }
        }

        public Object poll() {
            if (next < size()) {
                Object item = get(next);
                ++ next;
                return item;
            } else {
                return null;
            }
        }
    }

    // Reads: group (compoundOperator group)*
    private Predicate readPredicate(Queue<String> tokens, ParameterList parameters) {
        Predicate predicate = readGroup(tokens, parameters);

        if (predicate != null) {
            for (String operator; (operator = tokens.peek()) != null;) {

                operator = operator.toLowerCase(Locale.ENGLISH);
                String compoundOperator = getCompoundOperators().get(operator);
                if (compoundOperator == null) {
                    break;
                }

                tokens.remove();
                predicate = CompoundPredicate.combine(
                        compoundOperator,
                        predicate,
                        readGroup(tokens, parameters));
            }
        }

        return predicate;
    }

    // Reads: '(' predicate ')'
    private Predicate readGroup(Queue<String> tokens, ParameterList parameters) {
        Predicate predicate = null;
        String nextToken = tokens.peek();

        if ("(".equals(nextToken)) {
            tokens.remove();
            predicate = readPredicate(tokens, parameters);
            if (predicate == null) {
                throw new IllegalArgumentException("Empty group!");
            } else if (!")".equals(tokens.poll())) {
                throw new IllegalArgumentException(String.format(
                        "Unmatched ( after [%s]!", predicate));
            }

        } else if ("not".equals(nextToken)
                || NOT_OPERATOR.equals(nextToken)) {
            tokens.remove();
            predicate = new CompoundPredicate(
                    NOT_OPERATOR,
                    Arrays.asList(readGroup(tokens, parameters)));

        } else {
            predicate = readComparison(tokens, parameters);
        }

        return predicate;
    }

    // Reads: value | '[' value1, value2, valueN ']'
    private Object readValue(Queue<String> tokens) {
        String token = tokens.poll();
        if (!"[".equals(token)) {
            return token;

        } else {
            List<Object> values = new ArrayList<Object>();
            while (true) {
                if ("]".equals(tokens.peek())) {
                    tokens.remove();
                    break;
                }

                Object value = readValue(tokens);
                values.add(value);

                String delimiter = tokens.poll();
                if ("]".equals(delimiter)) {
                    break;

                } else if (!",".equals(delimiter)) {
                    throw new IllegalArgumentException(String.format(
                            "Expected a comma after [%s]!", value));
                }
            }

            return values;
        }
    }

    // Reads: key operator value
    private Predicate readComparison(Queue<String> tokens, ParameterList parameters) {
        String key = tokens.poll();
        if (key == null) {
            return null;
        }

        String realKey = Query.Static.getCanonicalKey(key);
        if (realKey != null) {
            key = realKey;
        }

        String operator = tokens.poll();
        boolean isIgnoreCase = false;
        if (operator == null) {
            throw new IllegalArgumentException(String.format(
                    "No operator after [%s] key!", key));
        } else {
            operator = operator.toLowerCase(Locale.ENGLISH);
            if (operator.endsWith("[c]")) {
                operator = operator.substring(0, operator.length() - 3);
                isIgnoreCase = true;
            }
        }

        Object value = readValue(tokens);
        if (value == null) {
            throw new IllegalArgumentException(String.format(
                    "No value after [%s] key and [%s] operator!",
                    key, operator));

        } else if (value instanceof String) {
            String valueString = (String) value;

            if (valueString.startsWith("?")) {
                if (valueString.length() == 1) {
                    value = parameters.poll();

                } else {
                    String path = valueString.substring(1);

                    // Parse a leading integer as a parameter index.
                    String indexString = "";
                    for (char c : path.toCharArray()) {

                        int digit = Character.digit(c, 10);

                        if (digit == -1) {
                            break;

                        }

                        indexString += c;
                    }

                    // Default index to 0 if indexString is empty.
                    int index = ObjectUtils.to(int.class, indexString);

                    // Remove an optional leading parameter index.
                    path = path.substring(indexString.length());

                    // Remove an optional leading forward slash.
                    if (path.indexOf('/') == 0) {
                        path = path.substring(1);
                    } else if (path.length() > 0) {
                        throw new IllegalArgumentException(String.format(
                                "Missing [/] between [%s] and [%s]!",
                                valueString.substring(0, 1 + indexString.length()),
                                valueString.substring(1 + indexString.length())));
                    }

                    // Obtain parameter from the specified ParameterList using the parameter index.
                    value = index < parameters.size() ? parameters.get(index) : null;

                    if (value != null && path.length() > 0) {

                        // Produce a value using the specified parameter and path.
                        if (value instanceof State) {
                            value = ((State) value).getByPath(path);
                        } else if (value instanceof Recordable) {
                            value = ((Recordable) value).getState().getByPath(path);
                        } else {
                            value = CollectionUtils.getByPath(value, path);
                        }
                    }
                }

            } else if ("true".equalsIgnoreCase(valueString)) {
                value = Boolean.TRUE;

            } else if ("false".equalsIgnoreCase(valueString)) {
                value = Boolean.FALSE;

            } else if ("null".equalsIgnoreCase(valueString)) {
                value = null;

            } else if ("missing".equalsIgnoreCase(valueString)) {
                value = Query.MISSING_VALUE;
            }
        }

        String comparisonOperator = getComparisonOperators().get(operator);
        if (comparisonOperator != null) {

            if (value instanceof State) {
                return new ComparisonPredicate(
                        comparisonOperator,
                        isIgnoreCase,
                        key,
                        Collections.singleton(value));
            } else {
                return new ComparisonPredicate(
                        comparisonOperator,
                        isIgnoreCase,
                        key,
                        ObjectUtils.to(Iterable.class, value));
            }

        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid comparison operator!",
                    operator));
        }
    }

    /**
     * Returns {@code true} if the given {@code predicate} matches
     * the given {@code object}.
     *
     * @param predicate If {@code null}, returns {@code true}.
     * @throws UnsupportedOperationException If the given {@code predicate}
     * operator isn't supported.
     */
    public boolean evaluate(Object object, Predicate predicate) {
        if (predicate == null) {
            return true;
        }

        String operator = predicate.getOperator();
        Evaluator evaluator = evaluators.get(operator);

        if (evaluator == null) {
            throw new UnsupportedOperationException(String.format(
                    "[%s] operator not supported!", operator));
        }

        return evaluator.evaluate(this, object, predicate);
    }

    public interface Evaluator {

        public boolean evaluate(PredicateParser parser, Object object, Predicate predicate);
    }

    private static class AndEvaluator implements Evaluator {

        @Override
        public boolean evaluate(PredicateParser parser, Object object, Predicate predicate) {
            for (Predicate child : ((CompoundPredicate) predicate).getChildren()) {
                if (!parser.evaluate(object, child)) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class OrEvaluator implements Evaluator {

        @Override
        public boolean evaluate(PredicateParser parser, Object object, Predicate predicate) {
            for (Predicate child : ((CompoundPredicate) predicate).getChildren()) {
                if (parser.evaluate(object, child)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class NotEvaluator extends OrEvaluator {

        @Override
        public boolean evaluate(PredicateParser parser, Object object, Predicate predicate) {
            return !super.evaluate(parser, object, predicate);
        }
    }

    private abstract static class ComparisonEvaluator implements Evaluator {

        @Override
        public final boolean evaluate(PredicateParser parser, Object object, Predicate predicate) {
            State state = State.getInstance(object);
            ComparisonPredicate comparison = (ComparisonPredicate) predicate;
            Object keyValue;

            if (Query.ANY_KEY.equals(comparison.getKey())) {
                if (Collections.singletonList("*").equals(comparison.getValues())) {
                    return true;
                }
                List<Object> values = new ArrayList<Object>();
                for (ObjectIndex index : state.getIndexes()) {
                    for (String field : index.getFields()) {
                        values.add(state.getByPath(field));
                    }
                }
                keyValue = values;

            } else {
                keyValue = state.getByPath(comparison.getKey());
            }

            List<Object> values = comparison.resolveValues(state.getDatabase());

            if (keyValue == null) {
                keyValue = Query.MISSING_VALUE;
                return compare(state, keyValue, values);

            } else if (keyValue instanceof Iterable) {
                for (Object item : (Iterable<?>) keyValue) {
                    List<Object> valuesCopy = new ArrayList<>(values);
                    if (evaluateOne(state, item, valuesCopy)) {
                        return true;
                    }
                }
                return false;

            } else {
                return evaluateOne(state, keyValue, values);
            }
        }

        private boolean evaluateOne(State state, Object keyValue, List<Object> values) {
            if (!(keyValue instanceof Recordable || keyValue instanceof UUID) && keyValue != null) {
                Class<?> keyValueClass = keyValue.getClass();
                for (ListIterator<Object> i = values.listIterator(); i.hasNext();) {
                    i.set(ObjectUtils.to(keyValueClass, i.next()));
                }
            }
            return compare(state, keyValue, values);
        }

        protected abstract boolean compare(State state, Object keyValue, List<Object> values);
    }

    private static class EqualsAnyEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            if (keyValue instanceof String) {
                String keyValueString = ((String) keyValue).trim();
                for (Object value : values) {
                    if (value != null
                            && keyValueString.equalsIgnoreCase(value.toString().trim())) {
                        return true;
                    }
                }

            } else {
                for (Object value : values) {
                    if (getIdOrObject(keyValue).equals(getIdOrObject(value))) {
                        return true;
                    }
                }
            }

            return false;
        }

        private Object getIdOrObject(Object object) {
            if (object instanceof Recordable) {
                return ((Recordable) object).getState().getId();

            } else if (object instanceof State) {
                return ((State) object).getId();

            } else {
                return object;
            }
        }
    }

    private static class NotEqualsAllEvaluator extends EqualsAnyEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            return !super.compare(state, keyValue, values);
        }
    }

    private static class LessThanEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            for (Object value : values) {
                if (ObjectUtils.compare(keyValue, value, false) >= 0) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class LessThanOrEqualsEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            for (Object value : values) {
                if (ObjectUtils.compare(keyValue, value, false) > 0) {
                    return false;
                }
            }
            return true;
        }
    }

    private static class GreaterThanEvaluator extends LessThanOrEqualsEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            return !super.compare(state, keyValue, values);
        }
    }

    private static class GreaterThanOrEqualsEvaluator extends LessThanEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            return !super.compare(state, keyValue, values);
        }
    }

    private static class StartsWithEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            if (keyValue != null) {
                String keyValueString = keyValue.toString().trim().toLowerCase(Locale.ENGLISH);
                for (Object value : values) {
                    if (value != null
                            && keyValueString.startsWith(value.toString().trim().toLowerCase(Locale.ENGLISH))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static class ContainsEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            if (keyValue != null) {
                String keyValueString = keyValue.toString().trim().toLowerCase(Locale.ENGLISH);
                for (Object value : values) {
                    if (value != null
                            && keyValueString.contains(value.toString().trim().toLowerCase(Locale.ENGLISH))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static class MatchesAnyEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            if (keyValue != null) {
                String keyValueString = keyValue.toString().trim().toLowerCase(Locale.ENGLISH);
                for (Object value : values) {
                    if (value != null
                            && keyValueString.contains(value.toString().trim().toLowerCase(Locale.ENGLISH))) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    private static class MatchesAllEvaluator extends ComparisonEvaluator {

        @Override
        protected boolean compare(State state, Object keyValue, List<Object> values) {
            if (keyValue != null) {
                String keyValueString = keyValue.toString().trim().toLowerCase(Locale.ENGLISH);
                for (Object value : values) {
                    if (value == null
                            || !keyValueString.contains(value.toString().trim().toLowerCase(Locale.ENGLISH))) {
                        return false;
                    }
                }
            }
            return true;
        }
    }

    /** {@link PredicateParser} utility methods. */
    public static final class Static {

        private static final PredicateParser DEFAULT_PARSER = new PredicateParser();

        /**
         * Parses the given {@code predicateString} along with the given
         * {@code parameters} into a predicate object.
         */
        public static Predicate parse(String predicateString, Object... parameters) {
            return DEFAULT_PARSER.parse(predicateString, parameters);
        }

        /**
         * Returns {@code true} if the given {@code predicate} matches
         * the given {@code object}.
         */
        public static boolean evaluate(Object object, Predicate predicate) {
            return DEFAULT_PARSER.evaluate(object, predicate);
        }

        /**
         * Returns {@code true} if the given {@code predicateString} along
         * with the given {@code parameters} matches the given {@code object}.
         */
        public static boolean evaluate(Object object, String predicateString, Object... parameters) {
            return evaluate(object, parse(predicateString, parameters));
        }
    }
}
