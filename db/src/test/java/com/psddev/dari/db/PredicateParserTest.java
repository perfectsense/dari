package com.psddev.dari.db;

import static org.junit.Assert.assertEquals;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PredicateParserTest {
	PredicateParser parser = new PredicateParser();
	static final Queue<Object> EMPTY_OBJECT_QUEUE = new ArrayDeque<Object>();
	static final Queue<String> EMPTY_STRING_QUEUE = new ArrayDeque<String>();

    @Before
    public void before() {}

    @After
    public void after() {}

    /**
     * public Predicate parse(String predicateString, Object... parameters)
     */
    @Test
    public void parse_empty() {
    	assertEquals(null, parser.parse(""));
    }
    @Test
    public void parse_empty_2() {
    	assertEquals(null, parser.parse(" "));
    }

    @Test (expected=NullPointerException.class)
    public void parse_null() {
    	parser.parse(null);
    }

    @Test
    public void parse_simple() {
    	Predicate pred = parser.parse("a = 1");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_nospace() {
    	parser.parse("a=1");
    }

    @Test
    public void parse_compound() {
    	Predicate pred = parser.parse("a = 1 and b != 2");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.AND_OPERATOR,
    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
                new ComparisonPredicate(PredicateParser.NOT_EQUALS_ALL_OPERATOR, false, "b", Arrays.asList("2"))
    	);
    	assertEquals(expect, pred);
    }

    @Test // left to right
    public void parse_noparens() {
    	Predicate pred = parser.parse("a = 1 and b = 2 or c = 3");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.OR_OPERATOR,
    			CompoundPredicate.combine(
    			        PredicateParser.AND_OPERATOR,
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("2"))
    			),
    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "c", Arrays.asList("3"))
    	);
    	assertEquals(expect, pred);
    }

    @Test // to group
    public void parse_parens() {
    	Predicate pred = parser.parse("a = 1 and (b = 2 or c = 3)");
    	Predicate expect = CompoundPredicate.combine(
    	        PredicateParser.AND_OPERATOR,
				new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList("1")),
    			CompoundPredicate.combine(
    			        PredicateParser.OR_OPERATOR,
    					new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("2")),
    	    			new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "c", Arrays.asList("3"))
    			)
    	);
    	assertEquals(expect, pred);
    }
    
    @Test
    public void parse_matches_fuzzy(){
    	Predicate pred = parser.parse("a ~ 1");
    	Predicate expect = new ComparisonPredicate(PredicateParser.MATCHES_FUZZY_OPERATOR, false, "a", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    /*
     * Key handling
     */
    @Test
    public void parse_key_canonical_id() {
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "_id", Arrays.asList("1"));
    	assertEquals(expect, parser.parse("id = 1"));
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_key_missing() {
    	parser.parse(" = 1 ");
    }

    /*
     * Comparison handling
     */
    @Test
    public void parse_operator_negative() {
    	Predicate expect = new ComparisonPredicate(PredicateParser.NOT_EQUALS_ALL_OPERATOR, false, "token1", Arrays.asList("1"));
    	assertEquals(expect, parser.parse("token1 != 1"));
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_operator_missing() {
    	parser.parse("a 1");
    }

    /*
     * Value handling
     */
    @Test (expected=IllegalArgumentException.class)
    public void parse_error_value_missing() {
    	parser.parse("a = ");
    }

    @Test
    public void parse_value_boolean_true() {
    	ComparisonPredicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			Arrays.asList(true));
    	assertEquals(expect, parser.parse("token1 = true"));
    }

    @Test
    public void parse_value_boolean_false() {
    	Predicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			Arrays.asList(false));
    	assertEquals(expect, parser.parse("token1 = false"));
    }

    @Test
    public void parse_value_null() {
    	Predicate expect = new ComparisonPredicate(
    	        PredicateParser.EQUALS_ANY_OPERATOR,
    			false,
    			"token1",
    			null);
    	assertEquals(expect, parser.parse("token1 = null"));
    }

    /*
     * Params
     */
    @Test
    public void parse_params() {
    	Predicate pred = parser.parse("b = ?", "1");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_multivalue() {
    	Predicate pred = parser.parse("a in ?", Arrays.asList(1,2,3));
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "a", Arrays.asList(1,2,3));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_notstring() {
    	Predicate pred = parser.parse("b = ?", 1);
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList(1));
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_params_null() {
    	Predicate pred = parser.parse("b = ?", (Object)null);
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", null);
    	assertEquals(expect, pred);
    }
    @Test
    public void parse_params_missing() {
    	Predicate pred = parser.parse("b = ?");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", null);
    	assertEquals(expect, pred);
    }

    @Test
    public void parse_extra() {
    	Predicate pred = parser.parse("b = ?", "1", "2");
    	Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "b", Arrays.asList("1"));
    	assertEquals(expect, pred);
    }

    @Test (expected=IllegalArgumentException.class)
    public void parse_parens_empty() {
    	assertEquals(null, parser.parse(" ( ) "));
    }

    /*
     * Options // TODO: Add tests here when we know what this should look like
     */

}
