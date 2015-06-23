package com.psddev.dari.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Queue;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PredicateParserTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PredicateParserTest.class);

    private static final String REFLECTIVE_IDENTITY_SYNTAX_STANDARD = "?";
    private static final String REFLECTIVE_IDENTITY_DELIMITED_SYNTAX = "?/";
    private static final String REFLECTIVE_IDENTITY_REDUNDANT_SYNTAX = "?_id";
    private static final String REFLECTIVE_IDENTITY_DELIMITED_REDUNDANT_SYNTAX = "?/_id";
    private static final String REFLECTIVE_IDENTITY_INDEXED_SYNTAX = "?0";
    private static final String REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX = "?0/";
    private static final String REFLECTIVE_IDENTITY_INDEXED_REDUNDANT_SYNTAX = "?0_id";
    private static final String REFLECTIVE_IDENTITY_INDEXED_DELIMITED_REDUNDANT_SYNTAX = "?0/_id";

    private static List<TestDatabase> TEST_DATABASES;
    private static List<Database> DATABASES;

	PredicateParser parser = new PredicateParser();
	static final Queue<Object> EMPTY_OBJECT_QUEUE = new ArrayDeque<Object>();
	static final Queue<String> EMPTY_STRING_QUEUE = new ArrayDeque<String>();

    @BeforeClass
    public static void beforeClass() {

        TEST_DATABASES = DatabaseTestUtils.getNewDefaultTestDatabaseInstances();
        DATABASES = new ArrayList<Database>();

        for (TestDatabase testDb : TEST_DATABASES) {
            Database db = testDb.get();
            DATABASES.add(db);
        }

        LOGGER.info("Running tests against " + TEST_DATABASES.size() + " databases.");
    }

    @AfterClass
    public static void afterClass() {
        if (TEST_DATABASES != null) for (TestDatabase testDb : TEST_DATABASES) {
            testDb.close();
        }
    }

    @Before
    public void before() {

    }

    @After
    public void after() {

    }

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

    @Test
    public void parse_null() {
    	assertEquals(null, parser.parse(null));
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

    /**
     * Utility method for testing different identity syntax formats.
     * @param identitySyntax the format string to test
     */
    private void parse_identity_syntax_recordable(String identitySyntax) {

        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);

            Predicate predicate = parser.parse("record = " + identitySyntax, current);
            Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "record", Arrays.asList(current.getId()));
            assertEquals(expect, predicate);
        }
    }

    /**
     * Utility method for testing unsupported identity syntax formats.
     * @param identitySyntax the format string to test
     */
    private void illegal_identity_syntax_recordable(String identitySyntax) {

        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);
            parser.parse("record = " + identitySyntax, current);
        }
    }

    /**
     * Utility method for testing unsupported identity syntax formats in parsing predicates with field expressions.
     * @param identitySyntax the format string to test
     */
    private void illegal_identity_with_field_syntax_recordable(String identitySyntax) {
        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);
            current.setName("testName");
            parser.parse("record = " + identitySyntax + "name", current);
        }
    }

    /**
     * Utility method for testing different identity syntax formats in parsing predicates with field expressions.
     * @param identitySyntax the format string to test
     */
    private void parse_identity_with_field_syntax_recordable(String identitySyntax) {
        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);
            current.setName("testName");

            Predicate predicate = parser.parse("record = " + identitySyntax + "name", current);
            Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "record", Arrays.asList(current.getName()));
            assertEquals(expect, predicate);
        }
    }

    /**
     * Test the reduction of a {@link State} to a UUID in {@link ComparisonPredicate}
     */
    @Test
    public void parse_identity_state() {
        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);

            Predicate predicate = parser.parse("record = ?", current.getState());
            Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "record", Arrays.asList(current.getId()));
            assertEquals(expect, predicate);
        }
    }

    /**
     * Test reduction of a {@link Recordable} to a String field value in {@link ComparisonPredicate}
     */
    @Test
    public void parse_name_spaced_field_recordable() {
        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);
            current.setName("testName");

            Predicate predicate = parser.parse("record = ?/" + TestRecord.class.getName() + "/name", current);
            Predicate expect = new ComparisonPredicate(PredicateParser.EQUALS_ANY_OPERATOR, false, "record", Arrays.asList(current.getName()));
            assertEquals(expect, predicate);
        }
    }

    /** Supported Syntax Tests **/

    /** Test reduction of a Recordable to a UUID using the identity syntax "?" **/
    @Test
    public void parse_identity_standard_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_SYNTAX_STANDARD);
    }

    /** Test reduction of a Recordable to a UUID using the identity syntax "?/" **/
    @Test
    public void parse_identity_delimited_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_DELIMITED_SYNTAX);
    }

    /** Test reduction of a Recordable to a String field value using the syntax "?/field" **/
    @Test
    public void parse_identity_delimited_syntax_with_field() {
        parse_identity_with_field_syntax_recordable(REFLECTIVE_IDENTITY_DELIMITED_SYNTAX);
    }

    /** Test reduction of a Recordable to a UUID using the identity syntax "?/_id" **/
    @Test
    public void parse_identity_delimited_redundant_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_DELIMITED_REDUNDANT_SYNTAX);
    }

    /** Test reduction of a Recordable to a UUID using the identity syntax "?0" **/
    @Test
    public void parse_identity_indexed_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_SYNTAX);
    }

    /** Test reduction of a Recordable to a UUID using the identity syntax "?0/" **/
    @Test
    public void parse_identity_indexed_delimited_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX);
    }

    /** Test reduction of a Recordable to a String field value using the syntax "?0/field" **/
    @Test
    public void parse_identity_indexed_delimited_syntax_with_field() {
        parse_identity_with_field_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_SYNTAX);
    }

    /** Test reduction of a Recordable to a UUID using the identity syntax "?0/_id" **/
    @Test
    public void parse_identity_indexed_delimited_redundant_syntax() {

        parse_identity_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_DELIMITED_REDUNDANT_SYNTAX);
    }

    /** Illegal Syntax Tests, Expected Failures **/

    /** Fail reduction of a Recordable to a String field value using the syntax "?field" **/
    @Test(expected=IllegalArgumentException.class)
    public void illegal_identity_standard_syntax_with_field() {
        illegal_identity_with_field_syntax_recordable(REFLECTIVE_IDENTITY_SYNTAX_STANDARD);
    }

    /** Fail reduction of a Recordable to a UUID using the identity syntax "?_id" **/
    @Test(expected=IllegalArgumentException.class)
    public void illegal_identity_redundant_syntax() {

        illegal_identity_syntax_recordable(REFLECTIVE_IDENTITY_REDUNDANT_SYNTAX);
    }

    /** Fail reduction of a Recordable to a String field value using the syntax "?0field" **/
    @Test(expected=IllegalArgumentException.class)
    public void illegal_identity_indexed_syntax_with_field() {
        illegal_identity_with_field_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_SYNTAX);
    }

    /** Fail reduction of a Recordable to a UUID using the identity syntax "?0_id" **/
    @Test(expected=IllegalArgumentException.class)
    public void illegal_identity_indexed_redundant_syntax() {

        illegal_identity_syntax_recordable(REFLECTIVE_IDENTITY_INDEXED_REDUNDANT_SYNTAX);
    }

    /**
     * Utility method for testing different identity syntax formats in parsing predicates with field expressions.
     */
    @Test
    public void evaluate_any_syntax_hysteresis() {
        for (Database database : DATABASES) {

            TestRecord current = TestRecord.getInstance(database);
            current.setName("testName");

            HysteresisTestRecord other = HysteresisTestRecord.getInstance(database);
            other.setIndexedDate(new Date());
            other.setIndexedName("testName");

            Predicate predicate = parser.parse("_any matches ?/name", current);
            Predicate expected = new ComparisonPredicate(PredicateParser.MATCHES_ANY_OPERATOR, false, Query.ANY_KEY, Arrays.asList(current.getName()));

            assertEquals(predicate, expected);

            assertTrue(PredicateParser.Static.evaluate(other, predicate));
        }
    }

    /**
     * For use in testing {@link PredicateParser.Static#evaluate} against hysteresis
     * effects of evaluating successive incompatibly-typed fields.
     */
    public static class HysteresisTestRecord extends Record {

        public static HysteresisTestRecord getInstance(Database database) {

            HysteresisTestRecord object = new HysteresisTestRecord();
            object.getState().setDatabase(database);
            return object;
        }

        // indexedDate MUST be defined before indexedName
        @Indexed
        private Date indexedDate;

        @Indexed
        private String indexedName;

        public Date getIndexedDate() {
            return indexedDate;
        }

        public void setIndexedDate(Date indexedDate) {
            this.indexedDate = indexedDate;
        }

        public String getIndexedName() {
            return indexedName;
        }

        public void setIndexedName(String indexedName) {
            this.indexedName = indexedName;
        }
    }

    /** Test Record type for parser identity syntax tests **/
    public static class TestRecord extends Record {

        public static TestRecord getInstance(Database database) {

            TestRecord object = new TestRecord();
            object.getState().setDatabase(database);
            return object;
        }

        private String name;

        private TestRecord other;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public TestRecord getOther() {
            return other;
        }

        public void setOther(TestRecord other) {
            this.other = other;
        }
    }

    /*
     * Options // TODO: Add tests here when we know what this should look like
     */

}
