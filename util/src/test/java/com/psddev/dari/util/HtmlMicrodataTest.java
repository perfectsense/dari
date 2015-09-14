package com.psddev.dari.util;

import com.psddev.dari.util.TestUtils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.psddev.dari.util.ObjectUtils;

/**
 * Created by rhseeger on 12/17/14.
 */
public class HtmlMicrodataTest {
    @Rule public TestName testName = new TestName();
    public static URL testUrl;

    @BeforeClass
    public static void beforeClass() {
        try {
            testUrl = new URL("http://www.test.me/this/is/a/test/url");
        } catch(Exception ex) {}
    }

    @AfterClass
    public static void afterClass() {}

    @Before
    public void before() {
        System.out.println("Running test [" + testName.getMethodName() + "]");
    }

    @After
    public void after() {}

	/* public static HtmlMicrodata getFirstType(List<HtmlMicrodata> htmlMicrodatas, Collection<String> allowedSchemaTypes) */

    @Test
    public void Static_getFirstType_valid() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] {
                "http://schema.org/Recipe"
        }));
        assertEquals("http://schema.org/Recipe", microdata.getFirstType());
    }

    @Test
    public void Static_getFirstType_notFirst() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] {
                "http://schema.org/NotRecipe",
                "http://schema.org/Recipe"
        }));
        assertEquals("http://schema.org/Recipe", microdata.getFirstType());
    }

    @Test
    public void Static_getFirstType_invalid() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] {
                "http://schema.org/NotRecipe"
        }));
        assertEquals(null, microdata);
    }

    /* Generic parsing functionality, values */

    @Test
    public void Static_parse_value_text() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] { "http://schema.org/Recipe" }));
        assertEquals("Mexican Rigatoni and Cheese", microdata.getFirstProperty("name"));
    }

    @Test
    public void Static_parse_value_content() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] { "http://schema.org/Recipe" }));
        assertEquals("PT30M", microdata.getFirstProperty("prepTime"));
    }


    @Test
    public void Static_parse_time_datetime() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] { "http://schema.org/Recipe" }));
        assertEquals("PT15M", microdata.getFirstProperty("cookTimeDateTime"));
    }

    @Test
    public void Static_parse_time_content() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] { "http://schema.org/Recipe" }));
        assertEquals("PT14M", microdata.getFirstProperty("cookTimeContent"));
    }

    @Test
    public void Static_parse_time_value() {
        List<HtmlMicrodata> microdatas = HtmlMicrodata.Static.parseDocument(testUrl, getDocument("simple-content.html"));
        HtmlMicrodata microdata = HtmlMicrodata.Static.getFirstType(microdatas, Arrays.asList(new String[] { "http://schema.org/Recipe" }));
        assertEquals("13 mins", microdata.getFirstProperty("cookTimeValue"));
    }


    /** UTILITY **/
    public static Document getDocument(String file) {
        try {
            String html = TestUtils.loadSampleFile("com/psddev/dari/util/HtmlMicrodata_Test/" + file);
            Parser parser = Parser.htmlParser();
            return parser.parseInput(html, testUrl.toString());
        } catch(IOException ex) {
            return null;
        }
    }

}
