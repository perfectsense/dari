package com.psddev.dari.util;

import com.google.common.collect.testing.MapTestSuiteBuilder;
import com.google.common.collect.testing.TestStringMapGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.MapFeature;
import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Map;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    CompactMapTest.GuavaTests.class
})
public class CompactMapTest {

    public static class GuavaTests {

        public static TestSuite suite() {
            return MapTestSuiteBuilder.using(new TestStringMapGenerator() {

                @Override
                protected Map<String, String> create(Map.Entry<String, String>[] entries) {
                    Map<String, String> map = new CompactMap<>();

                    if (entries != null) {
                        for (Map.Entry<String, String> entry : entries) {
                            map.put(entry.getKey(), entry.getValue());
                        }
                    }

                    return map;
                }
            }).
                named("CompactMap Guava Tests").
                withFeatures(
                    CollectionSize.ANY,

                    CollectionFeature.ALLOWS_NULL_QUERIES,
                    CollectionFeature.KNOWN_ORDER,
                    CollectionFeature.REMOVE_OPERATIONS,

                    MapFeature.ALLOWS_ANY_NULL_QUERIES,
                    MapFeature.ALLOWS_NULL_KEYS,
                    MapFeature.ALLOWS_NULL_VALUES,
                    MapFeature.GENERAL_PURPOSE
                ).
                createTestSuite();
        }
    }
}