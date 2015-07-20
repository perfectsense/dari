package com.psddev.dari.util;

import com.google.common.collect.testing.SetTestSuiteBuilder;
import com.google.common.collect.testing.TestStringSetGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.SetFeature;
import junit.framework.TestSuite;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.Collections;
import java.util.Set;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        CompactSetTest.GuavaTests.class
})
public class CompactSetTest {

    public static class GuavaTests {

        public static TestSuite suite() {
            return SetTestSuiteBuilder.using(new TestStringSetGenerator() {

                @Override
                protected Set<String> create(String[] elements) {
                    Set<String> set = new CompactSet<>();

                    if (elements != null) {
                        Collections.addAll(set, elements);
                    }

                    return set;
                }
            })
                    .named("CompactSet Guava Tests")
                    .withFeatures(
                            CollectionSize.ANY,

                            CollectionFeature.ALLOWS_NULL_QUERIES,
                            CollectionFeature.KNOWN_ORDER,
                            CollectionFeature.REMOVE_OPERATIONS,

                            SetFeature.GENERAL_PURPOSE
                    )
                    .createTestSuite();
        }
    }
}
