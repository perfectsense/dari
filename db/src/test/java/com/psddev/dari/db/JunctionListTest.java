package com.psddev.dari.db;

import com.google.common.collect.testing.ListTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestListGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.ListFeature;
import junit.framework.TestSuite;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

@RunWith(Suite.class)
@Suite.SuiteClasses({
        JunctionListTest.General.class,
        JunctionListTest.Guava.class
})
public class JunctionListTest {

    public static class General {

        @Test(expected = NullPointerException.class)
        public void constructStateNull() {
            new JunctionList(null, mock(ObjectField.class));
        }

        @Test(expected = NullPointerException.class)
        public void constructFieldNull() {
            new JunctionList(mock(State.class), null);
        }
    }

    public static class Guava {

        public static TestSuite suite() {
            return ListTestSuiteBuilder.using(new JunctionListGenerator())
                    .named(Guava.class.getName())
                    .withFeatures(
                            CollectionSize.ANY,

                            CollectionFeature.ALLOWS_NULL_QUERIES,
                            CollectionFeature.KNOWN_ORDER,
                            CollectionFeature.REMOVE_OPERATIONS,

                            ListFeature.GENERAL_PURPOSE
                    )
                    .createTestSuite();
        }

        private static class JunctionListGenerator implements TestListGenerator<Object> {

            @Override
            public List<Object> create(Object... elements) {
                State state = mock(State.class);
                ObjectField field = mock(ObjectField.class);

                when(field.findJunctionItems(state)).thenReturn(new ArrayList<>());

                List<Object> list = new JunctionList(state, field);

                if (elements != null) {
                    Collections.addAll(list, elements);
                }

                return list;
            }

            @Override
            public SampleElements<Object> samples() {
                return new SampleElements<>("b", "a", "c", "d", "e");
            }

            @Override
            public Object[] createArray(int length) {
                return new Object[length];
            }

            @Override
            public Iterable<Object> order(List<Object> insertionOrder) {
                return insertionOrder;
            }
        }
    }
}
