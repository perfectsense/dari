package com.psddev.dari.db;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class AtomicOperationTest {

    private static final String FIELD_NAME = "test";

    private static void equalsContractSame(AtomicOperation operation1, AtomicOperation operation2) {
        assertThat(operation1.equals(operation2), equalTo(true));
        assertThat(operation1.hashCode(), equalTo(operation2.hashCode()));
    }

    private static void equalsContractDifferent(AtomicOperation operation1, AtomicOperation operation2) {
        assertThat(operation1.equals(operation2), equalTo(false));
        assertThat(operation1.hashCode(), not(equalTo(operation2.hashCode())));
    }

    public static class Abstract {

        @Test(expected = NullPointerException.class)
        public void constructFieldNull() {
            new NullAtomicOperation(null);
        }

        @Test
        public void getField() {
            AtomicOperation operation = new NullAtomicOperation(FIELD_NAME);

            assertThat(operation.getField(), equalTo(FIELD_NAME));
        }

        private static class NullAtomicOperation extends AtomicOperation {

            public NullAtomicOperation(String field) {
                super(field);
            }

            @Override
            public void execute(State state) {
            }
        }
    }

    public static class Increment {

        private double value;
        private AtomicOperation.Increment operation;
        private State state;

        @Before
        public void before() {
            value = 1.0;
            operation = new AtomicOperation.Increment(FIELD_NAME, value);
            state = mock(State.class);
        }

        @Test
        public void executeNumber() {
            double existingNumber = 1.0;

            when(state.getByPath(FIELD_NAME)).thenReturn(existingNumber);

            operation.execute(state);

            verify(state).putByPath(FIELD_NAME, existingNumber + value);
        }

        private void verifyNonNumber() {
            verify(state).putByPath(FIELD_NAME, value);
        }

        @Test
        public void executeNull() {
            operation.execute(state);

            verifyNonNumber();
        }

        @Test
        public void executeNonNumber() {
            when(state.getByPath(FIELD_NAME)).thenReturn(new Object());

            operation.execute(state);

            verifyNonNumber();
        }

        @Test
        public void equalsContract() {
            equalsContractSame(operation, new AtomicOperation.Increment(FIELD_NAME, value));
            equalsContractDifferent(operation, new AtomicOperation.Increment(FIELD_NAME, value + 1.0));
        }
    }

    public static class Add {

        private Object item;
        private AtomicOperation.Add operation;
        private State state;

        @Before
        public void before() {
            item = new Object();
            operation = new AtomicOperation.Add(FIELD_NAME, item);
            state = mock(State.class);
        }

        @Test
        public void executeCollection() {
            List<Object> existingValues = new ArrayList<>();

            existingValues.add(new Object());
            when(state.getByPath(FIELD_NAME)).thenReturn(existingValues);

            operation.execute(state);

            assertThat(existingValues, hasSize(2));
            assertThat(existingValues, hasItem(item));
        }

        private void verifyNonCollection() {
            ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);

            verify(state).putByPath(eq(FIELD_NAME), valueCaptor.capture());

            Object value = valueCaptor.getValue();

            assertThat(value, instanceOf(Collection.class));

            Collection<?> valueCollection = (Collection<?>) value;

            assertThat(valueCollection, contains(item));
        }

        @Test
        public void executeNull() {
            operation.execute(state);

            verifyNonCollection();
        }

        @Test
        public void executeNonCollection() {
            when(state.getByPath(FIELD_NAME)).thenReturn(new Object());

            operation.execute(state);

            verifyNonCollection();
        }

        @Test
        public void equalsContract() {
            equalsContractSame(operation, new AtomicOperation.Add(FIELD_NAME, item));
            equalsContractDifferent(operation, new AtomicOperation.Add(FIELD_NAME, new Object()));
        }
    }

    public static class Remove {

        private Object item;
        private AtomicOperation.Remove operation;
        private State state;

        @Before
        public void before() {
            item = new Object();
            operation = new AtomicOperation.Remove(FIELD_NAME, item);
            state = mock(State.class);
        }

        @Test
        public void executeCollection() {
            List<Object> existingValues = new ArrayList<>();

            existingValues.add(item);
            when(state.getByPath(FIELD_NAME)).thenReturn(existingValues);

            operation.execute(state);

            assertThat(existingValues, empty());
        }

        private void verifyNonCollection() {
            verify(state).getByPath(FIELD_NAME);
            verifyNoMoreInteractions(state);
        }

        @Test
        public void executeNull() {
            operation.execute(state);

            verifyNonCollection();
        }

        @Test
        public void executeNonCollection() {
            when(state.getByPath(FIELD_NAME)).thenReturn(new Object());

            operation.execute(state);

            verifyNonCollection();
        }

        @Test
        public void equalsContract() {
            equalsContractSame(operation, new AtomicOperation.Remove(FIELD_NAME, item));
            equalsContractDifferent(operation, new AtomicOperation.Remove(FIELD_NAME, new Object()));
        }
    }

    public static class Replace {

        private Object oldValue;
        private Object newValue;
        private AtomicOperation.Replace operation;
        private State state;

        @Before
        public void before() {
            oldValue = new Object();
            newValue = new Object();
            operation = new AtomicOperation.Replace(FIELD_NAME, oldValue, newValue);
            state = mock(State.class);
        }

        @Test
        public void execute() {
            when(state.getByPath(FIELD_NAME)).thenReturn(oldValue);

            operation.execute(state);

            verify(state).putByPath(FIELD_NAME, newValue);
        }

        @Test
        public void executeError() {
            try {
                operation.execute(state);

            } catch (AtomicOperation.ReplacementException error) {
                assertThat(error.getState(), equalTo(state));
                assertThat(error.getField(), equalTo(FIELD_NAME));
                assertThat(error.getOldValue(), equalTo(oldValue));
                assertThat(error.getNewValue(), equalTo(newValue));
                return;
            }

            fail();
        }

        @Test
        public void equalsContract() {
            equalsContractSame(operation, new AtomicOperation.Replace(FIELD_NAME, oldValue, newValue));
            equalsContractDifferent(operation, new AtomicOperation.Replace(FIELD_NAME, oldValue, new Object()));
            equalsContractDifferent(operation, new AtomicOperation.Replace(FIELD_NAME, new Object(), newValue));
            equalsContractDifferent(operation, new AtomicOperation.Replace(FIELD_NAME, new Object(), new Object()));
        }
    }

    public static class Put {

        private Object value;
        private AtomicOperation.Put operation;
        private State state;

        @Before
        public void before() {
            value = new Object();
            operation = new AtomicOperation.Put(FIELD_NAME, value);
            state = mock(State.class);
        }

        @Test
        public void execute() {
            operation.execute(state);

            verify(state).putByPath(FIELD_NAME, value);
        }

        @Test
        public void equalsContract() {
            equalsContractSame(operation, new AtomicOperation.Put(FIELD_NAME, value));
            equalsContractDifferent(operation, new AtomicOperation.Put(FIELD_NAME, new Object()));
        }
    }
}
