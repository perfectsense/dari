package com.psddev.dari.db;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.InOrder;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class StateTest {

    public static class GetSet {

        private State state;

        @Before
        public void before() {
            state = new State();
        }

        @Test
        public void getId() {
            assertThat(state.getId(), notNullValue());
        }

        @Test
        public void setId() {
            UUID id = UUID.randomUUID();

            state.setId(id);

            assertThat(state.getId(), equalTo(id));
        }

        @Test
        public void isNew() {
            assertThat(state.getStatus(), nullValue());
            assertThat(state.isNew(), equalTo(true));
        }

        @Test
        public void setStatusDeleted() {
            state.setStatus(StateStatus.DELETED);

            assertThat(state.getStatus(), equalTo(StateStatus.DELETED));
            assertThat(state.isDeleted(), equalTo(true));
            assertThat(state.getAtomicOperations(), empty());
            assertThat(state.getErrorFields(), empty());
        }

        @Test
        public void setStatusReferenceOnly() {
            state.setStatus(StateStatus.REFERENCE_ONLY);

            assertThat(state.getStatus(), equalTo(StateStatus.REFERENCE_ONLY));
            assertThat(state.isReferenceOnly(), equalTo(true));
            assertThat(state.getAtomicOperations(), empty());
            assertThat(state.getErrorFields(), empty());
        }

        @Test
        public void setStatusSaved() {
            state.setStatus(StateStatus.SAVED);

            assertThat(state.getStatus(), equalTo(StateStatus.SAVED));
            assertThat(state.getAtomicOperations(), empty());
            assertThat(state.getErrorFields(), empty());
        }

        @Test
        public void resolveToReferenceOnly() {
            assertThat(state.isResolveToReferenceOnly(), equalTo(false));

            state.setResolveToReferenceOnly(true);

            assertThat(state.isResolveToReferenceOnly(), equalTo(true));

            state.setResolveToReferenceOnly(false);

            assertThat(state.isResolveToReferenceOnly(), equalTo(false));
        }
    }

    public static class MapImplementation {

        private State state;

        @Before
        public void before() {
            state = new State();
        }

        @Test
        public void removeNonString() {
            assertThat(state.remove(new Object()), nullValue());
        }
    }

    public static class DatabaseInteraction {

        private DatabaseEnvironment environment;
        private Database database;
        private State state;

        @Before
        public void before() {
            environment = mock(DatabaseEnvironment.class);
            database = mock(Database.class);

            when(database.getEnvironment()).thenReturn(environment);

            state = new State();

            state.setDatabase(database);
        }

        @Test
        public void beginWrites() {
            state.beginWrites();

            verify(database).beginWrites();
        }

        @Test
        public void commitWrites() {
            state.commitWrites();

            verify(database).commitWrites();
        }

        @Test
        public void endWrites() {
            state.endWrites();

            verify(database).endWrites();
        }

        @Test
        public void save() {
            state.save();

            verify(database).save(state);
        }

        @Test
        public void saveImmediately() {
            state.saveImmediately();

            InOrder inOrder = inOrder(database);

            inOrder.verify(database).beginIsolatedWrites();
            inOrder.verify(database).save(state);
            inOrder.verify(database).commitWrites();
            inOrder.verify(database).endWrites();
        }

        @Test
        public void saveImmediatelyError() {
            doThrow(DatabaseException.class).when(database).save(state);

            try {
                state.saveImmediately();

            } catch (DatabaseException error) {
                // Expected.
            }

            InOrder inOrder = inOrder(database);

            inOrder.verify(database).beginIsolatedWrites();
            inOrder.verify(database).save(state);
            inOrder.verify(database, never()).commitWrites();
            inOrder.verify(database).endWrites();
        }

        @Test
        public void saveEventually() {
            state.saveEventually();

            InOrder inOrder = inOrder(database);

            inOrder.verify(database).beginWrites();
            inOrder.verify(database).save(state);
            inOrder.verify(database).commitWritesEventually();
            inOrder.verify(database).endWrites();
        }

        @Test
        public void saveEventuallyError() {
            doThrow(DatabaseException.class).when(database).save(state);

            try {
                state.saveEventually();

            } catch (DatabaseException error) {
                // Expected.
            }

            InOrder inOrder = inOrder(database);

            inOrder.verify(database).beginWrites();
            inOrder.verify(database).save(state);
            inOrder.verify(database, never()).commitWritesEventually();
            inOrder.verify(database).endWrites();
        }

        @Test
        public void saveUnsafely() {
            state.saveUnsafely();

            verify(database).saveUnsafely(state);
        }

        @Test
        public void index() {
            state.index();

            verify(database).index(state);
        }

        @Test
        public void delete() {
            state.delete();

            verify(database).delete(state);
        }

        @Test
        public void deleteImmediately() {
            state.deleteImmediately();

            InOrder inOrder = inOrder(database);

            inOrder.verify(database).beginIsolatedWrites();
            inOrder.verify(database).delete(state);
            inOrder.verify(database).commitWrites();
            inOrder.verify(database).endWrites();
        }
    }
}
