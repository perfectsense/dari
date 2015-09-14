package com.psddev.dari.db;

import com.google.common.collect.ImmutableMap;
import com.psddev.dari.util.CompactMap;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.internal.stubbing.defaultanswers.ReturnsMocks;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class AggregateDatabaseTest {

    public static class General {

        @Test
        public void implementsAll() throws NoSuchMethodException {
            for (Method method : Database.class.getMethods()) {
                if (Modifier.isStatic(method.getModifiers())) {
                    continue;
                }

                AggregateDatabase.class.getMethod(
                        method.getName(),
                        method.getParameterTypes());
            }
        }
    }

    // Test getters and setters.
    public static class GetSet {

        private AggregateDatabase database;

        @Before
        public void before() {
            database = new AggregateDatabase();
        }

        private <T> void test(
                Supplier<T> getter,
                Matcher<? super T> getterMatcher,
                Consumer<T> setter,
                Supplier<T> setterValue) {

            assertThat(getter.get(), getterMatcher);

            T value = setterValue.get();

            setter.accept(value);

            assertThat(getter.get(), equalTo(value));
        }

        @Test
        public void defaultDelegate() {
            test(
                    database::getDefaultDelegate,
                    nullValue(),
                    database::setDefaultDelegate,
                    () -> mock(Database.class));
        }

        @Test
        public void defaultReadDelegate() {
            test(
                    database::getDefaultReadDelegate,
                    nullValue(),
                    database::setDefaultReadDelegate,
                    () -> mock(Database.class));
        }

        @Test
        public void delegates() {
            test(
                    database::getDelegates,
                    equalTo(ImmutableMap.of()),
                    database::setDelegates,
                    () -> ImmutableMap.of("delegate", mock(Database.class)));
        }

        @Test
        public void readDelegates() {
            test(
                    database::getReadDelegates,
                    equalTo(ImmutableMap.of()),
                    database::setReadDelegates,
                    () -> ImmutableMap.of("delegate", mock(Database.class)));
        }

        @Test
        public void name() {
            test(
                    database::getName,
                    nullValue(),
                    database::setName,
                    () -> UUID.randomUUID().toString());
        }

        @Test
        public void environment() {
            test(
                    database::getEnvironment,
                    notNullValue(),
                    database::setEnvironment,
                    () -> mock(DatabaseEnvironment.class));
        }
    }

    // Test delegate helper methods.
    public static class Delegate {

        private Database defaultDelegate;
        private Database secondaryDelegate;
        private AggregateDatabase database;

        @Before
        public void before() {
            defaultDelegate = mock(DefaultDelegate.class);
            secondaryDelegate = mock(SecondaryDelegate.class);
            database = new AggregateDatabase();

            database.setDefaultDelegate(defaultDelegate);
            database.setDelegates(ImmutableMap.of(
                    "default", defaultDelegate,
                    "secondary", secondaryDelegate
            ));
        }

        @Test(expected = NullPointerException.class)
        public void getDelegatesByClassNull() {
            database.getDelegatesByClass(null);
        }

        @Test
        public void getDelegatesByClass() {
            assertThat(database.getDelegatesByClass(DefaultDelegate.class), contains(defaultDelegate));
            assertThat(database.getDelegatesByClass(MissingDelegate.class), empty());
        }

        @Test(expected = NullPointerException.class)
        public void getFirstDelegateByClassNull() {
            database.getFirstDelegateByClass(null);
        }

        @Test
        public void getFirstDelegateByClass() {
            assertThat(database.getFirstDelegateByClass(DefaultDelegate.class), equalTo(defaultDelegate));
            assertThat(database.getFirstDelegateByClass(MissingDelegate.class), nullValue());
        }

        @Test
        public void setDelegatesNull() {
            database.setDelegates(null);

            assertThat(database.getDelegates().size(), equalTo(0));
        }

        private interface DefaultDelegate extends Database {
        }

        private interface SecondaryDelegate extends Database {
        }

        private interface MissingDelegate extends Database {
        }
    }

    public static abstract class ReadOrWrite {

        protected Map<String, Database> goodDelegates;
        protected Map<String, Database> badDelegates;
        protected DatabaseEnvironment environment;
        protected AggregateDatabase database;

        @Before
        public void beforeReadOrWrite() {
            goodDelegates = new CompactMap<>();

            for (int i = 0; i < 2; ++ i) {
                goodDelegates.put("good" + i, mock(Database.class));
            }

            badDelegates = new CompactMap<>();

            for (int i = 0; i < 2; ++ i) {
                Database badDelegate = mock(Database.class, (Answer<Object>) invocation -> {
                    throw new UnsupportedOperationException("Bad delegate doesn't support any operation!");
                });

                doReturn("Bad delegate " + i).when(badDelegate).toString();
                badDelegates.put("bad" + i, badDelegate);
            }

            environment = mock(DatabaseEnvironment.class);

            when(environment.getTypesByGroup(any())).thenReturn(Collections.emptySet());

            database = new AggregateDatabase();

            database.setEnvironment(environment);
        }
    }

    // Test read methods.
    public static abstract class Read extends ReadOrWrite {

        protected Database goodDelegate;
        protected Query<Object> query;

        @Before
        @SuppressWarnings("unchecked")
        public void beforeRead() {
            goodDelegate = goodDelegates.values().stream().findFirst().get();

            Map<String, Database> readDelegates = new CompactMap<>();

            readDelegates.put("good", goodDelegate);
            readDelegates.putAll(badDelegates);

            database.setReadDelegates(readDelegates);

            query = (Query<Object>) mock(Query.class);
        }

        @SuppressWarnings("unchecked")
        private <R> void test(Function<Database, R> reader) {
            SavesAndReturnsMocks answer = new SavesAndReturnsMocks();

            when(reader.apply(goodDelegate)).then(answer);

            assertThat(reader.apply(database), equalTo(answer.getLastObject()));
        }

        @Test
        public void readAll() {
            test(db -> db.readAll(query));
        }

        @Test
        public void readAllGrouped() {
            test(db -> db.readAllGrouped(query));
        }

        @Test
        public void readCount() {
            test(db -> db.readCount(query));
        }

        @Test
        public void readFirst() {
            test(db -> db.readFirst(query));
        }

        @Test
        public void readIterable() {
            test(db -> db.readIterable(query, 0));
        }

        @Test
        public void readPartial() {
            test(db -> db.readPartial(query, 0L, 1));
        }

        @Test
        public void readPartialGrouped() {
            test(db -> db.readPartialGrouped(query, 0L, 1, "field"));
        }

        @Test
        public void readLastUpdate() {
            test(db -> db.readLastUpdate(query));
        }

        private static class SavesAndReturnsMocks extends ReturnsMocks {

            private Object lastObject;

            public Object getLastObject() {
                return lastObject;

            }

            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                lastObject = super.answer(invocation);

                return lastObject;
            }
        }
    }

    public static class ReadGood extends Read {

        @Before
        public void before() {
            database.setDefaultDelegate(goodDelegate);
        }
    }

    public static class ReadBad extends Read {

        @Before
        public void before() {
            database.setDefaultDelegate(badDelegates.values().stream().findFirst().get());
        }
    }

    // Test batch write methods.
    public static class Batch extends ReadOrWrite {

        @Before
        public void before() {
            database.setDefaultDelegate(goodDelegates.values().stream().findFirst().get());
            database.setDelegates(goodDelegates);
        }

        private void test(Consumer<Database> batcher) {
            batcher.accept(database);

            for (Database delegate : goodDelegates.values()) {
                batcher.accept(verify(delegate));
            }
        }

        @Test
        public void beginWrites() {
            test(Database::beginWrites);
        }

        @Test
        public void beginIsolatedWrites() {
            test(Database::beginIsolatedWrites);
        }

        @Test
        public void commitWrites() {
            test(Database::commitWrites);
        }

        @Test
        public void commitWritesEventually() {
            test(Database::commitWritesEventually);
        }

        @Test
        public void endWrites() {
            test(Database::endWrites);
        }
    }

    // Test write methods.
    public static class Write extends ReadOrWrite {

        private Database defaultDelegate;
        private Map<String, Database> delegates;
        private State state;

        @Before
        public void before() {
            defaultDelegate = goodDelegates.values().stream().findFirst().get();

            database.setDefaultDelegate(defaultDelegate);

            delegates = new CompactMap<>();

            delegates.putAll(goodDelegates);
            delegates.putAll(badDelegates);
            database.setDelegates(delegates);

            state = mock(State.class);

            when(state.getType()).thenReturn(mock(ObjectType.class));
        }

        @Test
        public void save() {
            database.save(state);

            verify(defaultDelegate).save(state);

            delegates.values().stream()
                    .filter(delegate -> !delegate.equals(defaultDelegate))
                    .forEach(delegate -> {
                        verify(delegate, never()).save(state);
                        verify(delegate).saveUnsafely(state);
                    });
        }

        private void testOne(Consumer<Database> writer) {
            writer.accept(database);

            writer.accept(verify(defaultDelegate));
            delegates.values().forEach(delegate -> writer.accept(verify(delegate)));
        }

        @Test
        public void saveUnsafely() {
            testOne(db -> db.saveUnsafely(state));
        }

        @Test
        public void index() {
            testOne(db -> db.index(state));
        }

        @Test
        public void recalculate() {
            testOne(db -> db.recalculate(state));
        }

        @Test
        public void delete() {
            testOne(db -> db.delete(state));
        }

        private void testAll(Consumer<Database> writer) {
            writer.accept(database);

            writer.accept(verify(defaultDelegate));
            delegates.values().forEach(delegate -> writer.accept(verify(delegate)));
        }

        @Test
        public void indexAll() {
            ObjectIndex index = mock(ObjectIndex.class);

            testAll(db -> db.indexAll(index));
        }

        @Test
        public void deleteByQuery() {
            Query<?> query = mock(Query.class);

            testAll(db -> db.deleteByQuery(query));
        }
    }

    // Test methods that forward the call to the default delegate.
    public static class Forward {

        private Database defaultDelegate;
        private AggregateDatabase database;

        @Before
        public void before() {
            defaultDelegate = mock(Database.class);
            database = new AggregateDatabase();

            database.setDefaultDelegate(defaultDelegate);
        }

        private void test(Consumer<Database> consumer) {
            consumer.accept(database);

            consumer.accept(verify(defaultDelegate));
        }

        @Test
        public void now() {
            test(Database::now);
        }

        @Test
        public void addUpdateNotifier() {
            UpdateNotifier<?> notifier = mock(UpdateNotifier.class);

            test(db -> db.addUpdateNotifier(notifier));
        }

        @Test
        public void removeUpdateNotifier() {
            UpdateNotifier<?> notifier = mock(UpdateNotifier.class);

            test(db -> db.removeUpdateNotifier(notifier));
        }
    }
}
