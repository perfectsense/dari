package com.psddev.dari.db;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;

/**
 * Skeletal database implementation that forwards all calls
 * to another database.
 */
public abstract class ForwardingDatabase implements Database {

    public static final String DELEGATE_SETTING = "delegate";

    private Database delegate;

    /** Returns the database where all calls are forwarded to. */
    public Database getDelegate() {
        return delegate;
    }

    /** Sets the database where all calls are forwarded to. */
    public void setDelegate(Database delegate) {
        this.delegate = delegate;
    }

    // --- Database support ---

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setDelegate(Database.Static.getInstance(ObjectUtils.to(String.class, settings.get(DELEGATE_SETTING))));
    }

    @Override
    public String getName() {
        return getDelegate().getName();
    }

    @Override
    public void setName(String name) {
        getDelegate().setName(name);
    }

    @Override
    public DatabaseEnvironment getEnvironment() {
        return getDelegate().getEnvironment();
    }

    @Override
    public void setEnvironment(DatabaseEnvironment environment) {
        getDelegate().setEnvironment(environment);
    }

    protected <T> Query<T> filterQuery(Query<T> query) {
        return query;
    }

    @Override
    public <T> List<T> readAll(Query<T> query) {
        return getDelegate().readAll(filterQuery(query));
    }

    @Override
    public <T> List<Grouping<T>> readAllGrouped(Query<T> query, String... fields) {
        return getDelegate().readAllGrouped(filterQuery(query), fields);
    }

    @Override
    public long readCount(Query<?> query) {
        return getDelegate().readCount(filterQuery(query));
    };

    @Override
    public <T> T readFirst(Query<T> query) {
        return getDelegate().readFirst(filterQuery(query));
    }

    @Override
    public <T> Iterable<T> readIterable(Query<T> query, int fetchSize) {
        return getDelegate().readIterable(filterQuery(query), fetchSize);
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        return getDelegate().readLastUpdate(filterQuery(query));
    }

    @Override
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        return getDelegate().readPartial(filterQuery(query), offset, limit);
    }

    @Override
    public <T> PaginatedResult<Grouping<T>> readPartialGrouped(Query<T> query, long offset, int limit, String... fields) {
        return getDelegate().readPartialGrouped(filterQuery(query), offset, limit, fields);
    }

    @Override
    public boolean beginWrites() {
        return getDelegate().beginWrites();
    }

    @Override
    public void beginIsolatedWrites() {
        getDelegate().beginIsolatedWrites();
    }

    @Override
    public boolean commitWrites() {
        return getDelegate().commitWrites();
    }

    @Override
    public boolean commitWritesEventually() {
        return getDelegate().commitWritesEventually();
    }

    @Override
    public boolean endWrites() {
        return getDelegate().endWrites();
    }

    protected State filterState(State state) {
        return state;
    }

    @Override
    public void save(State state) {
        getDelegate().save(filterState(state));
    }

    @Override
    public void saveUnsafely(State state) {
        getDelegate().saveUnsafely(filterState(state));
    }

    @Override
    public void index(State state) {
        getDelegate().index(filterState(state));
    }

    @Override
    public void indexAll(ObjectIndex index) {
        getDelegate().indexAll(index);
    }

    @Override
    public void recalculate(State state, ObjectIndex... indexes) {
        getDelegate().recalculate(state, indexes);
    }

    @Override
    public void delete(State state) {
        getDelegate().delete(filterState(state));
    }

    @Override
    public void deleteByQuery(Query<?> query) {
        getDelegate().deleteByQuery(filterQuery(query));
    }

    @Override
    public long now() {
        return getDelegate().now();
    }

    @Override
    public void addUpdateNotifier(UpdateNotifier<?> notifier) {
        getDelegate().addUpdateNotifier(notifier);
    }

    @Override
    public void removeUpdateNotifier(UpdateNotifier<?> notifier) {
        getDelegate().removeUpdateNotifier(notifier);
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(super.toString());
        s.append("(delegate=");
        s.append(getDelegate().toString());
        s.append(')');
        return s.toString();
    }
}
