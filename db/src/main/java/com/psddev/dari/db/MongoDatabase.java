package com.psddev.dari.db;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.mongodb.BasicDBObject;
import com.mongodb.DBAddress;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;

// CHECKSTYLE:OFF
/**
 * Database backed by <a href="http://www.mongodb.org/">MongoDB</a>.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class MongoDatabase extends AbstractDatabase<DBCollection> {

    public static final String ADDRESS_SETTING = "address";
    public static final String DATABASE_SETTING = "database";
    public static final String COLLECTION_SETTING = "collection";

    public static final String DEFAULT_COLLECTION = "dari";

    public static final String ID_KEY = "_id";
    public static final String TYPE_ID_KEY = "_typeId";

    private String address;
    private String database;
    private String collection;
    private transient DBCollection mongoCollection;

    /** Returns the address. */
    public String getAddress() {
        return address;
    }

    /** Sets the address. */
    public synchronized void setAddress(String address) {
        this.address = address;
        mongoCollection = null;
    }

    /** Returns the database. */
    public String getDatabase() {
        return database;
    }

    /** Sets the database. */
    public synchronized void setDatabase(String database) {
        this.database = database;
        mongoCollection = null;
    }

    /** Returns the collection. */
    public String getCollection() {
        return collection;
    }

    /** Sets the collection. */
    public synchronized void setCollection(String collection) {
        this.collection = collection;
        mongoCollection = null;
    }

    /** Builds MongoDB query based on the given {@code query}. */
    public DBObject buildMongoQuery(Query<?> query) {
        Predicate predicate = query.getPredicate();
        List<DBObject> mongoChildren = new ArrayList<DBObject>();

        if (predicate != null) {
            mongoChildren.add(buildMongoPredicate(query, predicate));
        }

        if (!query.isFromAll()) {
            Set<ObjectType> types = getEnvironment().getTypesByGroup(query.getGroup());
            List<String> typeIds = new ArrayList<String>();
            for (ObjectType type : types) {
                typeIds.add(type.getId().toString());
            }
            mongoChildren.add(new BasicDBObject(
                    TYPE_ID_KEY, new BasicDBObject("$in", typeIds)));
        }

        return new BasicDBObject("$and", mongoChildren);
    }

    private DBObject buildMongoPredicate(Query<?> query, Predicate predicate) {
        Class<?> predicateClass = predicate.getClass();

        if (CompoundPredicate.class.isAssignableFrom(predicateClass)) {
            CompoundPredicate compoundPredicate = (CompoundPredicate) predicate;
            String operator = compoundPredicate.getOperator();
            String mongoOperator =
                    PredicateParser.AND_OPERATOR.equals(operator) ? "$and" :
                    PredicateParser.OR_OPERATOR.equals(operator) ? "$or" :
                    PredicateParser.NOT_OPERATOR.equals(operator) ? "$nor" :
                    null;

            if (mongoOperator != null) {
                List<DBObject> mongoChildren = new ArrayList<DBObject>();
                for (Predicate child : compoundPredicate.getChildren()) {
                    mongoChildren.add(buildMongoPredicate(query, child));
                }
                return new BasicDBObject(mongoOperator, mongoChildren);
            }

        } else if (ComparisonPredicate.class.isAssignableFrom(predicateClass)) {
            ComparisonPredicate comparisonPredicate = (ComparisonPredicate) predicate;
            String operator = comparisonPredicate.getOperator();
            String indexKey = query.mapEmbeddedKey(getEnvironment(), comparisonPredicate.getKey()).getIndexKey(null);

            if (PredicateParser.EQUALS_ANY_OPERATOR.equals(operator)) {
                return new BasicDBObject(
                        indexKey, new BasicDBObject("$in", comparisonPredicate.resolveValues(this)));

            } else {
                String mongoOperator =
                        PredicateParser.LESS_THAN_OPERATOR.equals(operator) ? "$lt" :
                        PredicateParser.GREATER_THAN_OPERATOR.equals(operator) ? "$gt" :
                        null;

                if (mongoOperator != null) {
                    List<DBObject> mongoChildren = new ArrayList<DBObject>();
                    for (Object value : comparisonPredicate.resolveValues(this)) {
                        mongoChildren.add(new BasicDBObject(
                                indexKey, new BasicDBObject(mongoOperator, value)));
                    }
                    return new BasicDBObject("$or", mongoChildren);
                }
            }
        }

        throw new UnsupportedPredicateException(this, predicate);
    }

    /** Builds MongoDB query based on the given {@code query}. */
    public DBObject buildMongoSort(Query<?> query) {
        List<Sorter> sorters = query.getSorters();
        if (sorters.isEmpty()) {
            return new BasicDBObject();
        }

        BasicDBObject mongoSorter = new BasicDBObject();
        for (Sorter sorter : sorters) {
            String operator = sorter.getOperator();

            if (Sorter.ASCENDING_OPERATOR.equals(operator)) {
                String indexKey = query.mapEmbeddedKey(getEnvironment(), (String) sorter.getOptions().get(0)).getIndexKey(null);
                mongoSorter.append(indexKey, 1);

            } else if (Sorter.DESCENDING_OPERATOR.equals(operator)) {
                String indexKey = query.mapEmbeddedKey(getEnvironment(), (String) sorter.getOptions().get(0)).getIndexKey(null);
                mongoSorter.append(indexKey, -1);

            } else {
                throw new UnsupportedSorterException(this, sorter);
            }
        }

        return mongoSorter;
    }

    public DBCursor find(Query<?> query) {
        return openQueryConnection(query).
                find(buildMongoQuery(query)).
                sort(buildMongoSort(query));
    }

    // --- AbstractDatabase support ---

    @Override
    public DBCollection openConnection() {
        if (mongoCollection == null) {

            String database = getDatabase();
            if (ObjectUtils.isBlank(database)) {
                throw new IllegalStateException("Database can't blank!");
            }

            String collection = getCollection();
            if (ObjectUtils.isBlank(collection)) {
                collection = DEFAULT_COLLECTION;
            }

            Mongo mongo;
            try {
                mongo = ObjectUtils.isBlank(address) ?
                        new Mongo() :
                        new Mongo(new DBAddress(address));
            } catch (UnknownHostException ex) {
                throw new IllegalArgumentException(String.format(
                        "[%s] isn't a valid address!", address), ex);
            }

            mongoCollection = mongo.getDB(database).getCollection(collection);
        }

        return mongoCollection;
    }

    @Override
    public void closeConnection(DBCollection mongoCollection) {
    }

    @Override
    protected void doInitialize(String settingsKey, Map<String, Object> settings) {
        setAddress((String) settings.get(ADDRESS_SETTING));
        setDatabase((String) settings.get(DATABASE_SETTING));
        setCollection((String) settings.get(COLLECTION_SETTING));
    }

    private Object loadObject(Map<String, Object> map) {
        unescapeKeys(map);
        Object object = getEnvironment().createObject(
                ObjectUtils.to(UUID.class, map.remove(TYPE_ID_KEY)),
                ObjectUtils.to(UUID.class, map.remove(ID_KEY)));

        State state = State.getInstance(object);
        for (Iterator<Map.Entry<String, Object>> i = map.entrySet().iterator(); i.hasNext();) {
            Map.Entry<String, Object> e = i.next();
            String key = e.getKey();
            if (key.startsWith("_")) {
                i.remove();
                state.getExtras().put(key.substring(1), e.getValue());
            }
        }

        state.getValues().putAll(map);
        state.setStatus(StateStatus.SAVED);
        return object;
    }

    private void unescapeKeys(Object object) {
        if (object instanceof List) {
            for (Object item : (List<?>) object) {
                unescapeKeys(item);
            }
        } else if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            Map<String, Object> newMap = new CompactMap<String, Object>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                String key = e.getKey().replace(",", ".");
                Object value = e.getValue();
                unescapeKeys(value);
                newMap.put(key, value);
            }
            map.clear();
            map.putAll(newMap);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> List<T> readAll(Query<T> query) {
        List<T> list = new ArrayList<T>();
        for (DBObject item : find(query)) {
            list.add((T) loadObject(item.toMap()));
        }
        return list;
    }

    @Override
    public long readCount(Query<?> query) {
        return find(query).count();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T readFirst(Query<T> query) {
        for (DBObject item : find(query).limit(1)) {
            return (T) loadObject(item.toMap());
        }
        return null;
    }

    @Override
    public Date readLastUpdate(Query<?> query) {
        throw new UnsupportedOperationException();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> PaginatedResult<T> readPartial(Query<T> query, long offset, int limit) {
        List<T> list = new ArrayList<T>();
        DBCursor cursor = find(query).skip((int) offset).limit(limit);
        for (DBObject item : cursor) {
            list.add((T) loadObject(item.toMap()));
        }
        return new PaginatedResult<T>(offset, limit, cursor.count(), list);
    }

    @Override
    protected void doSaves(DBCollection mongoCollection, boolean isImmediate, List<State> states) {
        List<DBObject> documents = new ArrayList<DBObject>();
        for (State state : states) {
            Map<String, Object> values = state.getSimpleValues();
            values.put(ID_KEY, state.getId().toString());
            values.put(TYPE_ID_KEY, state.getVisibilityAwareTypeId().toString());
            escapeKeys(values);
            documents.add(new BasicDBObject(values));
        }

        mongoCollection.insert(documents);
    }

    private void escapeKeys(Object object) {
        if (object instanceof List) {
            for (Object item : (List<?>) object) {
                escapeKeys(item);
            }
        } else if (object instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) object;
            Map<String, Object> newMap = new CompactMap<String, Object>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                String key = e.getKey().replace(".", ",");
                Object value = e.getValue();
                escapeKeys(value);
                newMap.put(key, value);
            }
            map.clear();
            map.putAll(newMap);
        }
    }

    @Override
    protected void doDeletes(DBCollection mongoCollection, boolean isImmediate, List<State> states) {
        List<String> ids = new ArrayList<String>();
        for (State state : states) {
            ids.add(state.getId().toString());
        }

        mongoCollection.remove(new BasicDBObject(
                ID_KEY, new BasicDBObject("$in", ids)));
    }
}
