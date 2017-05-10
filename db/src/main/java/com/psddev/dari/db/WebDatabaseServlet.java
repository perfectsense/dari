package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.DebugServlet;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.WebPageContext;

/**
 * Servlet that provides the APIs for a {@linkplain WebDatabase
 * web database}.
 */
@SuppressWarnings("serial")
public class WebDatabaseServlet extends DebugServlet {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-web");
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        WebPageContext context = new WebPageContext(this, request, response);
        Database database = Database.Static.getInstance(context.param(String.class, WebDatabase.DATABASE_PARAMETER));

        Map<String, Object> map = new CompactMap<String, Object>();
        map.put(WebDatabase.STATUS_KEY, WebDatabase.OK_STATUS);

        String action = context.param(String.class, WebDatabase.ACTION_PARAMETER);

        if (WebDatabase.READ_ALL_ACTION.equals(action)) {
            map.put(WebDatabase.RESULT_KEY, readAll(database, context));

        } else if (WebDatabase.READ_COUNT_ACTION.equals(action)) {
            map.put(WebDatabase.RESULT_KEY, readCount(database, context));

        } else if (WebDatabase.READ_FIRST_ACTION.equals(action)) {
            map.put(WebDatabase.RESULT_KEY, readFirst(database, context));

        } else if (WebDatabase.READ_LAST_UPDATE_ACTION.equals(action)) {
            map.put(WebDatabase.RESULT_KEY, readLastUpdate(database, context));

        } else if (WebDatabase.READ_PARTIAL_ACTION.equals(action)) {
            map.put(WebDatabase.RESULT_KEY, readPartial(database, context));

        } else if (WebDatabase.WRITE_ACTION.equals(action)) {
            // write(database, context);

        } else {
            map.put(WebDatabase.STATUS_KEY, WebDatabase.ERROR_STATUS);
            map.put(WebDatabase.RESULT_KEY, String.format("Invalid action! (%s)", action));
        }

        response.setContentType("text/javascript");
        context.write(ObjectUtils.toJson(map, true));
    }

    private Query<?> createQuery(Database database, WebPageContext context) {
        String queryString = context.param(String.class, WebDatabase.QUERY_PARAMETER);
        Object queryObject = ObjectUtils.fromJson(queryString);
        if (!(queryObject instanceof Map)) {
            throw new DatabaseException(database, String.format(
                    "Invalid query string! (%s)", queryString));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> queryMap = (Map<String, Object>) queryObject;
        convertTypeNameToId(database.getEnvironment(), queryMap);
        Query<?> query = Query.fromAll();
        query.getState().putAll(queryMap);
        query.using(database);
        return query;
    }

    @SuppressWarnings("unchecked")
    private void convertTypeNameToId(DatabaseEnvironment environment, Map<?, Object> map) {
        for (Map.Entry<?, Object> entry : map.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();

            if (StateSerializer.TYPE_KEY.equals(key)) {
                if (value != null) {
                    ObjectType type = environment.getTypeByName(value.toString());
                    if (type != null) {
                        entry.setValue(type.getId().toString());
                    }
                }

            } else if (value instanceof Map) {
                convertTypeNameToId(environment, (Map<?, Object>) value);

            } else if (value instanceof List) {
                for (Object item : (List<?>) value) {
                    if (item instanceof Map) {
                        convertTypeNameToId(environment, (Map<?, Object>) item);
                    }
                }
            }
        }
    }

    private List<Map<String, Object>> readAll(Database database, WebPageContext context) {
        List<Map<String, Object>> itemMaps = new ArrayList<Map<String, Object>>();
        for (Object item : createQuery(database, context).selectAll()) {
            itemMaps.add(State.getInstance(item).getSimpleValues());
        }
        return itemMaps;
    }

    private long readCount(Database database, WebPageContext context) {
        return createQuery(database, context).count();
    }

    private Map<String, Object> readFirst(Database database, WebPageContext context) {
        State state = State.getInstance(createQuery(database, context).first());
        return state != null ? state.getSimpleValues() : null;
    }

    private Date readLastUpdate(Database database, WebPageContext context) {
        return createQuery(database, context).lastUpdate();
    }

    private PaginatedResult<Map<String, Object>> readPartial(Database database, WebPageContext context) {
        long offset = context.param(long.class, WebDatabase.OFFSET_PARAMETER);
        int limit = context.paramOrDefault(int.class, WebDatabase.LIMIT_PARAMETER, 10);
        PaginatedResult<?> result = createQuery(database, context).select(offset, limit);
        List<Map<String, Object>> itemMaps = new ArrayList<Map<String, Object>>();

        for (Object item : result.getItems()) {
            itemMaps.add(State.getInstance(item).getSimpleValues());
        }

        return new PaginatedResult<Map<String, Object>>(
                result.getOffset(),
                result.getLimit(),
                result.getCount(),
                itemMaps);
    }

    private State createState(Database database, String stateString) {
        Object stateObject = ObjectUtils.fromJson(stateString);
        if (!(stateObject instanceof Map)) {
            throw new DatabaseException(database, String.format(
                    "Invalid state string! (%s)", stateString));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> stateMap = (Map<String, Object>) stateObject;
        UUID typeId = ObjectUtils.to(UUID.class, stateMap.get(StateSerializer.TYPE_KEY));
        UUID id = ObjectUtils.to(UUID.class, stateMap.get(StateSerializer.ID_KEY));
        Object object = database.getEnvironment().createObject(typeId, id);
        State state = State.getInstance(object);
        state.putAll(stateMap);
        return state;
    }

    private void write(Database database, WebPageContext context) {
        database.beginWrites();

        try {
            for (String stateString : context.params(String.class, WebDatabase.SAVES_PARAMETER)) {
                database.save(createState(database, stateString));
            }
            for (String stateString : context.params(String.class, WebDatabase.INDEXES_PARAMETER)) {
                database.index(createState(database, stateString));
            }
            for (String stateString : context.params(String.class, WebDatabase.DELETES_PARAMETER)) {
                database.delete(createState(database, stateString));
            }

            database.commitWrites();

        } finally {
            database.endWrites();
        }
    }
}
