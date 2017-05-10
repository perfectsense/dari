package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.DebugServlet;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.UuidUtils;

@SuppressWarnings("serial")
public class QueryDebugServlet extends DebugServlet {

    @Override
    public String getName() {
        return "Database: Query";
    }

    @Override
    public List<String> getPaths() {
        return Arrays.asList("db-query", "query");
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("all")
        Page page = new Page(getServletContext(), request, response);

        page.render();
    }

    private static class Page extends DebugFilter.PageWriter {

        private static final Pattern ID_PATTERN = Pattern.compile("([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})");
        private static final Pattern DATE_FIELD_PATTERN = Pattern.compile("(?m)^(\\s*\".*\" : (\\d{11,}),)$");
        private static final double INITIAL_VISIBILITY_HUE = Math.random();

        private enum SortOrder {

            ASCENDING("Ascending"),
            DESCENDING("Descending");

            public final String displayName;

            private SortOrder(String displayName) {
                this.displayName = displayName;
            }
        }

        private enum SubAction {

            RAW("Raw"),
            EDIT_RAW("Edit Raw");

            public final String displayName;

            private SubAction(String displayName) {
                this.displayName = displayName;
            }
        }

        private final String databaseName;
        private Database database;
        private final ObjectType type;
        private final String where;
        private final String sortField;
        private final SortOrder sortOrder;
        private final String additionalFieldsString;
        private final String ignoreReadConnectionString;
        private final boolean ignoreReadConnection;
        private final long offset;
        private final int limit;
        private final String filter;
        private final boolean showVisible;
        private final Map<String, List<String>> visibilityFilters;
        private final Map<String, Double> visibilityColors;
        private final Query<Object> query;

        public Page(ServletContext context, HttpServletRequest request, HttpServletResponse response) throws IOException {
            super(context, request, response);

            databaseName = page.param(String.class, "db");
            database = Database.Static.getInstance(databaseName);
            type = database.getEnvironment().getTypeById(page.param(UUID.class, "from"));
            where = page.paramOrDefault(String.class, "where", "").trim();
            sortField = page.param(String.class, "sortField");
            sortOrder = page.paramOrDefault(SortOrder.class, "sortOrder", SortOrder.ASCENDING);
            additionalFieldsString = page.param(String.class, "additionalFields");
            ignoreReadConnectionString = page.param(String.class, "ignoreReadConnection");
            ignoreReadConnection = ignoreReadConnectionString == null ? true : Boolean.parseBoolean(ignoreReadConnectionString);
            visibilityFilters = new HashMap<String, List<String>>();
            boolean showVisible = false;

            for (String visibilityFilterString : page.params(String.class, "visibilityFilters")) {
                if ("VISIBLE".equals(visibilityFilterString)) {
                    showVisible = true;
                    continue;
                }

                String[] parts = visibilityFilterString.split("\\x7c");

                if (parts.length == 2) {
                    String uniqueIndexName = parts[0];
                    String indexValue = parts[1];
                    List<String> indexValues = visibilityFilters.get(uniqueIndexName);

                    if (indexValues == null) {
                        indexValues = new ArrayList<String>();
                        visibilityFilters.put(uniqueIndexName, indexValues);
                    }

                    indexValues.add(indexValue);
                }
            }

            if (visibilityFilters.isEmpty()) {
                showVisible = true;
            }

            this.showVisible = showVisible;
            visibilityColors = new HashMap<String, Double>();
            offset = page.param(long.class, "offset");
            limit = page.paramOrDefault(int.class, "limit", 50);
            filter = "Filter".equals(page.param(String.class, "action")) ? page.param(String.class, "filter") : null;

            UUID idFromWhere = ObjectUtils.to(UUID.class, where);

            if (idFromWhere != null) {
                query = Query.from(Object.class).where("_id = ?", idFromWhere);

            } else {
                if (visibilityFilters.isEmpty()) {
                    query = Query.fromType(type);

                } else {
                    query = Query.from(Object.class);
                }

                Double timeout = page.param(Double.class, "timeout");
                if (timeout != null) {
                    query.setTimeout(timeout);
                }

                if (!visibilityFilters.isEmpty()) {
                    query.where(getVisibilitiesPredicate());
                }

                if (!ObjectUtils.isBlank(where)) {
                    if (visibilityFilters.isEmpty()) {
                        query.where(where);

                    } else {
                        query.where(getFullyQualifiedPredicateForType(type, PredicateParser.Static.parse(where)));
                    }
                }

                if (!ObjectUtils.isBlank(sortField)) {
                    if (SortOrder.DESCENDING.equals(sortOrder)) {
                        query.sortDescending(sortField);
                    } else {
                        query.sortAscending(sortField);
                    }
                }
            }

            if (ObjectUtils.isBlank(additionalFieldsString)
                    && ObjectUtils.isBlank(filter)) {
                query.resolveToReferenceOnly();
            }

            if (ObjectUtils.isBlank(databaseName)) {
                query.setDatabase(null);
                database = query.getDatabase();
            }

            CachingDatabase caching = new CachingDatabase();
            caching.setDelegate(database);
            query.using(caching);
            query.resolveInvisible();
        }

        @SuppressWarnings("deprecation")
        public void render() throws IOException {
            try {
                Database.Static.setIgnoreReadConnection(ignoreReadConnection);
                String action = page.param(String.class, "action");

                if ("count".equals(action)) {
                    renderCount();

                } else if ("form".equals(action)) {
                    renderForm();

                } else if ("select".equals(action)) {
                    renderSelect();

                } else if ("visibilityFilters".equals(action)) {
                    renderVisibilityFilters();

                } else {
                    renderDefault();
                }

            } catch (RuntimeException ex) {
                writeObject(ex);

            } finally {
                Database.Static.setIgnoreReadConnection(false);
            }
        }

        private Predicate getFullyQualifiedPredicateForType(ObjectType type, Predicate predicate) {
            if (type == null) {
                return predicate;
            }

            if (predicate instanceof ComparisonPredicate) {
                ComparisonPredicate comparison = (ComparisonPredicate) predicate;
                String operator = comparison.getOperator();
                boolean isIgnoreCase = comparison.isIgnoreCase();
                List<Object> values = comparison.getValues();

                // Try to get the full qualified version of the key based on
                // the type.
                String key = comparison.getKey();
                int slashAt = key.indexOf('/');

                if (slashAt >= 0) {
                    String firstPart = key.substring(0, slashAt);

                    if (ObjectUtils.getClassByName(firstPart) == null) {
                        String lastPart = key.substring(slashAt);
                        ObjectIndex index = type.getIndex(firstPart);

                        if (index != null) {
                            key = index.getUniqueName() + lastPart;
                        }
                    }

                } else {
                    ObjectIndex index = type.getIndex(key);

                    if (index != null) {
                        key = index.getUniqueName();
                    }
                }

                return new ComparisonPredicate(operator, isIgnoreCase, key, values);

            } else if (predicate instanceof CompoundPredicate) {
                CompoundPredicate compound = (CompoundPredicate) predicate;
                List<Predicate> children = null;

                if (compound.getChildren() != null) {
                    children = new ArrayList<Predicate>();

                    for (Predicate child : compound.getChildren()) {
                        children.add(getFullyQualifiedPredicateForType(type, child));
                    }
                }

                return new CompoundPredicate(compound.getOperator(), children);

            } else {
                return predicate;
            }
        }

        private Predicate getVisibilitiesPredicate() {
            if (!visibilityFilters.isEmpty()) {
                Predicate visibilityIndexPredicate = null;

                for (Map.Entry<String, List<String>> entry : visibilityFilters.entrySet()) {
                    String indexUniqueName = entry.getKey();
                    List<String> indexValues = entry.getValue();
                    Iterator<String> indexValuesIterator = indexValues.iterator();

                    if (showVisible) {
                        Predicate predicate = PredicateParser.Static.parse(indexUniqueName + " is missing");

                        if (visibilityIndexPredicate == null) {
                            visibilityIndexPredicate = predicate;

                        } else {
                            visibilityIndexPredicate = CompoundPredicate.combine(
                                    PredicateParser.OR_OPERATOR,
                                    visibilityIndexPredicate,
                                    predicate);
                        }
                    }

                    while (indexValuesIterator.hasNext()) {
                        Predicate predicate = PredicateParser.Static.parse(indexUniqueName + " = ?", indexValuesIterator.next());

                        if (visibilityIndexPredicate == null) {
                            visibilityIndexPredicate = predicate;

                        } else {
                            visibilityIndexPredicate = CompoundPredicate.combine(
                                    PredicateParser.OR_OPERATOR,
                                    visibilityIndexPredicate,
                                    predicate);
                        }
                    }
                }

                Predicate typePredicate = null;

                if (type != null) {
                    Set<UUID> typeIds = new HashSet<UUID>();

                    if (showVisible) {
                        for (ObjectType concreteType : type.findConcreteTypes()) {
                            typeIds.add(concreteType.getId());
                        }
                    }

                    for (Map.Entry<String, List<String>> entry : visibilityFilters.entrySet()) {
                        String indexUniqueName = entry.getKey();
                        List<String> indexValues = entry.getValue();
                        String indexName;
                        int slashAt = indexUniqueName.indexOf('/');

                        if (slashAt >= 0) {
                            indexName = indexUniqueName.substring(slashAt + 1);

                        } else {
                            indexName = indexUniqueName;
                        }

                        typeIds.addAll(getVisbilityTypeIdsForValuesAndTypes(indexName, new HashSet<String>(indexValues), type.findConcreteTypes()));
                    }

                    typePredicate = PredicateParser.Static.parse("_type = ?", typeIds);
                }

                if (typePredicate == null) {
                    return visibilityIndexPredicate;

                } else {
                    return CompoundPredicate.combine(PredicateParser.AND_OPERATOR, typePredicate, visibilityIndexPredicate);
                }

            } else {
                return PredicateParser.Static.parse("true");
            }
        }

        private Set<UUID> getVisbilityTypeIdsForValuesAndTypes(String indexName, Set<String> indexValues, Set<ObjectType> types) {
            Set<UUID> visibilityTypeIds = new HashSet<UUID>();

            for (String indexValue : indexValues) {
                for (ObjectType type : types) {
                    byte[] md5 = StringUtils.md5(indexName + "/" + indexValue);
                    byte[] typeId = UuidUtils.toBytes(type.getId());

                    for (int i = 0, length = typeId.length; i < length; ++ i) {
                        typeId[i] ^= md5[i];
                    }

                    visibilityTypeIds.add(UuidUtils.fromBytes(typeId));
                }
            }

            return visibilityTypeIds;
        }

        private ObjectField getVisibilityAwareField(State state) {
            @SuppressWarnings("unchecked")
            List<String> visibilities = (List<String>) state.get("dari.visibilities");

            if (visibilities != null && !visibilities.isEmpty()) {
                String fieldName = visibilities.get(visibilities.size() - 1);
                ObjectField field = state.getField(fieldName);

                return field;
            }

            return null;
        }

        private void renderVisibilityFilters() throws IOException {
            if (page.param(boolean.class, "showVisibilityFiltersLink")) {
                writeStart("a",
                        "target", "visibilityFilters",
                        "href", page.url("", "action", "visibilityFilters", "showVisibilityFiltersLink", null));
                    writeHtml("Show Visibility Filters");
                writeEnd();
                return;
            }

            Map<ObjectIndex, List<Object>> visibilityIndexValues = new HashMap<ObjectIndex, List<Object>>();
            boolean hasVisibilityValues = false;

            for (ObjectIndex index : database.getEnvironment().getIndexes()) {

                if (index.isVisibility()) {
                    visibilityIndexValues.put(index, new ArrayList<Object>());
                }
            }

            if (type != null) {
                for (ObjectIndex index : type.getIndexes()) {

                    if (index.isVisibility()) {
                        visibilityIndexValues.put(index, new ArrayList<Object>());
                    }
                }
            }

            for (Map.Entry<ObjectIndex, List<Object>> entry : visibilityIndexValues.entrySet()) {
                ObjectIndex index = entry.getKey();
                String uniqueName = index.getUniqueName();

                for (Grouping<Object> grouping : Query.from(Object.class).where(uniqueName + " != missing").groupBy(uniqueName)) {
                    Object key0 = grouping.getKeys().get(0);

                    if (key0 instanceof byte[]) {
                        key0 = new String((byte[]) key0);
                    }

                    entry.getValue().add(key0);
                    hasVisibilityValues = true;
                }
            }

            if (!hasVisibilityValues) {
                return;
            }

            writeStart("div", "class", "visibilityFilters");
                writeStart("ul", "class", "unstyled");
                    writeStart("li");
                        writeElement("input",
                                "class", "visibility-input",
                                "type", "checkbox",
                                "name", "visibilityFilters",
                                "value", "VISIBLE",
                                "checked", showVisible ? "checked" : null);
                        writeStart("span",
                                "class", "label visibility-label" + (showVisible ? "" : " disabled"),
                                "title", "Show visible objects",
                                "style", "background-color: #000;");
                            writeObject("VISIBLE");
                        writeEnd();
                    writeEnd();

                    double goldenRatio = 0.618033988749895;
                    double hue = INITIAL_VISIBILITY_HUE;

                    for (Map.Entry<ObjectIndex, List<Object>> entry : visibilityIndexValues.entrySet()) {
                        ObjectIndex index = entry.getKey();
                        List<Object> visibilityValues = entry.getValue();
                        ObjectField field = null;
                        ObjectType declaringType = null;
                        Class<?> declaringClass = ObjectUtils.getClassByName(index.getJavaDeclaringClassName());

                        if (declaringClass != null) {
                            declaringType = ObjectType.getInstance(declaringClass);

                            if (declaringType != null) {
                                field = declaringType.getField(index.getName());
                            }
                        }

                        for (Object visibilityValue : visibilityValues) {
                            hue += goldenRatio;
                            hue %= 1.0;

                            visibilityColors.put(index.getUniqueName() + "/" + visibilityValue, hue);

                            boolean visibilityValueChecked = false;

                            if (visibilityFilters != null) {
                                List<String> selectedVisibilityValues = visibilityFilters.get(index.getUniqueName());

                                if (selectedVisibilityValues != null) {
                                    visibilityValueChecked = selectedVisibilityValues.contains(visibilityValue);
                                }
                            }

                            String visibilityLabel = visibilityValue.toString();

                            if (field != null) {
                                State state = new State();
                                state.put(index.getName(), visibilityValue);
                                Object declaringObject = state.as(declaringClass);

                                if (declaringObject instanceof VisibilityLabel) {
                                    visibilityLabel = ((VisibilityLabel) declaringObject).createVisibilityLabel(field);
                                }
                            }

                            writeStart("li");
                                writeElement("input",
                                        "class", "visibility-input",
                                        "type", "checkbox",
                                        "name", "visibilityFilters",
                                        "value", index.getUniqueName() + "|" + visibilityValue,
                                        "checked", visibilityValueChecked ? "checked" : null);
                                writeStart("span",
                                        "class", "label visibility-label" + (visibilityValueChecked ? "" : " disabled"),
                                        "title", index.getUniqueName(),
                                        "style", "background: hsl(" + (hue * 360) + ",50%,50%);");
                                    writeObject(visibilityLabel);
                                writeEnd();
                            writeEnd();
                        }
                    }
                writeEnd();
            writeEnd();
        }

        private void renderCount() throws IOException {
            try {
                if (query.getTimeout() == null) {
                    query.setTimeout(1.0);
                }
                writeObject(query.count());

            } catch (RuntimeException ex) {
                writeHtml("Many (");
                writeStart("a", "href", page.url("", "timeout", 0));
                    writeHtml("Force Count");
                writeEnd();
                writeHtml(")");
            }
        }

        @SuppressWarnings("unchecked")
        private void renderForm() throws IOException {
            State state = State.getInstance(Query.fromAll()
                    .where("_id = ?", page.param(UUID.class, "id"))
                    .using(database)
                    .resolveInvisible()
                    .first());

            if (state == null) {
                writeStart("p", "class", "alert").writeHtml("No object!").writeEnd();

            } else {
                ObjectType type = state.getType();

                writeStart("div", "class", "edit", "style", "padding: 10px;");
                    writeStart("h2");
                        writeHtml(type != null ? type.getLabel() : "Unknown Type");
                        writeHtml(": ");
                        writeHtml(state.getLabel());
                    writeEnd();

                    SubAction subAction = page.paramOrDefault(SubAction.class, "subAction", SubAction.RAW);

                    writeStart("ul", "class", "nav nav-tabs");
                        for (SubAction a : SubAction.values()) {
                            writeStart("li", "class", a.equals(subAction) ? "active" : null);
                                writeStart("a", "href", page.url("", "subAction", a));
                                    writeHtml(a.displayName);
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();

                    if (SubAction.EDIT_RAW.equals(subAction)) {
                        if (page.isFormPost()) {
                            try {
                                state.setValues((Map<String, Object>) ObjectUtils.fromJson(page.param(String.class, "data")));
                                state.save();
                                writeStart("p", "class", "alert alert-success").writeHtml("Saved successfully at " + new Date() + "!").writeEnd();
                            } catch (RuntimeException error) {
                                writeStart("div", "class", "alert alert-error").writeObject(error).writeEnd();
                            }
                        }

                        writeStart("form", "method", "post", "action", page.url(""));
                            writeStart("div", "class", "json");
                                writeStart("textarea", "name", "data", "style", "box-sizing: border-box; height: 40em; width: 100%;");
                                    writeHtml(ObjectUtils.toJson(state.getSimpleValues(), true));
                                writeEnd();
                            writeEnd();
                            writeStart("div", "class", "form-actions");
                                writeElement("input", "class", "btn btn-success", "type", "submit", "value", "Save");
                            writeEnd();
                        writeEnd();

                    } else {
                        writeStart("pre");
                            String json = ObjectUtils.toJson(state.getSimpleValues(), true);
                            Matcher dateFieldMatcher = DATE_FIELD_PATTERN.matcher(json);
                            StringBuilder newJson = new StringBuilder();
                            int end = 0;

                            for (; dateFieldMatcher.find(); end = dateFieldMatcher.end()) {
                                String dateString = dateFieldMatcher.group(2);

                                newJson.append(json.substring(end, dateFieldMatcher.start()));
                                newJson.append(dateFieldMatcher.group(1));
                                newJson.append(" /* ");
                                newJson.append(ObjectUtils.to(Date.class, dateString));
                                newJson.append(" */");
                            }

                            newJson.append(json.substring(end));

                            Matcher idMatcher = ID_PATTERN.matcher(page.h(newJson.toString()));
                            write(idMatcher.replaceAll("<a href=\"" + JspUtils.getAbsolutePath(page.getRequest(), "/_debug/query") + "?where=id+%3D+$1\" target=\"_blank\">$1</a>"));
                        writeEnd();
                    writeEnd();
                }
            }
        }

        private void renderSelect() throws IOException {
            writeStart("div", "style", "padding: 10px;");
                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    writeStart("h2").writeHtml("Query").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span6");
                            writeStart("select", "class", "span6", "name", "from");
                                writeStart("option", "value", "").writeHtml("ALL TYPES").writeEnd();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        writeStart("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            writeHtml(t.getLabel());
                                            writeHtml(" (");
                                            writeHtml(t.getInternalName());
                                            writeHtml(")");
                                        writeEnd();
                                    }
                                }
                            writeEnd();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            writeStart("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            writeEnd();

                            writeStart("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                writeHtml(where);
                            writeEnd();

                            writeElement("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        writeEnd();

                        writeStart("div", "class", "span6");
                            writeStart("select", "name", "db", "style", "margin-bottom: 4px;");
                                writeStart("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    writeHtml("Default");
                                writeEnd();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    writeStart("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        writeHtml(dbName);
                                    writeEnd();
                                }
                            writeEnd();

                            writeElement("br");
                            writeElement("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            writeHtml(' ');
                            writeStart("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    writeStart("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        writeHtml(so.displayName);
                                    writeEnd();
                                }
                            writeEnd();
                            writeHtml(' ');
                            writeElement("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            writeElement("br");
                            writeElement("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);
                        writeEnd();
                    writeEnd();
                writeEnd();

                try {
                    PaginatedResult<Object> result = query.select(offset, limit);
                    List<Object> items = result.getItems();

                    if (offset == 0 && items.isEmpty()) {
                        writeStart("p", "class", "alert").writeHtml("No matches!").writeEnd();

                    } else {
                        writeStart("h2");
                            writeHtml("Result ");
                            writeObject(result.getFirstItemIndex());
                            writeHtml(" to ");
                            writeObject(result.getLastItemIndex());
                            writeHtml(" of ");
                            writeStart("span", "class", "frame");
                                writeStart("a", "href", page.url("", "action", "count")).writeHtml("?").writeEnd();
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "btn-group");
                            writeStart("a",
                                    "class", "btn" + (offset > 0 ? "" : " disabled"),
                                    "href", page.url("", "offset", 0));
                                writeStart("i", "class", "icon-fast-backward").writeEnd();
                                writeHtml(" First");
                            writeEnd();
                            writeStart("a",
                                    "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getPreviousOffset()));
                                writeStart("i", "class", "icon-step-backward").writeEnd();
                                writeHtml(" Previous");
                            writeEnd();
                            writeStart("a",
                                    "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                    "href", page.url("", "offset", result.getNextOffset()));
                                writeHtml("Next ");
                                writeStart("i", "class", "icon-step-forward").writeEnd();
                            writeEnd();
                        writeEnd();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("#").writeEnd();
                                    writeStart("th").writeHtml("ID").writeEnd();
                                    writeStart("th").writeHtml("Type").writeEnd();
                                    writeStart("th").writeHtml("Label").writeEnd();
                                    for (String additionalField : additionalFields) {
                                        writeStart("th").writeHtml(additionalField).writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                            writeStart("tbody");
                                long offsetCopy = offset;
                                for (Object item : items) {
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    writeStart("tr");
                                        writeStart("td").writeHtml(++ offsetCopy).writeEnd();
                                        writeStart("td");
                                            writeStart("span",
                                                    "class", "link",
                                                    "onclick", "var $input = $(this).popup('source').prev();"
                                                            + "$input.val('" + itemState.getId() + "');"
                                                            + "$input.prev().text('" + StringUtils.escapeJavaScript(itemState.getLabel()) + "');"
                                                            + "$(this).popup('close');"
                                                            + "return false;");
                                                writeHtml(itemState.getId());
                                            writeEnd();
                                        writeEnd();
                                        writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                        writeStart("td").writeHtml(itemState.getLabel()).writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("td").writeHtml(itemState.getByPath(additionalField)).writeEnd();
                                        }
                                    writeEnd();
                                }
                            writeEnd();
                        writeEnd();
                    }

                } catch (RuntimeException ex) {
                    writeStart("div", "class", "alert alert-error");
                        writeObject(ex);
                    writeEnd();
                }
            writeEnd();
        }

        private void renderDefault() throws IOException {
            startPage("Database", "Query");

                writeStart("style", "type", "text/css");
                    write(".edit input[type=text], .edit textarea { width: 90%; }");
                    write(".edit textarea { min-height: 6em; }");

                    // For visibility filters.
                    write(".visibilityFilters li { display: inline-block; }");
                    write(".visibilityFilters input::-moz-focus-inner { border: 0; padding: 0; outline: 0; }");
                    write(".visibilityFilters .visibility-input { margin-right: 4px; }");
                    write(".visibilityFilters .visibility-label { margin-right: 12px; cursor: pointer }");
                    write(".visibilityFilters .visibility-label { -moz-user-select: none; -ms-user-select: none; -o-user-select: none; -webkit-user-select: none; user-select: none; }");
                    write(".visibilityFilters .visibility-label.disabled { background: #999 !important; }");
                writeEnd();

                includeStylesheet("/_resource/jquery/jquery.objectId.css");
                includeStylesheet("/_resource/jquery/jquery.repeatable.css");

                includeScript("/_resource/jquery/jquery.objectId.js");
                includeScript("/_resource/jquery/jquery.repeatable.js");

                writeStart("script", "type", "text/javascript");
                    write("(function() {");
                        write("$('.repeatable').repeatable();");
                        write("$('.objectId').objectId();");

                        // For visibility filters.
                        write("$(document).on('change', '.visibility-input', function(evt) {");
                            write("var $input = $(evt.target);");
                            write("$input.next('.visibility-label').toggleClass('disabled', !$input.prop('checked'));");
                        write("});");
                        write("$(document).on('click', '.visibility-label', function(evt) {");
                            write("var $label = $(evt.target);");
                            write("$label.prev('.visibility-input').trigger('click');");
                        write("});");
                        write("$(document).on('change', 'select[name=from]', function(evt) {");
                            write("console.log('changed!', $(evt.target).val());");
                            write("$('.typeChangeForm').find('input[name=from]').val($(evt.target).val()).end().submit();");
                        write("});");

                    write("})();");
                writeEnd();

                writeStart("form", "action", page.url(null), "class", "form-inline", "method", "get");

                    writeStart("h2").writeHtml("Query").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span6");
                            writeStart("select", "class", "span6", "name", "from");
                                writeStart("option", "value", "").writeHtml("ALL TYPES").writeEnd();

                                List<ObjectType> types = new ArrayList<ObjectType>(database.getEnvironment().getTypes());
                                Collections.sort(types, new ObjectFieldComparator("name", false));

                                for (ObjectType t : types) {
                                    if (!t.isEmbedded()) {
                                        writeStart("option",
                                                "selected", t.equals(type) ? "selected" : null,
                                                "value", t.getId());
                                            writeHtml(t.getLabel());
                                            writeHtml(" (");
                                            writeHtml(t.getInternalName());
                                            writeHtml(")");
                                        writeEnd();
                                    }
                                }
                            writeEnd();

                            includeStylesheet("/_resource/chosen/chosen.css");
                            includeScript("/_resource/chosen/chosen.jquery.min.js");
                            writeStart("script", "type", "text/javascript");
                                write("(function() {");
                                    write("$('select[name=from]').chosen({ 'search_contains': true });");
                                write("})();");
                            writeEnd();

                            writeStart("textarea",
                                    "class", "span6",
                                    "name", "where",
                                    "placeholder", "ID or Predicate (Leave Blank to Return All)",
                                    "rows", 4,
                                    "style", "margin-bottom: 4px; margin-top: 4px;");
                                writeHtml(where);
                            writeEnd();

                            writeElement("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Run");
                        writeEnd();

                        writeStart("div", "class", "span6");
                            writeStart("select", "name", "db", "style", "margin-bottom: 4px;");
                                writeStart("option",
                                        "value", "",
                                        "selected", ObjectUtils.isBlank(databaseName) ? "selected" : null);
                                    writeHtml("Default");
                                writeEnd();

                                for (Database db : Database.Static.getAll()) {
                                    String dbName = db.getName();
                                    writeStart("option",
                                            "value", dbName,
                                            "selected", dbName.equals(databaseName) ? "selected" : null);
                                        writeHtml(dbName);
                                    writeEnd();
                                }
                            writeEnd();

                            writeElement("br");
                            writeElement("input",
                                    "class", "input-small",
                                    "name", "sortField",
                                    "type", "text",
                                    "placeholder", "Sort",
                                    "value", sortField);
                            writeHtml(' ');
                            writeStart("select", "class", "input-small", "name", "sortOrder");
                                for (SortOrder so : SortOrder.values()) {
                                    writeStart("option",
                                            "selected", so.equals(sortOrder) ? "selected" : null,
                                            "value", so.name());
                                        writeHtml(so.displayName);
                                    writeEnd();
                                }
                            writeEnd();
                            writeHtml(' ');
                            writeElement("input",
                                    "class", "input-small",
                                    "name", "limit",
                                    "type", "text",
                                    "placeholder", "Limit",
                                    "value", limit);

                            writeElement("br");
                            writeElement("input",
                                    "class", "span6",
                                    "name", "additionalFields",
                                    "type", "text",
                                    "placeholder", "Additional Fields (Comma Separated)",
                                    "style", "margin-top: 4px;",
                                    "value", additionalFieldsString);

                            writeElement("br");
                            writeStart("label", "class", "checkbox");
                                writeElement("input",
                                        "name", "ignoreReadConnection",
                                        "type", "checkbox",
                                        "style", "margin-top: 4px;",
                                        "value", "true",
                                        "checked", ignoreReadConnection ? "checked" : null);
                                writeHtml(" Ignore read-specific connection settings");
                            writeEnd();

                            writeElement("br");
                            writeStart("div", "class", "frame", "name", "visibilityFilters");
                                if (visibilityFilters.isEmpty()) {
                                    writeStart("a",
                                            "target", "visibilityFilters",
                                            "href", page.url("", "action", "visibilityFilters"));
                                        writeHtml("Show Visibility Filters");
                                    writeEnd();

                                } else {
                                    renderVisibilityFilters();
                                }
                            writeEnd();

                        writeEnd();
                    writeEnd();

                    writeStart("h2", "style", "margin-top: 18px;").writeHtml("Filter").writeEnd();
                    writeStart("div", "class", "row");
                        writeStart("div", "class", "span12");
                            writeStart("textarea",
                                    "class", "span12",
                                    "name", "filter",
                                    "placeholder", "Predicate (Leave Blank to Return All)",
                                    "rows", 2,
                                    "style", "margin-bottom: 4px;");
                                writeHtml(filter);
                            writeEnd();
                            writeElement("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", "Filter");
                        writeEnd();
                    writeEnd();
                writeEnd();

                writeStart("form",
                        "target", "visibilityFilters",
                        "action", page.url("", "action", "visibilityFilters", "from", null),
                        "class", "form-inline typeChangeForm",
                        "method", "get");
                    writeElement("input",
                            "type", "hidden",
                            "name", "from",
                            "value", type != null ? type.getId() : null);
                    writeElement("input",
                            "type", "hidden",
                            "name", "showVisibilityFiltersLink",
                            "value", "true");
                writeEnd();

                try {
                    if (ObjectUtils.isBlank(filter)) {
                        PaginatedResult<Object> result = query.select(offset, limit);
                        List<Object> items = result.getItems();

                        if (offset == 0 && items.isEmpty()) {
                            writeStart("p", "class", "alert").writeHtml("No matches!").writeEnd();

                        } else {
                            writeStart("h2");
                                writeHtml("Result ");
                                writeObject(result.getFirstItemIndex());
                                writeHtml(" to ");
                                writeObject(result.getLastItemIndex());
                                writeHtml(" of ");
                                writeStart("span", "class", "frame");
                                    writeStart("a", "href", page.url("", "action", "count")).writeHtml("?").writeEnd();
                                writeEnd();
                            writeEnd();

                            writeStart("div", "class", "btn-group");
                                writeStart(offset > 0 ? "a" : "span",
                                        "class", "btn" + (offset > 0 ? "" : " disabled"),
                                        "href", offset > 0 ? page.url("", "offset", 0) : null);
                                    writeStart("i", "class", "icon-fast-backward").writeEnd();
                                    writeHtml(" First");
                                writeEnd();
                                writeStart(result.hasPrevious() ? "a" : "span",
                                        "class", "btn" + (result.hasPrevious() ? "" : " disabled"),
                                        "href", result.hasPrevious() ? page.url("", "offset", result.getPreviousOffset()) : null);
                                    writeStart("i", "class", "icon-step-backward").writeEnd();
                                    writeHtml(" Previous");
                                writeEnd();
                                writeStart(result.hasNext() ? "a" : "span",
                                        "class", "btn" + (result.hasNext() ? "" : " disabled"),
                                        "href", result.hasNext() ? page.url("", "offset", result.getNextOffset()) : null);
                                    writeHtml("Next ");
                                    writeStart("i", "class", "icon-step-forward").writeEnd();
                                writeEnd();

                            writeEnd();

                            String[] additionalFields;
                            if (ObjectUtils.isBlank(additionalFieldsString)) {
                                additionalFields = new String[0];
                            } else {
                                additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                            }

                            writeStart("table", "class", "table table-condensed");
                                writeStart("thead");
                                    writeStart("tr");
                                        writeStart("th").writeHtml("#").writeEnd();
                                        writeStart("th").writeHtml("ID").writeEnd();
                                        writeStart("th").writeHtml("Type").writeEnd();
                                        if (!visibilityFilters.isEmpty()) {
                                            writeStart("th").writeHtml("Visibility").writeEnd();
                                        }
                                        writeStart("th").writeHtml("Label").writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("th").writeHtml(additionalField).writeEnd();
                                        }
                                    writeEnd();
                                writeEnd();
                                writeStart("tbody");
                                    long offsetCopy = offset;
                                    for (Object item : items) {
                                        State itemState = State.getInstance(item);
                                        ObjectType itemType = itemState.getType();

                                        writeStart("tr");
                                            writeStart("td").writeHtml(++ offsetCopy).writeEnd();
                                            writeStart("td").writeHtml(itemState.getId()).writeEnd();
                                            writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                            if (!visibilityFilters.isEmpty()) {
                                                writeStart("td");
                                                    String visibilityLabel = itemState.getVisibilityLabel();

                                                    if (!StringUtils.isBlank(visibilityLabel)) {
                                                        Double hue = null;
                                                        ObjectField visibilityField = getVisibilityAwareField(itemState);

                                                        if (visibilityField != null) {
                                                            Object visibilityValue = itemState.get(visibilityField.getInternalName());

                                                            if (visibilityValue != null) {
                                                                hue = visibilityColors.get(visibilityField.getUniqueName() + "/" + String.valueOf(visibilityValue).toLowerCase().trim());
                                                            }
                                                        }

                                                        writeStart("span",
                                                                "class", "label",
                                                                "style", (hue != null ? "background: hsl(" + (hue * 360) + ",50%,50%);" : null));
                                                            writeHtmlOrDefault(itemState.getVisibilityLabel(), "?");
                                                        writeEnd();
                                                    }
                                                writeEnd();
                                            }
                                            writeStart("td");
                                                writeStart("a",
                                                        "target", "show",
                                                        "href", page.url("",
                                                                "action", "form",
                                                                "id", itemState.getId()));
                                                    writeHtml(itemState.getLabel());
                                                writeEnd();
                                            writeEnd();
                                            for (String additionalField : additionalFields) {
                                                writeStart("td").writeHtml(itemState.getByPath(additionalField)).writeEnd();
                                            }
                                        writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                        }

                    } else {
                        Predicate filterPredicate = PredicateParser.Static.parse(filter);
                        List<Object> items = new ArrayList<Object>();

                        writeStart("h2");
                            writeHtml("Filtered Result");
                        writeEnd();

                        String[] additionalFields;
                        if (ObjectUtils.isBlank(additionalFieldsString)) {
                            additionalFields = new String[0];
                        } else {
                            additionalFields = additionalFieldsString.trim().split("\\s*,\\s*");
                        }

                        writeStart("table", "class", "table table-condensed");
                            writeStart("thead");
                                writeStart("tr");
                                    writeStart("th").writeHtml("#").writeEnd();
                                    writeStart("th").writeHtml("ID").writeEnd();
                                    writeStart("th").writeHtml("Type").writeEnd();
                                    if (!visibilityFilters.isEmpty()) {
                                        writeStart("th").writeHtml("Visibility").writeEnd();
                                    }
                                    writeStart("th").writeHtml("Label").writeEnd();
                                    for (String additionalField : additionalFields) {
                                        writeStart("th").writeHtml(additionalField).writeEnd();
                                    }
                                writeEnd();
                            writeEnd();
                            writeStart("tbody");
                                long total = 0;
                                long matched = 0;

                                for (Object item : query.iterable(0)) {
                                    ++ total;
                                    if (total % 1000 == 0) {
                                        writeStart("tr");
                                            writeStart("td", "colspan", additionalFields.length + 4);
                                                writeHtml("Read ").writeObject(total).writeHtml(" items");
                                            writeEnd();
                                        writeEnd();
                                        flush();
                                    }

                                    if (!PredicateParser.Static.evaluate(item, filterPredicate)) {
                                        continue;
                                    }

                                    ++ matched;
                                    items.add(item);
                                    State itemState = State.getInstance(item);
                                    ObjectType itemType = itemState.getType();

                                    writeStart("tr");
                                        writeStart("td").writeHtml(matched).writeEnd();
                                        writeStart("td").writeHtml(itemState.getId()).writeEnd();
                                        writeStart("td").writeHtml(itemType != null ? itemType.getLabel() : null).writeEnd();
                                        if (!visibilityFilters.isEmpty()) {
                                            writeStart("td");
                                                String visibilityLabel = itemState.getVisibilityLabel();
                                                if (!StringUtils.isBlank(visibilityLabel)) {
                                                    Double hue = null;
                                                    ObjectField visibilityField = getVisibilityAwareField(itemState);

                                                    if (visibilityField != null) {
                                                        Object visibilityValue = itemState.get(visibilityField.getInternalName());

                                                        if (visibilityValue != null) {
                                                            hue = visibilityColors.get(visibilityField.getUniqueName() + "/" + String.valueOf(visibilityValue).toLowerCase().trim());
                                                        }
                                                    }

                                                    writeStart("span",
                                                            "class", "label",
                                                            "style", (hue != null ? "background: hsl(" + (hue * 360) + ",50%,50%);" : null));
                                                        writeHtmlOrDefault(itemState.getVisibilityLabel(), "?");
                                                    writeEnd();
                                                }
                                            writeEnd();
                                        }
                                        writeStart("td");
                                            writeStart("a", "href", "?where=id+%3D+" + itemState.getId(), "target", "_blank");
                                                writeHtml(itemState.getLabel());
                                            writeEnd();
                                        writeEnd();
                                        for (String additionalField : additionalFields) {
                                            writeStart("td").writeHtml(itemState.getByPath(additionalField)).writeEnd();
                                        }
                                    writeEnd();

                                    if (matched >= limit) {
                                        break;
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    }

                } catch (RuntimeException ex) {
                    writeStart("div", "class", "alert alert-error");
                        writeObject(ex);
                    writeEnd();
                }

            endPage();
        }
    }
}
