package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.DebugServlet;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.UuidUtils;
import com.psddev.dari.util.WebPageContext;

/** Debug servlet for running bulk database operations. */
@SuppressWarnings("serial")
public class BulkDebugServlet extends DebugServlet {

    public static final String INDEXER_PREFIX = "Database: Indexer: ";
    public static final String COPIER_PREFIX = "Database: Copier: ";

    @Override
    public String getName() {
        return "Database: Bulk Operations";
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-bulk");
    }

    @Override
    protected void service(
            final HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("all")
        final WebPageContext wp = new WebPageContext(this, request, response);
        Database selectedDatabase = Database.Static.getDefault();
        final List<ObjectType> types = new ArrayList<ObjectType>(selectedDatabase.getEnvironment().getTypes());
        Collections.sort(types, new ObjectFieldComparator("internalName", false));

        if (wp.isFormPost()) {
            String action = wp.param(String.class, "action");

            UUID selectedTypeId = wp.param(UUID.class, "typeId");
            ObjectType selectedType = null;
            for (ObjectType type : types) {
                if (type.getId().equals(selectedTypeId)) {
                    selectedType = type;
                    break;
                }
            }

            int writersCount = wp.paramOrDefault(int.class, "writersCount", 5);
            int commitSize = wp.paramOrDefault(int.class, "commitSize", 50);

            if ("index".equals(action)) {
                String executor = INDEXER_PREFIX + (selectedType != null ? selectedType.getInternalName() : "ALL");
                AsyncQueue<Object> queue = new AsyncQueue<Object>();
                Query<Object> query = Query
                        .fromType(selectedType)
                        .resolveToReferenceOnly();

                query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);

                new AsyncDatabaseReader<Object>(
                        executor, queue, selectedDatabase, query)
                        .submit();

                queue.closeAutomatically();

                for (int i = 0; i < writersCount; ++ i) {
                    new AsyncDatabaseWriter<Object>(
                            executor, queue, selectedDatabase, WriteOperation.INDEX, commitSize, true)
                            .submit();
                }

            } else if ("copy".equals(action)) {
                Database source = Database.Static.getInstance(wp.param(String.class, "source"));
                Database destination = Database.Static.getInstance(wp.param(String.class, "destination"));

                String executor = COPIER_PREFIX + " from " + source + " to " + destination;
                AsyncQueue<Object> queue = new AsyncQueue<Object>();
                Query<Object> query = Query
                        .fromType(selectedType)
                        .resolveToReferenceOnly();

                if (destination instanceof AbstractDatabase) {
                    Set<String> destinationGroups = ((AbstractDatabase<?>) destination).getGroups();

                    if (!destinationGroups.contains(UUID.randomUUID().toString())) {
                        Set<UUID> unsavableTypeIds = source.getEnvironment()
                                .getTypes()
                                .stream()
                                .map(Record::getId)
                                .collect(Collectors.toSet());

                        for (ObjectType type : source.getEnvironment().getTypes()) {
                            if (type.getObjectClass() == null
                                    || type.getGroups().contains(Modification.class.getName())
                                    || type.isAbstract()
                                    || type.isEmbedded()) {
                                unsavableTypeIds.remove(type.getId());
                            } else {
                                for (String typeGroup : type.getGroups()) {
                                    if (destinationGroups.contains(typeGroup)) {
                                        unsavableTypeIds.remove(type.getId());
                                        break;
                                    }
                                }
                            }
                        }

                        query.and("_type != ?", unsavableTypeIds);
                    }
                }

                query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);

                if (wp.param(boolean.class, "isResumable") && source instanceof SqlDatabase) {
                    String resumeIdStr = wp.param(String.class, "resumeId");
                    if (!StringUtils.isEmpty(resumeIdStr)) {
                        UUID resumeId = UuidUtils.fromString(resumeIdStr);
                        if (resumeId != null) {
                            query.where("_id >= ?", resumeId);
                        }
                    }
                }

                if (wp.param(boolean.class, "deleteDestination")) {
                    destination.deleteByQuery(query);
                }

                (new AsyncDatabaseReader<Object>(
                        executor, queue, source, query) {
                        @Override
                        protected Object produce() {
                            Object obj = super.produce();
                            if (obj instanceof Record) {
                                this.setProgress(this.getProgress() + " (last: " + ((Record) obj).getId() + ")");
                            }
                            return obj;
                        }
                }).submit();

                queue.closeAutomatically();
                System.gc();

                long maximumDataLength = Runtime.getRuntime().freeMemory() / 10 / writersCount / commitSize;

                System.out.println("maximum data length: " + maximumDataLength);

                for (int i = 0; i < writersCount; ++ i) {
                    AsyncDatabaseWriter<Object> writer = new AsyncDatabaseWriter<>(
                            executor, queue, destination, WriteOperation.SAVE_UNSAFELY, commitSize, true);

                    writer.setCommitSizeJitter(0.2);
                    writer.setMaximumDataLength(maximumDataLength);
                    writer.submit();
                }
            }

            wp.redirect(null);
            return;
        }

        final List<TaskExecutor> indexExecutors = new ArrayList<TaskExecutor>();
        final List<TaskExecutor> copyExecutors = new ArrayList<TaskExecutor>();
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            if (executor.getName().startsWith(INDEXER_PREFIX)) {
                indexExecutors.add(executor);
            } else if (executor.getName().startsWith(COPIER_PREFIX)) {
                copyExecutors.add(executor);
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "Bulk Operations");

                writeStart("h2").writeHtml("Index").writeEnd();

                writeStart("p");
                    writeHtml("Use this when you add ");
                    writeStart("code").writeHtml("@Indexed").writeEnd();
                    writeHtml(" to your model or if queries are returning unexpected results.");
                writeEnd();

                writeStart("form", "action", "", "class", "form-horizontal", "method", "post");
                    writeElement("input", "name", "action", "type", "hidden", "value", "index");

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label").writeHtml("Type").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "name", "typeId");
                                writeStart("option", "value", "").writeHtml("All").writeEnd();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        writeStart("option", "value", type.getId());
                                            writeHtml(type.getInternalName());
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("# Of Writers").writeEnd();
                        writeStart("div", "class", "controls");
                            writeElement("input", "name", "writersCount", "type", "text", "value", 5);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Commit Size").writeEnd();
                        writeStart("div", "class", "controls");
                            writeElement("input", "name", "commitSize", "type", "text", "value", 50);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "form-actions");
                        writeElement("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    writeEnd();
                writeEnd();

                if (!indexExecutors.isEmpty()) {
                    writeStart("h3").writeHtml("Ongoing Tasks").writeEnd();
                    writeStart("ul");
                        for (TaskExecutor executor : indexExecutors) {
                            writeStart("li");
                                writeStart("a", "href", "task");
                                    writeHtml(executor.getName());
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();
                }

                writeStart("h2").writeHtml("Copy").writeEnd();

                writeStart("p");
                    writeHtml("Use this to copy objects from one database to another.");
                writeEnd();

                writeStart("form", "action", "", "class", "form-horizontal", "method", "post");
                    writeElement("input", "name", "action", "type", "hidden", "value", "copy");

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label").writeHtml("Type").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "name", "typeId");
                                writeStart("option", "value", "").writeHtml("All").writeEnd();
                                for (ObjectType type : types) {
                                    if (!type.isEmbedded()) {
                                        writeStart("option", "value", type.getId());
                                            writeHtml(type.getInternalName());
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    List<Database> databases = Database.Static.getAll();

                    String resumeIdControlId = wp.createId();
                    String resumableControlId = wp.createId();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Source").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span3", "id", wp.getId(), "name", "source");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        writeStart("option", "value", database.getName(), "className", database.getClass().getName());
                                            writeHtml(database);
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                            writeStart("label", "class", "checkbox", "style", "margin-top: 5px;", "id", resumableControlId);
                                writeElement("input", "name", "isResumable", "type", "checkbox", "value", "true");
                                writeHtml("Resumable");
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group", "id", resumeIdControlId);
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Resume With ID").writeEnd();
                        writeStart("div", "class", "controls");
                            writeElement("input", "name", "resumeId", "type", "text");
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Destination").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span3", "id", wp.getId(), "name", "destination");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (Database database : databases) {
                                    if (!(database instanceof Iterable)) {
                                        writeStart("option", "value", database.getName(), "className", database.getClass().getName());
                                            writeHtml(database);
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                            writeStart("label", "class", "checkbox", "style", "margin-top: 5px;");
                                writeElement("input", "name", "deleteDestination", "type", "checkbox", "value", "true");
                                writeHtml("Delete before copy");
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("# Of Writers").writeEnd();
                        writeStart("div", "class", "controls");
                            writeElement("input", "name", "writersCount", "type", "text", "value", 5);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Commit Size").writeEnd();
                        writeStart("div", "class", "controls");
                            writeElement("input", "name", "commitSize", "type", "text", "value", 50);
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "form-actions");
                        writeElement("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    writeEnd();
                writeEnd();

                includeStylesheet("/_resource/chosen/chosen.css");
                includeScript("/_resource/chosen/chosen.jquery.min.js");

                writeStart("script", "type", "text/javascript");
                    write("$(document).ready(function() {");
                        write("$('#" + resumableControlId + "').hide();");
                        write("$('[name=source]').change(function() {");
                             write("if ($('option:selected', this).attr('className') == '" + SqlDatabase.class.getName() + "') {");
                                 write("$('#" + resumableControlId + "').show();");
                             write("} else {");
                                 write("$('#" + resumableControlId + "').hide();");
                             write("}");
                        write("});");
                        write("$('#" + resumeIdControlId + "').hide();");
                        write("$('[name=isResumable]').change(function() {");
                            write("if ($(this).is(':checked')) {");
                                write("$('#" + resumeIdControlId + "').show();");
                            write("} else {");
                                write("$('#" + resumeIdControlId + "').hide();");
                            write("}");
                        write("});");

                        //Initialize chosen select
                        write("$('select[name=typeId]').chosen({ 'search_contains': true });");
                        write("$('.chzn-drop').css({'width': 'auto', 'white-space': 'nowrap', 'border-top': '1px solid #aaa'});");

                    write("})");
                writeEnd();

                if (!copyExecutors.isEmpty()) {
                    writeStart("h3").writeHtml("Ongoing Tasks").writeEnd();
                    writeStart("ul");
                        for (TaskExecutor executor : copyExecutors) {
                            writeStart("li");
                                writeStart("a", "href", "task");
                                    writeHtml(executor.getName());
                                writeEnd();
                            writeEnd();
                        }
                    writeEnd();
                }

            endPage();
        } };
    }
}
