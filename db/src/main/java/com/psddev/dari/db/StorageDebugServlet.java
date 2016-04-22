package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.AsyncQueue;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.StorageItem;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.WebPageContext;

/** Debug servlet for running bulk storage item operations. */
@DebugFilter.Path("db-storage")
@SuppressWarnings("serial")
public class StorageDebugServlet extends HttpServlet {

    public static final String COPIER_PREFIX = "Database: Storage Item Copier: ";

    // --- HttpServlet support ---

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
            UUID selectedTypeId = wp.param(UUID.class, "typeId");
            ObjectType selectedType = null;
            for (ObjectType type : types) {
                if (type.getId().equals(selectedTypeId)) {
                    selectedType = type;
                    break;
                }
            }

            String source = wp.param(String.class, "source");
            String destination = wp.param(String.class, "destination");
            boolean saveObject = wp.param(boolean.class, "saveObject");

            String executor = COPIER_PREFIX + " from " + source + " to " + destination;
            AsyncQueue<Object> queue = new AsyncQueue<Object>();
            Database database = Database.Static.getDefault();
            Query<Object> query = Query.fromType(selectedType);

            query.getOptions().put(SqlDatabase.USE_JDBC_FETCH_SIZE_QUERY_OPTION, false);

            new AsyncDatabaseReader<Object>(
                    executor, queue, database, query)
                    .submit();

            queue.closeAutomatically();

            for (int i = 0; i < 5; ++ i) {
                new AsyncStorageItemWriter<Object>(
                        executor, queue, database, WriteOperation.SAVE_UNSAFELY, 50, true, source, destination, saveObject)
                        .submit();
            }

            wp.redirect(null);
            return;
        }

        final List<TaskExecutor> copyExecutors = new ArrayList<TaskExecutor>();
        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            if (executor.getName().startsWith(COPIER_PREFIX)) {
                copyExecutors.add(executor);
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "Storage Item Bulk Operations");

                writeStart("h2");
                    writeHtml("Copy");
                writeEnd();

                writeStart("p");
                    writeHtml("Use this to copy storage items from one location to another.");
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

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Source").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span2", "id", wp.getId(), "name", "source");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (String name : StorageItem.Static.getStorages()) {
                                    writeStart("option", "value", name).writeHtml(name).writeEnd();
                                }
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "control-group");
                        writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Destination").writeEnd();
                        writeStart("div", "class", "controls");
                            writeStart("select", "class", "span2", "id", wp.getId(), "name", "destination");
                                writeStart("option", "value", "").writeHtml("").writeEnd();
                                for (String name : StorageItem.Static.getStorages()) {
                                    writeStart("option", "value", name).writeHtml(name).writeEnd();
                                }
                            writeEnd();
                            writeStart("label", "class", "checkbox", "style", "margin-top: 5px;");
                                writeElement("input", "name", "saveObject", "type", "checkbox", "value", "true");
                                writeHtml("Save object? (slower - consider reusing the original storage name instead)");
                            writeEnd();
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "form-actions");
                        writeElement("input", "type", "submit", "class", "btn btn-success", "value", "Start");
                    writeEnd();
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
