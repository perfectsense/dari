package com.psddev.dari.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugServlet;
import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.joda.time.DateTime;

import com.psddev.dari.util.BuildDebugServlet;
import com.psddev.dari.util.DebugFilter;
import com.psddev.dari.util.MultipartRequest;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TaskExecutor;
import com.psddev.dari.util.WebPageContext;

/** Debug servlet for importing/exporting data. */
@SuppressWarnings("serial")
public class BootstrapDebugServlet extends DebugServlet {

    public static final String LIVE_DOWNLOAD_BUTTON_TEXT = "Live Download";
    public static final String SNAPSHOT_BUTTON_TEXT = "Save Snapshot";
    public static final String IMPORT_BUTTON_TEXT = "Import";
    public static final String DELETE_AND_IMPORT_BUTTON_TEXT = "Delete and Import";

    @Override
    public String getName() {
        return "Database: Bootstrap";
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-bootstrap");
    }

    @Override
    protected void service(
            final HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        @SuppressWarnings("all")
        final WebPageContext wp = new WebPageContext(this, request, response);
        final Database selectedDatabase = Database.Static.getDefault();

        final List<UUID> additionalTypeIds = wp.params(UUID.class, "additionalTypeIds");
        final Set<ObjectType> additionalTypes = new HashSet<ObjectType>();
        for (UUID typeId : additionalTypeIds) {
            ObjectType type = selectedDatabase.getEnvironment().getTypeById(typeId);
            if (type != null) {
                additionalTypes.add(type);
            }
        }
        final String pkgName = wp.param(String.class, "pkgName");

        if (wp.isFormPost()) {
            String action = wp.param(String.class, "action");
            if (LIVE_DOWNLOAD_BUTTON_TEXT.equals(action) || SNAPSHOT_BUTTON_TEXT.equals(action)) {
                Properties properties = BuildDebugServlet.getProperties(getServletContext());
                String projectName = ObjectUtils.firstNonBlank(properties.getProperty("name"), "dari");
                BootstrapPackage pkg = null;
                if (!StringUtils.isEmpty(pkgName)) {
                    pkg = BootstrapPackage.Static.getPackage(selectedDatabase, pkgName);
                }
                if (pkg != null && LIVE_DOWNLOAD_BUTTON_TEXT.equals(action)) {
                    response.setContentType("application/gzip");
                    String filename = "bootstrap." + StringUtils.toNormalized(projectName) + "." + StringUtils.toNormalized(pkgName) + "." + new DateTime().toString("yyyyMMdd.HHmm") + ".txt.gz";
                    response.setHeader("Content-Disposition", "attachment; filename=" + filename);
                    try {
                        GZIPOutputStream gzOut = new GZIPOutputStream(response.getOutputStream());
                        Writer outputWriter = new OutputStreamWriter(gzOut);
                        BootstrapPackage.Static.writeContents(selectedDatabase, pkg, additionalTypes, outputWriter, projectName);
                        gzOut.finish();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    return;
                } else if (pkg != null && SNAPSHOT_BUTTON_TEXT.equals(action)) {
                    // TODO: upload snapshot as StorageItem
                    // LOGGER.info("snapshot!");
                    return;
                }
            } else if (action == null) {
                // replace form with multipart form so we can read it
                if (ServletFileUpload.isMultipartContent(wp.getRequest())) {
                    MultipartRequest req = new MultipartRequest(wp.getRequest());
                    if (req != null) {
                        action = req.getParameter("action");
                        if (IMPORT_BUTTON_TEXT.equals(action) || DELETE_AND_IMPORT_BUTTON_TEXT.equals(action)) {
                            if (wp.param(boolean.class, "deleteBeforeImport")) {
                                action = DELETE_AND_IMPORT_BUTTON_TEXT;
                            }

                            FileItem file = req.getFileItem("file");
                            if (file != null) {
                                Integer numWriters = ObjectUtils.to(Integer.class, req.getParameter("numWriters"));
                                Integer commitSize = ObjectUtils.to(Integer.class, req.getParameter("commitSize"));

                                if (numWriters == null || numWriters < 1) {
                                    numWriters = 1;
                                }

                                if (commitSize == null || commitSize < 1) {
                                    commitSize = 1;
                                }

                                String contentType = file.getContentType();
                                String fileName = file.getName();
                                InputStream input = file.getInputStream();
                                InputStream fileInput = input;

                                if ((contentType != null && ("application/x-gzip".equals(contentType) || "application/gzip".equals(contentType))) || (fileName != null && fileName.endsWith(".gz"))) {
                                    fileInput = new GZIPInputStream(input);
                                }
                                boolean deleteFirst = false;

                                if (action.equals(DELETE_AND_IMPORT_BUTTON_TEXT)) {
                                    deleteFirst = true;
                                }
                                BootstrapPackage.Static.importContents(selectedDatabase, fileName, fileInput, deleteFirst, numWriters, commitSize);
                            }
                        }
                    }
                }
            }
        }

        new DebugFilter.PageWriter(getServletContext(), request, response) { {
            startPage("Database", "Bootstrap");

                writeStart("div", "class", "row-fluid");
                    writeStart("div", "class", "span6"); // lhs/rhs
                        writeStart("h2").writeHtml("Download Bootstrap Packages").writeEnd();

                        writeStart("p").writeHtml("Bootstrap Packages are created when a class is annotated with ").writeStart("code").writeRaw("@BootstrapPackages(\"My&nbsp;Package&nbsp;Name\")").writeEnd().writeHtml(". These files contain all of the records of the specified type(s) in the database. ").writeStart("code").writeHtml("Metric").writeEnd().writeHtml(" data is ").writeStart("strong").writeHtml("not included").writeEnd().writeHtml(".").writeEnd();

                        writeStart("table", "width", "100%");

                        List<BootstrapPackage> packages = BootstrapPackage.Static.getPackages(selectedDatabase);
                        for (BootstrapPackage pkg : packages) {
                            // if (pkgName != null && ! pkg.getName().equals(pkgName)) continue;
                            if (pkg.getName().equals(pkgName)) {
                                if (!additionalTypes.isEmpty()) {
                                    BootstrapPackage.Static.checkConsistency(selectedDatabase, pkg, new HashSet<BootstrapPackage>(packages), additionalTypes);
                                }
                            }
                            writeStart("form", "action", wp.url(null), "class", "form-horizontal", "method", "post");
                            boolean first;
                            String showWarningsId = wp.createId();
                            String showTypesId = wp.createId();
                            writeStart("tr", "style", "height: 3.5em;");
                                writeStart("td").writeStart("h3").writeHtml(pkg.getName()).writeEnd().writeEnd();

                                writeStart("td", "align", "right");
                                if (pkg.isInit()) {
                                    writeHtml(" ");
                                    writeStart("span", "class", "label label-important").writeHtml("All Types").writeEnd(); // /span
                                    writeHtml(" ");
                                } else {
                                    if (pkg.getMissingTypes().isEmpty() && pkg.getTypesInOtherPackages().isEmpty()) {
                                        // writeStart("span", "class", "label label-success").writeHtml("OK").writeEnd(); // /span
                                    } else {
                                        String labelClass = "label-warning";
                                        String labelLabel = "Dependencies";
                                        writeStart("span", "show-id", showWarningsId, "class", "show-button label " + labelClass).writeHtml(labelLabel).writeEnd(); // /span
                                    }

                                    writeHtml(" ");
                                    writeStart("span", "class", "show-button label label-success", "show-id", showTypesId).writeHtml("Types").writeEnd(); // /span
                                    writeHtml(" ");
                                }

                                    writeElement("input", "type", "hidden", "name", "pkgName", "value", pkg.getName());
                                    writeElement("input", "class", "btn btn-primary", "type", "submit", "name", "action", "value", LIVE_DOWNLOAD_BUTTON_TEXT);
                                    writeHtml(" ");
                                    // writeElement("input", "class", "btn btn-warning", "type", "submit", "name", "action", "value", SNAPSHOT_BUTTON_TEXT);
                                writeEnd();
                            writeEnd();

                            if (!pkg.getMissingTypes().isEmpty() || !pkg.getTypesInOtherPackages().isEmpty()) {
                                String adtlClass = "button-hidden";
                                if (pkg.getName().equals(pkgName)) {
                                    adtlClass = "";
                                }
                                writeStart("tr", "id", showWarningsId, "class", adtlClass).writeStart("td", "colspan", "2");
                                    if (!pkg.getTypesInOtherPackages().isEmpty() || !pkg.getMissingTypes().isEmpty()) {
                                        writeStart("h4").writeHtml("Dependencies and Missing Types: ").writeEnd();
                                        writeStart("p").writeStart("strong").writeHtml(pkg.getName()).writeEnd().writeHtml(" does not include the following types which are referenced by included types. If this package is imported as-is into a database that does not already contain this data and these types are not also imported, the values in these fields will be ").writeStart("code").writeHtml("null").writeEnd().writeHtml(". ");
                                        writeHtml("To include them in this download, check the checkboxes below before clicking " + LIVE_DOWNLOAD_BUTTON_TEXT + /*" or " + SNAPSHOT_BUTTON_TEXT + */ ".").writeEnd();

                                        Map<ObjectType, Set<ObjectField>> missingTypes = new TreeMap<ObjectType, Set<ObjectField>>(pkg.getMissingTypes());
                                        if (pkg.getName().equals(pkgName)) {
                                            for (ObjectType additionalType : additionalTypes) {
                                                if (!missingTypes.keySet().contains(additionalType)) {
                                                    missingTypes.put(additionalType, new HashSet<ObjectField>());
                                                }
                                            }
                                        }
                                        if (pkg.getName().equals(pkgName) && !additionalTypeIds.isEmpty()) {
                                            writeStart("a", "href", "?").writeStart("span", "class", "label label-important").writeHtml("Clear").writeEnd().writeEnd();
                                        }
                                        for (Map.Entry<ObjectType, Set<ObjectField>> entry : missingTypes.entrySet()) {
                                            boolean allGlobalModifications = true;
                                            for (ObjectField f : entry.getValue()) {
                                                if (f.getParentType() != null) {
                                                    allGlobalModifications = false;
                                                }
                                            }
                                            String adtlStyle = "";
                                            if (allGlobalModifications) {
                                                adtlStyle = "color: #888;";
                                            }
                                            writeStart("label", "class", "checkbox control-label", "style", adtlStyle);
                                                String adtlChecked = "";
                                                if (wp.params(UUID.class, "additionalTypeIds").contains(entry.getKey().getId())) {
                                                    adtlChecked = "CHECKED";
                                                }
                                                writeElement("input", "type", "checkbox", "name", "additionalTypeIds", "value", entry.getKey().getId(), adtlChecked, adtlChecked);
                                                writeStart("abbr", "style", "text-transform: none;", "title", entry.getKey().getInternalName());
                                                writeHtml(entry.getKey().getDisplayName());
                                                writeEnd();
                                                if (!entry.getValue().isEmpty()) {
                                                    writeHtml(" (referenced by ");
                                                    Map<String, String> uniqueNames = new TreeMap<String, String>();
                                                    for (ObjectField f : entry.getValue()) {
                                                        if (f.getParentType() == null) {
                                                            uniqueNames.put("Global Modification " + f.getUniqueName(), f.getDisplayName());
                                                        } else {
                                                            uniqueNames.put(f.getUniqueName(), f.getParentType().getDisplayName() + "/" + f.getDisplayName());
                                                        }
                                                    }
                                                    first = true;
                                                    for (Map.Entry<String, String> f : uniqueNames.entrySet()) {
                                                        if (!first) {
                                                            writeHtml(", ");
                                                        } else {
                                                            first = false;
                                                        }
                                                        writeStart("abbr", "style", "text-transform: none;", "title", f.getKey());
                                                        writeHtml(f.getValue());
                                                        writeEnd();
                                                    }
                                                    if (pkg.getTypesInOtherPackages().get(entry.getKey()) != null) {
                                                        writeHtml("; included in package(s) ");
                                                        Set<String> uniquePackageNames = new HashSet<String>();
                                                        for (BootstrapPackage bp : pkg.getTypesInOtherPackages().get(entry.getKey())) {
                                                            uniquePackageNames.add(bp.getName());
                                                        }
                                                        first = true;
                                                        for (String f : uniquePackageNames) {
                                                            if (!first) {
                                                                writeHtml(", ");
                                                            } else {
                                                                first = false;
                                                            }
                                                            writeStart("strong").writeHtml(f).writeEnd();
                                                        }
                                                    }
                                                    writeHtml(")");
                                                }
                                            writeEnd();
                                        }
                                    }
                                writeEnd().writeEnd(); // /td /tr
                            }

                            writeStart("tr", "class", "button-hidden", "id", showTypesId).writeStart("td", "colspan", "2");
                            writeStart("p").writeStart("h4").writeHtml("Types: ").writeEnd();
                                first = true;
                                Set<ObjectType> types = pkg.getTypes();
                                for (ObjectType type : types) {
                                    if (!first) {
                                        writeHtml(", ");
                                    } else {
                                        first = false;
                                    }
                                    writeStart("abbr", "style", "text-transform: none; font-weight: bold;", "title", type.getInternalName());
                                    writeHtml(type.getDisplayName());
                                    writeEnd();
                                }
                                for (ObjectType type : BootstrapPackage.Static.getAllTypes(selectedDatabase, pkg)) {
                                    if (types.contains(type)) {
                                        continue;
                                    }
                                    if (!first) {
                                        writeHtml(", ");
                                    } else {
                                        first = false;
                                    }
                                    writeStart("abbr", "style", "text-transform: none;", "title", type.getInternalName());
                                    writeHtml(type.getDisplayName());
                                    writeEnd();
                                }
                            writeEnd();

                            writeStart("p").writeStart("h4").writeHtml("Follow Reference Types: ").writeEnd();
                                first = true;
                                Set<ObjectType> followReferenceTypes = new HashSet<ObjectType>();
                                for (ObjectType type : types) {
                                    for (String fieldName : type.as(BootstrapPackage.TypeData.class).getFollowReferencesFields()) {
                                        followReferenceTypes.addAll(type.getField(fieldName).getTypes());
                                    }
                                }
                                for (ObjectType type : followReferenceTypes) {
                                    if (!first) {
                                        writeHtml(", ");
                                    } else {
                                        first = false;
                                    }
                                    writeStart("abbr", "style", "text-transform: none; font-weight: bold;", "title", type.getInternalName());
                                    writeHtml(type.getDisplayName());
                                    writeEnd();
                                }
                            writeEnd();

                            if (pkg.getName().equals(pkgName) && !additionalTypes.isEmpty()) {
                                writeStart("p").writeStart("h4").writeHtml("Included Dependencies: ").writeEnd();
                                    first = true;
                                    for (ObjectType type : additionalTypes) {
                                        if (!first) {
                                            writeHtml(", ");
                                        } else {
                                            first = false;
                                        }
                                        writeStart("abbr", "style", "text-transform: none; font-weight: bold;", "title", type.getInternalName());
                                        writeHtml(type.getDisplayName());
                                        writeEnd();
                                    }
                                    for (ObjectType type : BootstrapPackage.Static.getAllTypes(selectedDatabase, additionalTypes)) {
                                        if (additionalTypes.contains(type)) {
                                            continue;
                                        }
                                        if (!first) {
                                            writeHtml(", ");
                                        } else {
                                            first = false;
                                        }
                                        writeStart("abbr", "style", "text-transform: none;", "title", type.getInternalName());
                                        writeHtml(type.getDisplayName());
                                        writeEnd();
                                    }
                                writeEnd();
                            }
                            writeEnd().writeEnd();

                            writeEnd();
                        }

                        writeEnd();

                        writeStart("script", "type", "text/javascript");
                            write("$(document).ready(function() {");
                                write("$('.button-hidden').hide();");
                                write("$('.show-button').css('cursor', 'pointer').click(function() {");
                                    write("$('#'+$(this).attr('show-id')).toggle();");
                                write("});");
                                write("$('input[name=additionalTypeIds]').change(function() {");
                                    write("if ($('input[name=additionalTypeIds]:checked').length == 0) {");
                                    write("window.location='?';");
                                    write("} else {");
                                    write("$(this.form).attr('method', 'GET').submit();");
                                    write("}");
                                write("});");
                            write("})");
                        writeEnd();
                    writeEnd();

                    writeStart("div", "class", "span6"); // lhs/rhs
                        writeStart("h2").writeHtml("Import Bootstrap Package").writeEnd();

                        writeStart("form", "action", wp.url(null), "class", "form-horizontal", "method", "post", "enctype", "multipart/form-data");
                        writeStart("p").writeHtml("Upload a bootstrap export and import it into the database. Objects with the same ").writeStart("code").writeHtml("id").writeEnd().writeHtml(" will be overwritten. If \"Delete Before Import\" is checked, all objects of the included types will be deleted before the new objects are saved. ").writeEnd();
                        writeStart("p").writeHtml("This is a potentially ").writeStart("strong").writeHtml("DANGEROUS").writeEnd().writeHtml(" operation, and may result in data loss. ").writeEnd();

                        writeStart("div", "class", "control-group");
                            writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Upload Bootstrap File").writeEnd();
                            writeStart("div", "class", "controls");
                                writeElement("input", "type", "file", "name", "file");
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "control-group");
                            writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Delete Before Import?").writeEnd();
                            writeStart("div", "class", "controls");
                                writeElement("input", "id", "deleteCheckbox", "type", "checkbox", "name", "deleteBeforeImport", "value", "true");
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "control-group");
                            writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("# of Writers").writeEnd();
                            writeStart("div", "class", "controls");
                                writeElement("input", "id", "numWriters", "type", "text", "name", "numWriters", "value", 5);
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "control-group");
                            writeStart("label", "class", "control-label", "id", wp.createId()).writeHtml("Commit Size").writeEnd();
                            writeStart("div", "class", "controls");
                                writeElement("input", "name", "commitSize", "type", "text", "value", 50);
                            writeEnd();
                        writeEnd();

                        writeStart("div", "class", "form-actions");
                            writeElement("input", "id", "importBtn", "type", "submit", "name", "action", "class", "btn btn-primary", "value", IMPORT_BUTTON_TEXT);
                        writeEnd();

                        writeStart("script", "type", "text/javascript");
                            write("$(document).ready(function() {");
                                write("$('#deleteCheckbox').change(function() {");
                                    write("if ($(this).is(':checked')) {");
                                        write("$('#importBtn').val('" + DELETE_AND_IMPORT_BUTTON_TEXT + "').removeClass('btn-primary').addClass('btn-danger');");
                                    write("} else {");
                                        write("$('#importBtn').val('" + IMPORT_BUTTON_TEXT + "').removeClass('btn-danger').addClass('btn-primary');");
                                    write("}");
                                write("});");
                                write("$('#deleteCheckbox').change();");
                            write("})");
                        writeEnd();

                        writeEnd();

                        //

                        List<TaskExecutor> ongoingTasks = new ArrayList<TaskExecutor>();
                        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
                            if (executor.getName().startsWith(BootstrapImportTask.EXECUTOR_PREFIX)) {
                                ongoingTasks.add(executor);
                            }
                        }
                        if (!ongoingTasks.isEmpty()) {
                            writeStart("h3").writeHtml("Ongoing Tasks").writeEnd();
                            writeStart("ul");
                            for (TaskExecutor executor : ongoingTasks) {
                                writeStart("li");
                                    writeStart("a", "href", "task");
                                        writeHtml(executor.getName());
                                    writeEnd();
                                writeEnd();
                            }
                            writeEnd();
                        }
                    writeEnd();
                writeEnd();

            endPage();
        } };
    }
}
