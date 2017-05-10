package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.DebugServlet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.WebPageContext;

@Deprecated
@SuppressWarnings("serial")
public class ODataServlet extends DebugServlet {

    @Override
    public String getName() {
        return null;
    }

    @Override
    public List<String> getPaths() {
        return Collections.singletonList("db-odata");
    }

    @Override
    protected void service(
            HttpServletRequest request,
            HttpServletResponse response)
            throws IOException, ServletException {

        WebPageContext context = new WebPageContext(this, request, response);
        String pathInfo = request.getPathInfo();

        response.setContentType("application/xml");
        response.getWriter().write("<?xml version=\"1.0\" encoding=\"utf-8\" standalone=\"yes\"?>");

        if (pathInfo == null) {
            response.sendError(HttpServletResponse.SC_NOT_FOUND);

        } else if ("/".equals(pathInfo)) {
            renderServiceDocument(context);

        } else if ("/$metadata".equals(pathInfo)) {
            renderMetadataDocument(context);

        } else {
            String group = pathInfo.substring(1);
            // String extra = null;

            int slashAt = group.indexOf('/', 1);
            if (slashAt > -1) {
                // extra = group.substring(slashAt + 1);
                group = group.substring(0, slashAt);
            }

            Object id = null;
            int openAt = group.indexOf('(');
            if (openAt > -1) {
                int closeAt = group.indexOf(')', openAt);
                if (closeAt > -1) {
                    id = group.substring(openAt + 1, closeAt);
                    group = group.substring(0, openAt);
                }
            }

            Query<?> query = Query.fromGroup(group.replace('_', '.'));
            if (!ObjectUtils.isBlank(id)) {
                query.and("_id = ?", id);
            }

            String where = context.param(String.class, "$filter");
            if (!ObjectUtils.isBlank(where)) {
                query.and(where);
            }

            String sorts = context.param(String.class, "$orderby");
            if (!ObjectUtils.isBlank(sorts)) {
                for (String sort : sorts.trim().split("\\s*,\\s*")) {
                    String field;
                    String operator = Sorter.ASCENDING_OPERATOR;
                    int spaceAt = sort.indexOf(' ');

                    if (spaceAt < 0) {
                        field = sort.trim();

                    } else {
                        field = sort.substring(0, spaceAt).trim();
                        if ("desc".equalsIgnoreCase(sort.substring(spaceAt + 1).trim())) {
                            operator = Sorter.DESCENDING_OPERATOR;
                        }
                    }

                    query.sort(operator, field);
                }
            }

            renderAtomFeed(context, query.select(
                    context.param(long.class, "$skip"),
                    context.paramOrDefault(int.class, "$top", 10)));
        }
    }

    private String getAbsoluteUrl(WebPageContext context, String path, Object... parameters) {
        HttpServletRequest request = context.getRequest();
        String servletPath = request.getServletPath();
        String pathInfo = request.getPathInfo();
        path = servletPath.substring(0, servletPath.length() - pathInfo.length()) + "/" + path;
        return JspUtils.getAbsoluteUrl(request, path, parameters);
    }

    private String getEntitySetName(ObjectType type) {
        return type.getInternalName().replace('.', '_').replace('$', '_');
    }

    private String getEntityTypeName(ObjectType type) {
        return type.getInternalName().replace('.', '_').replace('$', '_');
    }

    private void renderServiceDocument(WebPageContext context) throws IOException {
        @SuppressWarnings("all")
        XmlWriter writer = new XmlWriter(context.getWriter());
        writer.writeStart("app:service",
                "xmlns:atom", "http://www.w3.org/2005/Atom",
                "xmlns:app", "http://www.w3.org/2007/app",
                "xmlns:data", "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "xmlns:metadata", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");
            writer.writeStart("app:workspace");
                for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
                    writer.writeStart("app:collection", "href", getAbsoluteUrl(context, getEntitySetName(type)));
                        writer.writeStart("atom:title").writeHtml(type.getDisplayName()).writeEnd();
                    writer.writeEnd();
                }
            writer.writeEnd();
        writer.writeEnd();
    }

    private String getEdmType(ObjectField field) {
        String edmType = "Edm.String";
        String itemType = field.getInternalItemType();

        if (ObjectField.BOOLEAN_TYPE.equals(itemType)) {
            edmType = "Edm.Boolean";

        } else if (ObjectField.DATE_TYPE.equals(itemType)) {
            edmType = "Edm.DateTime";

        } else if (ObjectField.NUMBER_TYPE.equals(itemType)) {
            if (ObjectUtils.equals(field.getStep(), 1.0)) {
                edmType = "Edm.Int64";
            } else {
                edmType = "Edm.Double";
            }
        }

        /*String type = field.getInternalType();
        for (int slashAt = 0; (slashAt = type.indexOf('/', slashAt + 1)) > -1; ) {
            edmType = "Collection(" + edmType + ")";
        }*/

        return edmType;
    }

    private void renderMetadataDocument(WebPageContext context) throws IOException {
        @SuppressWarnings("all")
        XmlWriter writer = new XmlWriter(context.getWriter());

        writer.writeStart("edmx:Edmx",
                "xmlns:edmx", "http://schemas.microsoft.com/ado/2007/06/edmx",
                "xmlns:m", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                "xmlns:d", "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "xmlns", "http://schemas.microsoft.com/ado/2008/09/edm",
                "Version", "1.0");
            writer.writeStart("edmx:DataServices", "m:DataServiceVersion", "1.0");

                writer.writeStart("Schema", "Namespace", "DariEntityTypes");
                    Map<ObjectType, Map<String, ObjectField>> associationsByType = new HashMap<ObjectType, Map<String, ObjectField>>();

                    for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
                        String typeName = getEntityTypeName(type);

                        writer.writeStart("EntityType", "Name", typeName);
                            writer.writeStart("Key");
                                writer.writeStart("PropertyRef", "Name", "id").writeEnd();
                            writer.writeEnd();
                            writer.writeStart("Property",
                                    "Name", "id",
                                    "Type", "Edm.Guid",
                                    "Nullable", "false").writeEnd();

                            Map<String, ObjectField> associations = new HashMap<String, ObjectField>();
                            associationsByType.put(type, associations);

                            FIELD: for (ObjectField field : type.getFields()) {
                                if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                                    for (ObjectType child : field.getTypes()) {
                                        String propertyName = field.getInternalName().replace('.', '_');
                                        String childName = getEntityTypeName(child);
                                        String relationship = propertyName + "_" + typeName + "_" + childName;
                                        associations.put(relationship, field);

                                        writer.writeStart("NavigationProperty",
                                                "Name", propertyName,
                                                "Relationship", "DariEntityTypes." + relationship,
                                                "FromRole", "E1",
                                                "ToRole", "E2").writeEnd();
                                        continue FIELD;
                                    }
                                }

                                writer.writeStart("Property",
                                        "Name", field.getInternalName().replace('.', '_'),
                                        "Type", getEdmType(field),
                                        "Nullable", "true").writeEnd();
                            }
                        writer.writeEnd();
                    }

                    for (Map.Entry<ObjectType, Map<String, ObjectField>> entry1 : associationsByType.entrySet()) {
                        ObjectType type = entry1.getKey();
                        String typeName = getEntityTypeName(type);

                        for (ObjectField field : entry1.getValue().values()) {
                            for (ObjectType child : field.getTypes()) {
                                String propertyName = field.getInternalName().replace('.', '_');
                                String childName = getEntityTypeName(child);
                                String relationship = propertyName + "_" + typeName + "_" + childName;

                                writer.writeStart("Association", "Name", relationship);
                                    writer.writeStart("End",
                                            "Type", "DariEntityTypes." + typeName,
                                            "Multiplicity", "*",
                                            "Role", "E1").writeEnd();
                                    writer.writeStart("End",
                                            "Type", "DariEntityTypes." + childName,
                                            "Multiplicity", field.getInternalType().indexOf('/') > -1 ? "*" : "0..1",
                                            "Role", "E2").writeEnd();
                                writer.writeEnd();
                            }
                        }
                    }
                writer.writeEnd();

                writer.writeStart("Schema", "Namespace", "DariEntitySets");
                    writer.writeStart("EntityContainer", "Name", "Entities", "m:IsDefaultEntityContainer", "true");
                        for (ObjectType type : Database.Static.getDefault().getEnvironment().getTypes()) {
                            if (!type.isEmbedded()) {
                                writer.writeStart("EntitySet",
                                        "Name", getEntitySetName(type),
                                        "EntityType", "DariEntityTypes." + getEntityTypeName(type)).writeEnd();
                            }
                        }
                    writer.writeEnd();
                writer.writeEnd();

            writer.writeEnd();
        writer.writeEnd();
    }

    private void renderAtomFeed(WebPageContext context, PaginatedResult<?> result) throws IOException {
        @SuppressWarnings("all")
        XmlWriter writer = new XmlWriter(context.getWriter());

        writer.writeStart("feed",
                "xmlns:m", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
                "xmlns:d", "http://schemas.microsoft.com/ado/2007/08/dataservices",
                "xmlns", "http://www.w3.org/2005/Atom");

            writer.writeStart("id").writeHtml(JspUtils.getAbsoluteUrl(context.getRequest(), "")).writeEnd();

            for (Object entry : result.getItems()) {
                renderAtomEntry(context, entry);
            }

        writer.writeEnd();
    }

    private void renderAtomEntry(WebPageContext context, Object entry) throws IOException {
        @SuppressWarnings("all")
        XmlWriter writer = new XmlWriter(context.getWriter());
        State entryState = State.getInstance(entry);
        ObjectType entryType = entryState.getType();
        String entryUrl = getAbsoluteUrl(context, getEntityTypeName(entryType) + "(guid'" + entryState.getId() + "')");

        writer.writeStart("entry");
            writer.writeStart("id");
                writer.writeHtml(entryUrl);
            writer.writeEnd();

            for (ObjectField field : entryType.getFields()) {
                if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                    String fieldName = field.getInternalName();

                    for (ObjectType child : field.getTypes()) {
                        writer.writeStart("link",
                                "rel", "http://schemas.microsoft.com/ado/2007/08/dataservices/related/" + getEntityTypeName(child),
                                "type", "application/atom+xml;type=entry",
                                "title", fieldName.replace('.', '_'),
                                "href", entryUrl + "/" + fieldName.replace('.', '_'));
                            if (field.isEmbedded()) {
                                writer.writeStart("m:inline");
                                    Object value = entryState.get(fieldName);
                                    if (value != null) {
                                        if (value instanceof Set) {
                                            value = new ArrayList<Object>((Set<?>) value);
                                        }
                                        if (value instanceof List) {
                                            @SuppressWarnings("unchecked")
                                            List<Object> valueList = (List<Object>) value;
                                            renderAtomFeed(context, new PaginatedResult<Object>(0, valueList.size(), valueList));
                                        } else {
                                            renderAtomEntry(context, value);
                                        }
                                    }
                                writer.writeEnd();
                            }
                        writer.writeEnd();
                        break;
                    }
                }
            }

            writer.writeStart("content", "type", "application/xml");
                writer.writeStart("m:properties");
                    writer.writeStart("d:id", "m:type", "Edm.Guid").writeHtml(entryState.getId()).writeEnd();

                    for (ObjectField field : entryType.getFields()) {
                        if (!ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                            String name = field.getInternalName();
                            Object value = entryState.get(name);

                            if (value == null) {
                                writer.writeStart("d:" + name.replace('.', '_'), "m:null", "true").writeEnd();

                            } else {
                                if (value instanceof Date) {
                                    value = ISODateTimeFormat.dateTime().print(new DateTime(value).withZone(DateTimeZone.UTC));
                                }

                                writer.writeStart("d:" + name.replace('.', '_'), "m:type", getEdmType(field));
                                    writer.writeHtml(value);
                                writer.writeEnd();
                            }
                        }
                    }
                writer.writeEnd();
            writer.writeEnd();
        writer.writeEnd();
    }

    private static class XmlWriter extends HtmlWriter {

        public XmlWriter(Writer writer) {
            super(writer);
        }

        @Override
        protected String escapeHtml(String html) {
            StringBuilder escapedBuilder = new StringBuilder();

            for (int i = 0, length = html.length(); i < length; ++ i) {
                char letter = html.charAt(i);

                if (letter == '"') {
                    escapedBuilder.append("&quot;");

                } else if (letter == '\'') {
                    escapedBuilder.append("&apos;");

                } else if (letter == '<') {
                    escapedBuilder.append("&lt;");

                } else if (letter == '>') {
                    escapedBuilder.append("&gt;");

                } else if (letter == '&') {
                    escapedBuilder.append("&amp;");

                } else {
                    escapedBuilder.append(letter);
                }
            }

            return escapedBuilder.toString();
        }
    }
}
