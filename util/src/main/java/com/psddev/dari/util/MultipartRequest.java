package com.psddev.dari.util;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * {@link HttpServletRequest} that's capable of reading
 * {@code multipart/form-data} POST request.
 */
public class MultipartRequest extends HttpServletRequestWrapper {

    private static final String ATTRIBUTE_PREFIX = MultipartRequest.class.getName() + ".";
    private static final String PARAMETERS_ATTRIBUTE = ATTRIBUTE_PREFIX + "parameters";

    private final Map<String, List<FileItem>> parameters;

    /**
     * Creates an instance that wraps the given {@code request}.
     *
     * @param request Can't be {@code null}.
     */
    public MultipartRequest(HttpServletRequest request) throws ServletException {
        super(request);

        // Make sure that the request body is only read once.
        @SuppressWarnings("unchecked")
        Map<String, List<FileItem>> parameters = (Map<String, List<FileItem>>) request.getAttribute(PARAMETERS_ATTRIBUTE);

        if (parameters == null) {
            if (ServletFileUpload.isMultipartContent(request)) {
                parameters = new CompactMap<String, List<FileItem>>();

                try {
                    ServletFileUpload upload = new ServletFileUpload(new DiskFileItemFactory());

                    upload.setHeaderEncoding(StandardCharsets.UTF_8.name());

                    @SuppressWarnings("unchecked")
                    List<FileItem> items = (List<FileItem>) upload.parseRequest(request);

                    for (FileItem item : items) {
                        String name = item.getFieldName();
                        List<FileItem> values = parameters.get(name);

                        if (values == null) {
                            values = new ArrayList<FileItem>();
                            parameters.put(name, values);
                        }

                        values.add(item);
                    }

                } catch (FileUploadException error) {
                    throw new ServletException(error);
                }

            } else {
                parameters = Collections.emptyMap();
            }
        }

        this.parameters = parameters;
    }

    /**
     * Returns the first file item associated with the given {@code name}.
     *
     * @param name May be {@code null}.
     * @return May be {@code null}.
     */
    public FileItem getFileItem(String name) {
        List<FileItem> values = parameters.get(name);

        return values == null || values.isEmpty() ? null : values.get(0);
    }

    /**
     * Returns all file items associated with the given {@code name}.
     *
     * @param name May be {@code null}.
     * @return May be {@code null}. If not {@code null}, never empty.
     */
    public FileItem[] getFileItems(String name) {
        List<FileItem> items = parameters.get(name);

        if (items == null || items.isEmpty()) {
            return null;

        } else {
            return items.toArray(new FileItem[items.size()]);
        }
    }

    // --- HttpServletRequestWrapper support ---

    @Override
    public String getParameter(String name) {
        String value = getRequest().getParameter(name);

        if (value != null) {
            return value;
        }

        List<FileItem> items = parameters.get(name);

        if (items == null || items.isEmpty()) {
            return null;
        }

        FileItem item = items.get(0);

        if (item.isFormField()) {
            try {
                return item.getString(StandardCharsets.UTF_8.name());

            } catch (UnsupportedEncodingException error) {
                throw new IllegalStateException(error);
            }
        }

        return item.getFieldName();
    }

    // Returns a combined set of all query string and form post parameter
    // names.
    private Set<String> getCombinedNames() {
        @SuppressWarnings("unchecked")
        Enumeration<String> requestNames = getRequest().getParameterNames();
        Set<String> names = new LinkedHashSet<String>();

        while (requestNames.hasMoreElements()) {
            names.add(requestNames.nextElement());
        }

        names.addAll(parameters.keySet());

        return names;
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        Map<String, String[]> map = new CompactMap<String, String[]>();

        for (String name : getCombinedNames()) {
            map.put(name, getParameterValues(name));
        }

        return map;
    }

    @Override
    public Enumeration<String> getParameterNames() {
        return Collections.enumeration(getCombinedNames());
    }

    @Override
    public String[] getParameterValues(String name) {
        String[] valuesArray = getRequest().getParameterValues(name);
        List<FileItem> items = parameters.get(name);

        if (items == null || items.isEmpty()) {
            return valuesArray;
        }

        List<String> valuesList = new ArrayList<String>();

        if (valuesArray != null && valuesArray.length > 0) {
            valuesList.addAll(Arrays.asList(valuesArray));
        }

        for (FileItem item : items) {
            if (item.isFormField()) {
                try {
                    valuesList.add(item.getString(StandardCharsets.UTF_8.name()));

                } catch (UnsupportedEncodingException ex) {
                    valuesList.add(item.getFieldName());
                }

            } else {
                valuesList.add(item.getFieldName());
            }
        }

        return valuesList.toArray(new String[valuesList.size()]);
    }
}
