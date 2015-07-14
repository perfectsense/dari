package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// CHECKSTYLE:OFF
/**
 * @see <a href="http://www.w3.org/TR/css3-grid-layout/">CSS Grid Layout</a>
 * @deprecated No replacement.
 */
@Deprecated
public class HtmlGrid {

    private String selector;
    private final List<CssUnit> columns;
    private final List<CssUnit> rows;
    private final List<List<String>> template;
    private final Map<String, String> contexts = new CompactMap<String, String>();

    /**
     * Creates an instance based on the given {@code columns}, {@code rows},
     * and {@code template}.
     *
     * @param columns Can't be blank.
     * @param rows Can't be blank.
     * @param template Can't be blank.
     */
    public HtmlGrid(List<CssUnit> columns, List<CssUnit> rows, List<List<String>> template) {
        ErrorUtils.errorIfBlank(columns, "columns");
        ErrorUtils.errorIfBlank(rows, "rows");
        ErrorUtils.errorIfBlank(template, "template");

        this.columns = columns;
        this.rows = rows;
        this.template = template;
    }

    /**
     * Creates an instance based on the given CSS {@code columnsString}
     * ({@code -dari-grid-definition-columns}), {@code rowsString}
     * ({@code -dari-grid-definition-rows}), and {@code templateStrings}
     * ({@code -dari-grid-template}).
     *
     * @param columnsString Can't be {@code blank}.
     * @param rowsString Can't be {@code blank}.
     * @param templateStrings Can't be {@code blank}.
     */
    public HtmlGrid(String columnsString, String rowsString, String... templateStrings) {
        ErrorUtils.errorIfBlank(columnsString, "columnsString");
        ErrorUtils.errorIfBlank(rowsString, "rowsString");
        ErrorUtils.errorIfBlank(templateStrings, "templateStrings");

        columns = createCssUnits(columnsString);
        rows = createCssUnits(rowsString);
        template = createTemplate(columnsString, rowsString, templateStrings);
    }

    private List<List<String>> createTemplate(String columnsString, String rowsString, String... templateStrings) {
        List<List<String>> template = new ArrayList<List<String>>();

        for (String t : templateStrings) {
            if (t != null && t.length() > 0) {
                for (String line : t.split("[\\r\\n]+")) {
                    line = line.trim();

                    if (line.length() > 0) {
                        List<String> words = Arrays.asList(line.split("\\s+"));
                        int wordsSize = words.size();

                        if (wordsSize > 0) {
                            int lastIndex = wordsSize - 1;
                            String lastWord = words.get(lastIndex);

                            if (lastWord.startsWith("/")) {
                                rows.add(new CssUnit(lastWord.substring(1)));
                                words.remove(lastIndex);
                            }
                        }

                        wordsSize = words.size();
                        int columnsSize = columns.size();

                        if (wordsSize != columnsSize) {
                            throw new IllegalArgumentException(String.format(
                                    "Columns mismatch! [%s] items in [%s] but [%s] in [%s]",
                                    wordsSize, line, columnsSize, columnsString));
                        }

                        template.add(words);
                    }
                }
            }
        }

        int templateSize = template.size();
        int rowsSize = rows.size();

        if (templateSize != rowsSize) {
            StringBuilder t = new StringBuilder();

            for (String templateString : templateStrings) {
                t.append('\n');
                t.append(templateString);
            }

            throw new IllegalArgumentException(String.format(
                    "Rows mismatch! [%s] items in [%s] but [%s] in [%s]",
                    templateSize, t, rowsSize, rowsString));
        }

        return template;
    }

    private List<CssUnit> createCssUnits(String values) {
        List<CssUnit> instances = new ArrayList<CssUnit>();

        if (values != null) {
            for (String value : values.trim().split("\\s+")) {
                instances.add(new CssUnit(value));
            }
        }

        return instances;
    }

    public HtmlGrid(String template) {
        ErrorUtils.errorIfBlank(template, "template");

        Matcher columnsMatcher = Pattern.compile("(?s)\\s*(?:([^/]+)/)?(.*)").matcher(template);

        if (!columnsMatcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "[%s] isn't a valid template!", template));
        }

        String columnsString = columnsMatcher.group(1);
        Matcher rowsMatcher = Pattern.compile("['\\\"]([^'\\\"]+)['\\\"]\\s*([^'\\\"\\s]+)").matcher(columnsMatcher.group(2));
        List<String> templateStrings = new ArrayList<String>();
        StringBuilder rowsStringBuilder = new StringBuilder();

        while (rowsMatcher.find()) {
            templateStrings.add(rowsMatcher.group(1));
            rowsStringBuilder.append(rowsMatcher.group(2));
            rowsStringBuilder.append(' ');
        }

        String rowsString = rowsStringBuilder.toString();
        this.columns = createCssUnits(columnsString);
        this.rows = createCssUnits(rowsString);
        this.template = createTemplate(
                columnsString,
                rowsString,
                templateStrings.toArray(new String[templateStrings.size()]));
    }

    protected String getSelector() {
        return selector;
    }

    public List<CssUnit> getColumns() {
        return columns;
    }

    public List<CssUnit> getRows() {
        return rows;
    }

    public List<List<String>> getTemplate() {
        return template;
    }

    public Map<String, String> getContexts() {
        return contexts;
    }

    private CssCombinedUnit combineNonFractionals(List<CssUnit> units) {
        List<CssUnit> filtered = new ArrayList<CssUnit>();

        for (CssUnit unit : units) {
            if (!"fr".equals(unit.getUnit())) {
                filtered.add(unit);
            }
        }

        return new CssCombinedUnit(filtered);
    }

    public CssCombinedUnit getMinimumWidth() {
        return combineNonFractionals(getColumns());
    }

    public CssCombinedUnit getMinimumHeight() {
        return combineNonFractionals(getRows());
    }

    /** Returns all CSS units used by this template. */
    public Set<String> getCssUnits() {
        Set<String> units = new HashSet<String>();

        for (CssUnit column : getColumns()) {
            units.add(column.getUnit());
        }

        for (CssUnit row : getRows()) {
            units.add(row.getUnit());
        }

        return units;
    }

    /** Returns all area names used by this template. */
    public Set<String> getAreas() {
        Set<String> areas = new LinkedHashSet<String>();

        for (List<String> row : getTemplate()) {
            for (String area : row) {
                if (!".".equals(area)) {
                    areas.add(area);
                }
            }
        }

        return areas;
    }

    public static final class Static {

        private static final Logger LOGGER = LoggerFactory.getLogger(HtmlGrid.class);

        private static final String ATTRIBUTE_PREFIX = HtmlGrid.class.getName() + ".";
        private static final String CSS_MODIFIED_ATTRIBUTE_PREFIX = ATTRIBUTE_PREFIX + "cssModified.";
        private static final String GRID_PATHS_ATTRIBUTE = ATTRIBUTE_PREFIX + "gridPaths";
        private static final String RESTRICT_GRID_PATHS_ATTRIBUTE = ATTRIBUTE_PREFIX + "restrictGridPaths";
        private static final String GRIDS_ATTRIBUTE_PREFIX = ATTRIBUTE_PREFIX + "grids.";
        private static final String REQUEST_GRIDS_ATTRIBUTE = ATTRIBUTE_PREFIX + "requestGrids";

        private static final String DISPLAY_GRID_VALUE = "-dari-grid";
        private static final String TEMPLATE_PROPERTY = "-dari-grid-template";
        private static final String COLUMNS_PROPERTY = "-dari-grid-definition-columns";
        private static final String ROWS_PROPERTY = "-dari-grid-definition-rows";
        private static final String CONTEXTS_PROPERTY = "-dari-grid-contexts";

        public static void setRestrictGridPaths(List<String> restrictedGridPaths, ServletContext context) {
            if (restrictedGridPaths != null && !restrictedGridPaths.isEmpty()) {
                context.setAttribute(RESTRICT_GRID_PATHS_ATTRIBUTE, restrictedGridPaths);
            } else {
                context.removeAttribute(RESTRICT_GRID_PATHS_ATTRIBUTE);
            }
        }

        public static Map<String, HtmlGrid> findAll(ServletContext context) throws IOException {
            return findGrids(context, null, findGridPaths(context));
        }

        public static void addStyleSheet(HttpServletRequest request, String path) {
            @SuppressWarnings("unchecked")
            List<String> paths = (List<String>) request.getAttribute(GRID_PATHS_ATTRIBUTE);

            if (paths == null) {
                paths = new ArrayList<String>();
                request.setAttribute(GRID_PATHS_ATTRIBUTE, paths);
            }

            paths.add(0, path);
        }

        /**
         * @deprecated Temporary. Do not use.
         */
        @Deprecated
        public static void addRemoteStyleSheet(
                ServletContext context,
                HttpServletRequest request,
                String path) {

            @SuppressWarnings("unchecked")
            List<String> requestPaths = (List<String>) request.getAttribute(GRID_PATHS_ATTRIBUTE);

            @SuppressWarnings("unchecked")
            List<String> contextPaths = (List<String>) context.getAttribute(GRID_PATHS_ATTRIBUTE);

            try {
                URI pathUri = new URI(path);
                List<String> externalPaths = null;

                if (pathUri.isAbsolute()) {
                    URL pathUrl = pathUri.toURL();
                    externalPaths = new ArrayList<String>();

                    // Ensure that local paths are populated first.
                    if (contextPaths == null) {
                        contextPaths = findGridPaths(context);
                    }

                    // Populate ServletContext cache if empty.
                    if (path.endsWith(".less") || path.endsWith(".css")) {
                        if (context.getAttribute(GRIDS_ATTRIBUTE_PREFIX + path) == null) {
                            parseGridCss(context, path, externalPaths, pathUrl.openConnection());

                        } else {
                            externalPaths.add(path);
                        }
                    }

                    // Add to list of ServletContext paths if necessary (non-prod).
                    if (!contextPaths.contains(path)) {
                        contextPaths.add(path);
                    }

                    if (requestPaths == null) {
                        requestPaths = new ArrayList<String>();
                        request.setAttribute(GRID_PATHS_ATTRIBUTE, requestPaths);
                    }
                }

                if (externalPaths != null) {
                    for (String externalPath : externalPaths) {
                        if (!requestPaths.contains(externalPath)) {
                            requestPaths.add(0, externalPath);
                        }
                    }
                }

            } catch (MalformedURLException e) {
                // Ignore.
            } catch (IOException e) {
                // Ignore.
            } catch (URISyntaxException e) {
                // Ignore.
            }
        }

        /**
         * Adds the given {@code grid} definition and associates it with the
         * given {@code selector} so that it's valid for use in the given
         * {@code request}.
         *
         * @param request Can't be {@code null}.
         * @param selector Can't be blank.
         * @param grid Can't be {@code null}.
         */
        public static void addGrid(HttpServletRequest request, String selector, HtmlGrid grid) {
            ErrorUtils.errorIfBlank(selector, "selector");
            ErrorUtils.errorIfNull(grid, "grid");

            @SuppressWarnings("unchecked")
            Map<String, HtmlGrid> grids = (Map<String, HtmlGrid>) request.getAttribute(REQUEST_GRIDS_ATTRIBUTE);

            if (grids == null) {
                grids = new CompactMap<String, HtmlGrid>();
                request.setAttribute(REQUEST_GRIDS_ATTRIBUTE, grids);
            }

            grids.put(selector, grid);
        }

        public static Map<String, HtmlGrid> findAll(ServletContext context, HttpServletRequest request) throws IOException {
            @SuppressWarnings("unchecked")
            List<String> usedPaths = request != null ? (List<String>) request.getAttribute(GRID_PATHS_ATTRIBUTE) : null;
            List<String> gridPaths = findGridPaths(context);

            return findGrids(context, request, usedPaths == null || usedPaths.isEmpty() ? gridPaths : usedPaths);
        }

        /** @deprecated Use {@link #findAll} instead. */
        @Deprecated
        public static HtmlGrid find(ServletContext context, String cssClass) throws IOException {
            return ObjectUtils.isBlank(cssClass) ? null : findAll(context).get("." + cssClass);
        }

        @SuppressWarnings("unchecked")
        private static List<String> findGridPaths(ServletContext context) throws IOException {
            List<String> gridPaths = null;

            List<String> restrictGridPaths = (List<String>) context.getAttribute(RESTRICT_GRID_PATHS_ATTRIBUTE);
            if (restrictGridPaths != null) {
                gridPaths = new ArrayList<String>();
                for (String path : restrictGridPaths) {
                    URLConnection cssConnection = CodeUtils.getResource(context, path).openConnection();
                    parseGridCss(context, path, gridPaths, cssConnection);
                }
                context.setAttribute(GRID_PATHS_ATTRIBUTE, gridPaths);
            }

            if (gridPaths == null && Settings.isProduction()) {
                gridPaths = (List<String>) context.getAttribute(GRID_PATHS_ATTRIBUTE);
            }

            if (gridPaths == null) {
                gridPaths = new ArrayList<String>();

                findGridPathsNamed(context, "/", gridPaths, ".less");
                findGridPathsNamed(context, "/", gridPaths, ".css");
                context.setAttribute(GRID_PATHS_ATTRIBUTE, gridPaths);
            }

            return gridPaths;
        }

        private static void findGridPathsNamed(
                ServletContext context,
                String path,
                List<String> gridPaths,
                String suffix)
                throws IOException {

            Set<String> children = CodeUtils.getResourcePaths(context, path);

            if (children == null) {
                return;
            }

            for (String child : children) {
                if (child.endsWith("/")) {
                    findGridPathsNamed(context, child, gridPaths, suffix);

                } else if (child.endsWith(suffix)) {
                    URLConnection cssConnection = CodeUtils.getResource(context, child).openConnection();

                    parseGridCss(context, child, gridPaths, cssConnection);
                }
            }
        }

        private static void parseGridCss(
                ServletContext context,
                String path,
                List<String> gridPaths,
                URLConnection cssConnection)
                throws IOException {

            String modifiedAttr = CSS_MODIFIED_ATTRIBUTE_PREFIX + path;
            InputStream cssInput = cssConnection.getInputStream();

            gridPaths.add(path);

            try {
                Long oldModified = (Long) context.getAttribute(modifiedAttr);
                long cssModified = cssConnection.getLastModified();

                if (oldModified != null && oldModified == cssModified) {
                    return;
                }

                LOGGER.debug("Reading stylesheet [{}] modified [{}]", path, cssModified);

                Css css = new Css(IoUtils.toString(cssInput, StringUtils.UTF_8));
                Map<String, HtmlGrid> grids = new CompactMap<String, HtmlGrid>();

                for (CssRule rule : css.getRules()) {
                    String display = rule.getValue("display");

                    if (!(DISPLAY_GRID_VALUE.equals(display) ||
                            "grid".equals(display))) {
                        continue;
                    }

                    String selector = rule.getSelector();
                    LOGGER.debug("Found grid matching [{}] in [{}]", selector, path);

                    String templateValue = rule.getValue(TEMPLATE_PROPERTY);

                    if (ObjectUtils.isBlank(templateValue)) {
                        templateValue = rule.getValue("grid-template");
                    }

                    if (ObjectUtils.isBlank(templateValue)) {
                        throw new IllegalStateException(String.format(
                                "Path: [%s], Selector: [%s], Missing [%s]!",
                                path, selector, TEMPLATE_PROPERTY));
                    }

                    String columnsValue = rule.getValue(COLUMNS_PROPERTY);

                    if (ObjectUtils.isBlank(columnsValue)) {
                        columnsValue = rule.getValue("grid-definition-columns");
                    }

                    if (ObjectUtils.isBlank(columnsValue)) {
                        throw new IllegalStateException(String.format(
                                "Path: [%s], Selector: [%s], Missing [%s]!",
                                path, selector, COLUMNS_PROPERTY));
                    }

                    String rowsValue = rule.getValue(ROWS_PROPERTY);

                    if (ObjectUtils.isBlank(rowsValue)) {
                        rowsValue = rule.getValue("grid-definition-rows");
                    }

                    if (ObjectUtils.isBlank(rowsValue)) {
                        throw new IllegalStateException(String.format(
                                "Path: [%s], Selector: [%s], Missing [%s]!",
                                path, selector, ROWS_PROPERTY));
                    }

                    char[] letters = templateValue.toCharArray();
                    StringBuilder word = new StringBuilder();
                    List<String> list = new ArrayList<String>();

                    for (int i = 0, length = letters.length; i < length; ++ i) {
                        char letter = letters[i];

                        if (letter == '"') {
                            for (++ i; i < length; ++ i) {
                                letter = letters[i];

                                if (letter == '"') {
                                    list.add(word.toString());
                                    word.setLength(0);
                                    break;

                                } else {
                                    word.append(letter);
                                }
                            }

                        } else if (Character.isWhitespace(letter)) {
                            if (word.length() > 0) {
                                list.add(word.toString());
                                word.setLength(0);
                            }

                        } else {
                            word.append(letter);
                        }
                    }

                    StringBuilder t = new StringBuilder();

                    for (String v : list) {
                        t.append(v);
                        t.append('\n');
                    }

                    try {
                        HtmlGrid grid = new HtmlGrid(
                                columnsValue,
                                rowsValue,
                                t.toString());

                        grid.selector = selector;
                        Map<String, String> contexts = grid.contexts;
                        String contextsString = rule.getValue(CONTEXTS_PROPERTY);

                        if (!ObjectUtils.isBlank(contextsString)) {
                            String[] entries = contextsString.trim().split("\\s+");

                            for (int i = 1, length = entries.length; i < length; i += 2) {
                                contexts.put(entries[i - 1], entries[i]);
                            }
                        }

                        grids.put(selector, grid);

                    } catch (IllegalArgumentException error) {
                        throw new IllegalArgumentException(String.format(
                                "Path: [%s], Selector: [%s], %s",
                                path, selector, error.getMessage()));
                    }
                }

                context.setAttribute(modifiedAttr, cssModified);
                context.setAttribute(GRIDS_ATTRIBUTE_PREFIX + path, grids);

            } finally {
                cssInput.close();
            }
        }

        private static Map<String, HtmlGrid> findGrids(ServletContext context, HttpServletRequest request, List<String> gridPaths) {
            Map<String, HtmlGrid> all = new CompactMap<String, HtmlGrid>();

            for (int i = gridPaths.size() - 1; i >= 0; -- i) {
                String gridPath = gridPaths.get(i);
                @SuppressWarnings("unchecked")
                Map<String, HtmlGrid> grids = (Map<String, HtmlGrid>) context.getAttribute(GRIDS_ATTRIBUTE_PREFIX + gridPath);

                if (grids != null) {
                    all.putAll(grids);
                }
            }

            if (request != null) {
                @SuppressWarnings("unchecked")
                Map<String, HtmlGrid> grids = (Map<String, HtmlGrid>) request.getAttribute(REQUEST_GRIDS_ATTRIBUTE);

                if (grids != null) {
                    all.putAll(grids);
                }
            }

            return all;
        }
    }
}
