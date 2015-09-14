package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** Debug servlet for inspecting application build information. */
@DebugFilter.Path("build")
@SuppressWarnings("serial")
public class BuildDebugServlet extends HttpServlet {

    public static final String PROPERTIES_FILE_NAME = "build.properties";
    public static final String PROPERTIES_FILE = "/WEB-INF/classes/" + PROPERTIES_FILE_NAME;
    public static final String LIB_PATH = "/WEB-INF/lib";
    private static final String GIT_CONNECTION_PREFIX = "scm:git:";
    private static final Cache<String, Properties> PROPERTIES_CACHE = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();

    /** Returns all the properties in the build file. */
    public static Properties getProperties(ServletContext context) throws IOException {
        return getEmbeddedProperties(context, null);
    }

    /** Returns all the properties in the build file of an embedded war file. */
    public static synchronized Properties getEmbeddedProperties(ServletContext context, String embeddedPath) throws IOException {
        embeddedPath = embeddedPath != null ? embeddedPath : "";
        Properties build = PROPERTIES_CACHE.getIfPresent(embeddedPath);
        if (build == null) {
            build = new Properties();
            InputStream stream = context.getResourceAsStream(embeddedPath + PROPERTIES_FILE);
            if (stream != null) {
                try {
                    build.load(stream);
                } finally {
                    stream.close();
                }
            }
            PROPERTIES_CACHE.put(embeddedPath, build);
        }
        return build;
    }

    /** Returns all the properties in the build file of an embedded jar file. */
    public static synchronized Properties getEmbeddedPropertiesInJar(ServletContext context, String jarResource) throws IOException {
        Properties build = PROPERTIES_CACHE.getIfPresent(jarResource);
        if (build == null) {
            build = new Properties();
            InputStream inputStream = context.getResourceAsStream(jarResource);
            if (inputStream != null) {
                try {
                    JarInputStream jarStream = new JarInputStream(inputStream);
                    if (jarStream != null) {
                        try {
                            JarEntry entry = null;
                            while ((entry = jarStream.getNextJarEntry()) != null) {
                                if (PROPERTIES_FILE_NAME.equals(entry.getName())) {
                                    byte[] buffer = new byte[4096];
                                    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
                                    int read = 0;
                                    while ((read = jarStream.read(buffer)) != -1) {
                                        outputBytes.write(buffer, 0, read);
                                    }
                                    InputStream propertiesFileInputStream = new ByteArrayInputStream(outputBytes.toByteArray());
                                    if (propertiesFileInputStream != null) {
                                        try {
                                            build.load(propertiesFileInputStream);
                                            break;
                                        } finally {
                                            propertiesFileInputStream.close();
                                        }
                                    }
                                }
                            }
                        } finally {
                            jarStream.close();
                        }
                    }
                } finally {
                    inputStream.close();
                }
            }
            PROPERTIES_CACHE.put(jarResource, build);
        }
        return build;
    }

    /**
     * Returns a descriptive label that represents the build within the
     * given {@code context}.
     */
    public static String getLabel(ServletContext context) {
        Properties build = null;
        try {
            build = getProperties(context);
        } catch (IOException error) {
            // If the build properties can't be read, pretend it's empty so
            // that the default label can be returned.
        }
        if (build == null) {
            build = new Properties();
        }
        return getLabel(build);
    }

    // Returns a descriptive label using the given properties.
    private static String getLabel(Properties properties) {
        String title = properties.getProperty("name");
        if (ObjectUtils.isBlank(title)) {
            title = "Anonymous Application";
        }
        String version = properties.getProperty("version");
        if (!ObjectUtils.isBlank(version)) {
            title += ": " + version;
        }
        String buildNumber = properties.getProperty("buildNumber");
        if (!ObjectUtils.isBlank(buildNumber)) {
            title += " build " + buildNumber;
        }
        return title;
    }

    @Override
    protected void doGet(
            HttpServletRequest request,
            final HttpServletResponse response)
            throws IOException, ServletException {

        final String buildContext = request.getParameter("context");
        final String format = request.getParameter("format");
        final boolean showCommits = ObjectUtils.to(Boolean.class, request.getParameter("commits") != null ? request.getParameter("commits") : "true");

        new DebugFilter.PageWriter(getServletContext(), request, response) {

            final Map<String, Properties> embeddedProperties = getAllEmbeddedProperties();
            final Properties build = getBuild(embeddedProperties);

            final String issueSystem = build.getProperty("issueManagementSystem");
            final String issueUrl = build.getProperty("issueManagementUrl");
            Pattern issuePattern = null;
            String issueUrlFormat = null;
            Map<String, List<GitCommit>> commitsMap;

            {

                if ("JIRA".equals(issueSystem)) {
                    String prefix = "/browse/";
                    int prefixAt = issueUrl.indexOf(prefix);
                    if (prefixAt > -1) {
                        prefixAt += prefix.length();
                        int slashAt = issueUrl.indexOf('/', prefixAt);
                        String jiraId = slashAt > -1
                                ? issueUrl.substring(prefixAt, slashAt)
                                : issueUrl.substring(prefixAt);
                        issuePattern = Pattern.compile("\\Q" + jiraId + "\\E-\\d+");
                        issueUrlFormat = issueUrl.substring(0, prefixAt) + "%s";
                    }
                }

                commitsMap = getCommits(build);

                if ("text".equals(format)) {
                    response.setContentType("text/plain");
                    writeTextPage();
                } else if ("json".equals(format)) {
                    response.setContentType("application/json");
                    writeJsonPage();
                } else {
                    writeHtmlPage();
                }

            }

            private void writeTextPage() throws IOException {

                Properties contextBuildProperties = getProperties(getServletContext());
                writeRaw("Context: application\n");
                writeRaw("Name: ");
                writeRaw(contextBuildProperties.get("name") != null ? contextBuildProperties.getProperty("name") : "Anonymous Application");
                writeRaw("\n");
                writeRaw("Build Number: ");
                writeRaw(contextBuildProperties.getProperty("buildNumber"));
                writeRaw("\n");
                writeRaw("Build Date: ");
                writeRaw(contextBuildProperties.getProperty("buildDate"));
                writeRaw("\n");
                writeRaw("Version: ");
                writeRaw(contextBuildProperties.getProperty("version"));
                writeRaw("\n");
                writeRaw("\n");

                for (Map.Entry<String, Properties> entry : embeddedProperties.entrySet()) {
                    writeRaw("Context: ");
                    writeRaw(entry.getKey());
                    writeRaw("\n");
                    writeRaw("Name: ");
                    writeRaw(entry.getValue().get("name") != null ? entry.getValue().getProperty("name") : "Anonymous Application");
                    writeRaw("\n");
                    writeRaw("Build Number: ");
                    writeRaw(entry.getValue().getProperty("buildNumber"));
                    writeRaw("\n");
                    writeRaw("Build Date: ");
                    writeRaw(entry.getValue().getProperty("buildDate"));
                    writeRaw("\n");
                    writeRaw("Version: ");
                    writeRaw(entry.getValue().getProperty("version"));
                    writeRaw("\n");
                    writeRaw("\n");
                }
                writeRaw("\n");

                if (showCommits) {
                    writeRaw("Commits for ");
                    writeRaw(build.getProperty("name") != null ? build.getProperty("name") : "Anonymous Application");
                    writeRaw("\n");
                    for (Map.Entry<String, List<GitCommit>> entry : commitsMap.entrySet()) {
                        boolean first = true;
                        for (GitCommit commit : entry.getValue()) {

                            writeRaw(commit.hash.substring(0, 6));
                            writeRaw(" - ");
                            if (first) {
                                first = false;
                                writeRaw("(");
                                writeRaw(entry.getKey());
                                writeRaw(") ");
                            }
                            writeRaw(commit.subject);
                            writeRaw(" (");
                            writeRaw(commit.date);
                            writeRaw(")");
                            writeRaw(" <");
                            writeRaw(commit.author);
                            writeRaw(">");
                            writeRaw("\n");
                        }
                    }
                    writeRaw("\n");
                }

            }

            private void writeJsonPage() throws IOException {
                Map<String, Object> result = new CompactMap<String, Object>();

                Map<String, Object> builds = new CompactMap<String, Object>();
                Map<String, Object> contextBuild = new CompactMap<String, Object>();
                Properties contextBuildProperties = getProperties(getServletContext());
                contextBuild.put("name", contextBuildProperties.get("name"));
                contextBuild.put("buildNumber", contextBuildProperties.get("buildNumber"));
                contextBuild.put("buildDate", contextBuildProperties.get("buildDate"));
                contextBuild.put("version", contextBuildProperties.get("version"));
                builds.put("application", contextBuild);
                for (Map.Entry<String, Properties> entry : embeddedProperties.entrySet()) {
                    Map<String, Object> build = new CompactMap<String, Object>();
                    build.put("name", entry.getValue().get("name"));
                    build.put("buildNumber", entry.getValue().get("buildNumber"));
                    build.put("buildDate", entry.getValue().get("buildDate"));
                    build.put("version", entry.getValue().get("version"));
                    builds.put(entry.getKey(), build);
                }
                result.put("builds", builds);

                if (showCommits) {
                    List<Map<String, String>> commits = new ArrayList<Map<String, String>>();
                    for (Map.Entry<String, List<GitCommit>> entry : commitsMap.entrySet()) {
                        boolean first = true;
                        for (GitCommit commit : entry.getValue()) {
                            Map<String, String> commitEntry = new CompactMap<String, String>();
                            commits.add(commitEntry);
                            commitEntry.put("hash", commit.hash);
                            if (first) {
                                first = false;
                                commitEntry.put("refs", entry.getKey());
                            }
                            commitEntry.put("subject", commit.subject);
                            commitEntry.put("body", StringUtils.removeSurrounding(commit.body, "\""));
                            commitEntry.put("date", commit.date.toString());
                            commitEntry.put("author", commit.author);
                        }
                    }
                    result.put("commits", commits);
                }

                writeRaw(ObjectUtils.toJson(result));
            }

            private void writeHtmlPage() throws IOException {
                startPage("Build Information");

                    writeStart("style", "type", "text/css");
                        write("tr.merge { color: rgba(0, 0, 0, 0.3); }");
                        write("td.num { text-align: right; }");
                        write("td:not(.wrap) { white-space: nowrap; }");
                    writeEnd();

                    writeStart("script");
                        write("$(document).ready(function(){");
                            write("$('#contextPicker').change(function() {");
                                write("this.form.submit();");
                            write("});");
                        write("});");
                    writeEnd();

                    Map<String, Properties> embeddedProperties = getAllEmbeddedProperties();
                    Properties build = getBuild(embeddedProperties);

                    String scmUrlFormat = null;
                    String scmConnection = build.getProperty("scmConnection");
                    if (ObjectUtils.isBlank(scmConnection)) {
                        scmConnection = build.getProperty("scmDeveloperConnection");
                    }
                    if (!ObjectUtils.isBlank(scmConnection)) {
                        if (scmConnection.startsWith(GIT_CONNECTION_PREFIX)) {

                            scmUrlFormat = build.getProperty("scmUrl");

                            try {
                                URI uri = new URI(scmConnection.substring(GIT_CONNECTION_PREFIX.length()));

                                if (uri.getHost() != null && uri.getHost().toLowerCase().endsWith("bitbucket.org")) {
                                    scmUrlFormat += "/commits/%s"; // bitbucket.org uses "/commits/"
                                } else {
                                    scmUrlFormat += "/commit/%s"; // github.com uses "/commit/"
                                }

                            } catch (URISyntaxException e) {
                                scmUrlFormat += "/commit/%s";
                            }
                        }
                    }

                    writeStart("h2").writeHtml("Commits").writeEnd();

                    writeStart("form", "action", "", "method", "GET", "class", "form-inline");
                        writeHtml("For: ");
                        writeStart("select", "style", "width:auto;", "id", "contextPicker", "name", "context", "class", "input-xlarge");
                            writeStart("option", "value", "");
                                writeHtml(getLabel(getServletContext()));
                            writeEnd();
                            for (Map.Entry<String, Properties> entry : embeddedProperties.entrySet()) {
                                writeStart("option", "value", entry.getKey(), "selected", entry.getKey().equals(buildContext) ? "selected" : null);
                                    writeHtml(getLabel(entry.getValue()));
                                writeEnd();
                            }
                        writeEnd();
                    writeEnd();

                    if (commitsMap.isEmpty()) {
                        writeStart("p", "class", "alert");
                            writeHtml("Not available!");
                        writeEnd();

                    } else {
                        int colspan = 3;

                        writeStart("table", "class", "table table-condensed table-striped");
                            writeStart("thead");
                                writeStart("tr");

                                    writeStart("th").writeHtml("Date").writeEnd();

                                    if (issuePattern != null) {
                                        writeStart("th").writeHtml("Issues").writeEnd();
                                        ++ colspan;
                                    }

                                    writeStart("th").writeHtml("Author").writeEnd();
                                    writeStart("th").writeHtml("Subject").writeEnd();

                                    if (scmUrlFormat != null) {
                                        writeStart("th").writeHtml("SCM").writeEnd();
                                        ++ colspan;
                                    }
                                writeEnd();
                            writeEnd();

                            writeStart("tbody");
                                for (Map.Entry<String, List<GitCommit>> entry : commitsMap.entrySet()) {

                                    writeStart("tr");
                                        writeStart("td", "class", "wrap", "colspan", colspan);
                                            writeStart("strong").writeHtml(entry.getKey()).writeEnd();
                                        writeEnd();
                                    writeEnd();

                                    for (GitCommit commit : entry.getValue()) {
                                        writeStart("tr", "class", commit.subject != null && commit.subject.startsWith("Merge branch ") ? "merge" : null);

                                            writeStart("td").writeHtml(commit.date).writeEnd();

                                            if (issuePattern != null) {
                                                writeStart("td");
                                                    for (String issue : commit.issues) {
                                                        if (issueUrlFormat != null) {
                                                            writeStart("a", "href", String.format(issueUrlFormat, issue), "target", "_blank");
                                                                writeHtml(issue);
                                                            writeEnd();
                                                        } else {
                                                            writeHtml(issue);
                                                        }
                                                        writeElement("br");
                                                    }
                                                writeEnd();
                                            }

                                            writeStart("td").writeHtml(commit.author).writeEnd();
                                            writeStart("td", "class", "wrap").writeHtml(commit.subject).writeEnd();

                                            if (scmUrlFormat != null) {
                                                writeStart("td");
                                                    writeStart("a", "href", String.format(scmUrlFormat, commit.hash), "target", "_blank");
                                                        writeHtml(commit.hash.substring(0, 6));
                                                    writeEnd();
                                                writeEnd();
                                            }
                                        writeEnd();
                                    }
                                }
                            writeEnd();
                        writeEnd();
                    }

                    writeStart("h2").writeHtml("Resources").writeEnd();
                    writeStart("table", "class", "table table-condensed");
                        writeStart("thead");
                            writeStart("tr");
                                writeStart("th").writeHtml("Path").writeEnd();
                                writeStart("th").writeHtml("Size (Bytes)").writeEnd();
                                writeStart("th").writeHtml("MD5").writeEnd();
                            writeEnd();
                        writeEnd();
                        writeStart("tbody");
                            writeResourcesOfPath("", 0, "/");
                        writeEnd();
                    writeEnd();

                endPage();
            }

            private void writeResourcesOfPath(String parentPath, int depth, String path) throws IOException {
                writeStart("tr");
                writeStart("td", "style", "padding-left: " + (depth * 20) + "px").writeHtml(path).writeEnd();

                if (path.endsWith("/")) {
                    writeStart("td").writeEnd();
                    writeStart("td").writeEnd();

                    List<String> subPaths = new ArrayList<String>();

                    @SuppressWarnings("unchecked")
                    Set<String> resourcePaths = (Set<String>) getServletContext().getResourcePaths(path);
                    if (resourcePaths != null) {
                        subPaths.addAll(resourcePaths);
                    }
                    Collections.sort(subPaths);

                    int subDepth = depth + 1;
                    for (String subPath : subPaths) {
                        writeResourcesOfPath(path, subDepth, subPath);
                    }

                    writeEnd();

                } else {
                    MessageDigest md5;

                    try {
                        md5 = MessageDigest.getInstance("MD5");
                    } catch (NoSuchAlgorithmException error) {
                        throw new IllegalStateException(error);
                    }

                    try {
                        InputStream input = getServletContext().getResourceAsStream(path);

                        if (input != null) {
                            try {
                                input = new DigestInputStream(input, md5);
                                int totalBytesRead = 0;
                                int bytesRead = 0;
                                byte[] buffer = new byte[4096];

                                while ((bytesRead = input.read(buffer)) > 0) {
                                    totalBytesRead += bytesRead;
                                }

                                writeStart("td", "class", "num").writeObject(totalBytesRead).writeEnd();
                                writeStart("td");
                                    write(StringUtils.hex(md5.digest()));
                                writeEnd();

                            } finally {
                                input.close();
                            }
                        }

                    } catch (IOException error) {
                        writeObject(error);
                    }

                    writeEnd();
                }
            }

            private Map<String, Properties> getAllEmbeddedProperties() throws IOException {
                Map<String, Properties> embeddedProperties = new CompactMap<String, Properties>();
                @SuppressWarnings("unchecked")
                Set<String> paths = (Set<String>) getServletContext().getResourcePaths("/");
                if (paths != null) {
                    for (String path : paths) {
                        if (path.endsWith("/")) {
                            path = path.substring(0, path.length() - 1);
                            Properties properties = getEmbeddedProperties(getServletContext(), path);
                            if (!properties.isEmpty()) {
                                embeddedProperties.put(path.substring(1), properties);
                            }
                        }
                    }
                }

                @SuppressWarnings("unchecked")
                Set<String> libJars = (Set<String>) getServletContext().getResourcePaths(LIB_PATH);
                for (Object jar : libJars) {
                    Properties properties = getEmbeddedPropertiesInJar(getServletContext(), (String) jar);
                    if (!properties.isEmpty()) {
                        embeddedProperties.put((String) jar, properties);
                    }
                }

                return embeddedProperties;
            }

            private Properties getBuild(Map<String, Properties> embeddedProperties) throws IOException {

                Properties build = null;
                if (embeddedProperties.containsKey(buildContext)) {
                    build = embeddedProperties.get(buildContext);
                } else {
                    build = getProperties(getServletContext());
                }
                return build;
            }

            private Map<String, List<GitCommit>> getCommits(Properties build) {

                String commitsString = build.getProperty("gitCommits");
                Map<String, List<GitCommit>> commitsMap = new CompactMap<String, List<GitCommit>>();
                if (!ObjectUtils.isBlank(commitsString)) {
                    String currRefNames = null;
                    for (String e : StringUtils.split(commitsString, "(?m)\\s*~-~\\s*")) {
                        GitCommit commit = new GitCommit(e, issuePattern);
                        String refNames = commit.refNames;
                        if (!ObjectUtils.isBlank(refNames)) {
                            if (refNames.startsWith("(")) {
                                refNames = refNames.substring(1);
                            }
                            if (refNames.endsWith(")")) {
                                refNames = refNames.substring(
                                        0, refNames.length() - 1);
                            }
                            currRefNames = refNames;
                        }
                        List<GitCommit> commits = commitsMap.get(currRefNames);
                        if (commits == null) {
                            commits = new ArrayList<GitCommit>();
                            commitsMap.put(currRefNames, commits);
                        }
                        commits.add(commit);
                    }
                }
                return commitsMap;
            }

        };
    }

    private static class GitCommit {

        public String hash;
        public String author;
        public Date date;
        public String refNames;
        public String subject;
        public String body;
        public List<String> issues;

        public GitCommit(String line, Pattern issuePattern) {

            String[] items = StringUtils.split(line, "(?m)\\s*~\\|~\\s*");
            hash = items[0];
            author = items.length > 1 ? items[1] : null;
            if (items.length > 2) {
                Long timestamp = ObjectUtils.to(Long.class, items[2]);
                if (timestamp != null) {
                    date = new Date(timestamp * 1000);
                }
            }

            refNames = items.length > 3 ? items[3] : null;
            subject = items.length > 4 ? items[4] : null;
            body = items.length > 5 ? items[5] : null;

            if (issuePattern != null) {
                issues = new ArrayList<String>();
                for (String e : new String[] { subject, body }) {
                    if (e != null) {
                        Matcher matcher = issuePattern.matcher(e);
                        while (matcher.find()) {
                            issues.add(matcher.group(0));
                        }
                    }
                }
            }
        }
    }
}
