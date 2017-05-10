package com.psddev.dari.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.servlet.jsp.JspFactory;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** JSP utility methods. */
public final class JspUtils {

    private static final Pattern ABSOLUTE_URI_PATTERN = Pattern.compile("(?i)[a-z][-a-z0-9+.]*:.*");
    private static final Logger LOGGER = LoggerFactory.getLogger(JspUtils.class);
    private static final String WEB_INF_DIRECTORY = "WEB-INF/";

    private static final String ATTRIBUTE_PREFIX = JspUtils.class.getName() + ".";
    private static final String EMBEDDED_SETTINGS_ATTRIBUTE = ATTRIBUTE_PREFIX + "embeddedSettings";
    private static final String EMBEDDED_CONTEXT_PATHS = ATTRIBUTE_PREFIX + "embeddedContextPaths";
    private static final String HEADER_RESPONSE_ATTRIBUTE = ATTRIBUTE_PREFIX + "headerResponse";
    private static final String ID_ATTRIBUTE = ATTRIBUTE_PREFIX + "id";
    private static final String IS_FINISHED_ATTRIBUTE = ATTRIBUTE_PREFIX + "isFinished";

    /**
     * Controls access using the basic authentication scheme described
     * in <a href="http://tools.ietf.org/html/rfc2617">RFC 2617</a>.
     * Note that if running in non-{@linkplain Settings#isProduction
     * production environment} and the given {@code username} and
     * {@code password} is blank, this method will return {@code true}.
     * Typical use looks like:
     *
     * <p><blockquote><pre>
     * if (!JspUtils.authenticateBasic(request, response, realm, username, password)) {
     * &nbsp;   return; // Should not send anything else to the response.
     * }
     * </pre></blockquote>
     *
     * @deprecated Use {@link #getBasicCredentials} and {@link #setBasicAuthenticationHeader} instead.
     */
    @Deprecated
    public static boolean authenticateBasic(
            HttpServletRequest request,
            HttpServletResponse response,
            String realm,
            String username,
            String password) {

        if (ObjectUtils.isBlank(username)
                && ObjectUtils.isBlank(password)
                && !Settings.isProduction()) {
            return true;
        }

        String[] credentials = getBasicCredentials(request);

        if (credentials != null
                && credentials[0].equals(username)
                && credentials[1].equals(password)) {
            return true;

        } else {
            setBasicAuthenticationHeader(response, realm);
            return false;
        }
    }

    /**
     * Returns the basic access authentication credentials from the
     * {@code Authorization} header in the given {@code request}.
     *
     * @param request
     *        Can't be {@code null}.
     *
     * @return {@code null} if not found, or a 2-element string array
     *         containing the username and the password.
     */
    public static String[] getBasicCredentials(HttpServletRequest request) {
        Preconditions.checkNotNull(request);

        String header = request.getHeader("Authorization");

        if (!ObjectUtils.isBlank(header)) {
            int spaceAt = header.indexOf(' ');

            if (spaceAt > -1 && "Basic".equals(header.substring(0, spaceAt))) {
                try {
                    String encoding = request.getCharacterEncoding();
                    Charset charset = ObjectUtils.isBlank(encoding)
                            ? StandardCharsets.UTF_8
                            : Charset.forName(encoding);

                    String decoded = new String(
                            Base64.getDecoder().decode(header.substring(spaceAt + 1).getBytes(charset)),
                            charset);

                    if (!ObjectUtils.isBlank(decoded)) {
                        int colonAt = decoded.indexOf(':');

                        if (colonAt > -1) {
                            return new String[] {
                                    decoded.substring(0, colonAt),
                                    decoded.substring(colonAt + 1) };
                        }
                    }

                } catch (IllegalArgumentException error) {
                    // Not a valid Base64 string, so just ignore the header
                    // value.
                }
            }
        }

        return null;
    }

    /**
     * Sets the {@code WWW-Authenticate} header requiring basic access
     * authentication in the given {@code realm}.
     *
     * @param response Can't be {@code null}.
     * @param realm May be @{code null}.
     */
    public static void setBasicAuthenticationHeader(HttpServletResponse response, String realm) {
        StringBuilder header = new StringBuilder();

        header.append("Basic realm=\"");

        if (realm != null) {
            header.append(realm.replace("\"", "\\\""));
        }

        header.append('"');
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setHeader("WWW-Authenticate", header.toString());
    }

    /**
     * Creates a cookie signature based on the given {@code name},
     * {@code value}, and {@code timestamp}.
     */
    private static String createCookieSignature(
            String name,
            String value,
            long timestamp) {

        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");

            sha1.update(name.getBytes(StandardCharsets.UTF_8));
            sha1.update(value.getBytes(StandardCharsets.UTF_8));
            sha1.update(Long.valueOf(timestamp).byteValue());
            sha1.update(Settings.getSecret().getBytes(StandardCharsets.UTF_8));
            return StringUtils.hex(sha1.digest());

        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("Can't hash using SHA-1!", error);
        }
    }

    /**
     * Creates a new unique ID that can be used to identify anything
     * within the given {@code request}.
     *
     * @see #getId
     */
    public static String createId(ServletRequest request) {
        String id = "i" + UUID.randomUUID().toString().replace("-", "");
        request.setAttribute(ID_ATTRIBUTE, id);
        return id;
    }

    /**
     * Signals the given {@code request} that it's finished
     * and shouldn't process anything further.
     *
     * @see #isFinished
     */
    public static void finish(ServletRequest request) {
        request.setAttribute(IS_FINISHED_ATTRIBUTE, Boolean.TRUE);
    }

    /**
     * Forwards to the resource at the given {@code path} with the given
     * {@code attributes} overridden.
     *
     * @param request
     *        Can't be {@code null}.
     *
     * @param response
     *        Can't be {@code null}.
     *
     * @param path
     *        Can't be {@code null}.
     *
     * @param attributes
     *        If {@code null}, doesn't override any attributes.
     */
    public static void forward(
            ServletRequest request,
            ServletResponse response,
            String path,
            Object... attributes)
            throws IOException, ServletException {

        Preconditions.checkNotNull(request);
        Preconditions.checkNotNull(response);
        Preconditions.checkNotNull(path);

        Map<String, Object> oldAttributes = setAttributes(request, attributes);

        try {
            request.getRequestDispatcher(path).forward(request, response);

        } finally {
            setAttributesWithMap(request, oldAttributes);
        }
    }

    /**
     * Returns the absolute version of the given {@code path}. The return
     * value includes the context path and is meant for final display
     * (e.g. an HTML page).
     *
     * @param path If empty, existing query parameters from the given
     * {@code request} are added in addition to the given {@code parameters}.
     *
     * @see #resolvePath
     */
    public static String getAbsolutePath(
            HttpServletRequest request,
            String path,
            Object... parameters) {

        return getEmbeddedAbsolutePath(null, request, path, parameters);
    }

    /** Returns the absolute version of the given {@code url}. */
    public static String getAbsoluteUrl(
            HttpServletRequest request,
            String url,
            Object... parameters) {

        return getEmbeddedAbsoluteUrl(null, request, url, parameters);
    }

    /**
     * Returns the cookie with the given {@code name} from the given
     * {@code request}.
     */
    public static Cookie getCookie(HttpServletRequest request, String name) {
        Cookie[] cookies = request.getCookies();
        if (cookies != null) {
            for (Cookie cookie : cookies) {
                if (cookie.getName().equals(name)) {
                    return cookie;
                }
            }
        }
        return null;
    }

    /** Returns the current context path of the given {@code request}. */
    public static String getCurrentContextPath(HttpServletRequest request) {
        return isIncluded(request)
                ? (String) request.getAttribute("javax.servlet.include.context_path")
                : request.getContextPath();
    }

    /** Returns the current path info of the given {@code request}. */
    public static String getCurrentPathInfo(HttpServletRequest request) {
        return isIncluded(request)
                ? (String) request.getAttribute("javax.servlet.include.path_info")
                : request.getPathInfo();
    }

    /** Returns the current query string of the given {@code request}. */
    public static String getCurrentQueryString(HttpServletRequest request) {
        return isIncluded(request)
                ? (String) request.getAttribute("javax.servlet.include.query_string")
                : request.getQueryString();
    }

    /** Returns the current servlet path of the given {@code request}. */
    public static String getCurrentServletPath(HttpServletRequest request) {
        return isIncluded(request)
                ? (String) request.getAttribute("javax.servlet.include.servlet_path")
                : request.getServletPath();
    }

    /** Returns the exception that the given {@code request} is handling. */
    public static Throwable getErrorException(HttpServletRequest request) {
        return (Throwable) request.getAttribute("javax.servlet.error.exception");
    }

    /**
     * Returns the URI that caused the error currently being handled
     * by the given {@code request}.
     */
    public static String getErrorRequestUri(ServletRequest request) {
        return (String) request.getAttribute("javax.servlet.error.request_uri");
    }

    /**
     * Use {@link #getErrorRequestUri(ServletRequest)} instead.
     */
    public static String getErrorRequestUri(HttpServletRequest request) {
        return getErrorRequestUri((ServletRequest) request);
    }

    /**
     * Returns the first proxy header value, which may be a
     * comma-separated list of values.
     */
    private static String getFirstProxyHeader(String header) {
        if (ObjectUtils.isBlank(header)) {
            return null;
        } else {
            int commaAt = header.indexOf(',');
            if (commaAt > -1) {
                header = header.substring(0, commaAt);
            }
        }
        return header;
    }

    /**
     * Returns the servlet response associated with the given
     * {@code request} that can be used to write the headers.
     *
     * @see #setHeaderResponse
     * @see HeaderResponseFilter
     */
    public static ServletResponse getHeaderResponse(
            ServletRequest request,
            ServletResponse response) {

        ServletResponse headerResponse = (ServletResponse) request.getAttribute(HEADER_RESPONSE_ATTRIBUTE);
        return headerResponse != null ? headerResponse : response;
    }

    /**
     * Returns the host ({@code X-Forwarded-Host} or {@code Host} header which
     * may include the port number) from the given {@code request}.
     *
     * @param request Can't be {@code null}.
     * @return Never {@code null}.
     */
    public static String getHost(HttpServletRequest request) {
        String host = getFirstProxyHeader(request.getHeader("X-Forwarded-Host"));

        return host != null ? host : request.getHeader("Host");
    }

    /**
     * Returns the host name (no port number) from the given {@code request}.
     *
     * @param request Can't be {@code null}.
     * @return Never {@code null}.
     */
    public static String getHostname(HttpServletRequest request) {
        String host = getHost(request);
        int colonAt = host.lastIndexOf(':');

        return colonAt > -1 ? host.substring(0, colonAt) : host;
    }

    /** Returns the host URL from the given {@code request}. */
    public static String getHostUrl(HttpServletRequest request) {
        return request.getScheme() + "://" + getHost(request);
    }

    /** Returns the protocol relative host URL from the given {@code request}. */
    public static String getProtocolRelativeHostUrl(HttpServletRequest request) {
        return "//" + getHost(request);
    }

    /**
     * Returns the unique ID last created within the given
     * {@code request}.
     *
     * @see #createId
     */
    public static String getId(ServletRequest request) {
        Object id = request.getAttribute(ID_ATTRIBUTE);
        if (!(id instanceof String)) {
            throw new IllegalStateException("Unique ID was never created!");
        }
        return (String) id;
    }

    /** Returns the original context path of the given {@code request}. */
    public static String getOriginalContextPath(HttpServletRequest request) {
        return isForwarded(request)
                ? (String) request.getAttribute("javax.servlet.forward.context_path")
                : request.getContextPath();
    }

    /** Returns the original path info of the given {@code request}. */
    public static String getOriginalPathInfo(HttpServletRequest request) {
        return isForwarded(request)
                ? (String) request.getAttribute("javax.servlet.forward.path_info")
                : request.getPathInfo();
    }

    /** Returns the original query string of the given {@code request}. */
    public static String getOriginalQueryString(HttpServletRequest request) {
        return isForwarded(request)
                ? (String) request.getAttribute("javax.servlet.forward.query_string")
                : request.getQueryString();
    }

    /** Returns the original servlet path of the given {@code request}. */
    public static String getOriginalServletPath(HttpServletRequest request) {
        return isForwarded(request)
                ? (String) request.getAttribute("javax.servlet.forward.servlet_path")
                : request.getServletPath();
    }

    /**
     * Returns the IP address of the client that made the given
     * {@code request}.
     */
    public static String getRemoteAddress(HttpServletRequest request) {
        String address = getFirstProxyHeader(request.getHeader("X-Forwarded-For"));
        return address != null ? address : request.getRemoteAddr();
    }

    /**
     * Returns the value of the signed cookie with the given {@code name}.
     *
     * @see #setSignedCookie
     */
    public static String getSignedCookie(
            HttpServletRequest request,
            String name) {

        return getSignedCookieWithExpiry(request, name, 0);
    }

    /**
     * Returns the value of the signed cookie with the given {@code name} so
     * long as the given {@code expirationDuration} has not been exceeded. A
     * zero or negative {@code expirationDuration} signifies that the cookie
     * does not expire.
     *
     * @see #setSignedCookie
     */
    public static String getSignedCookieWithExpiry(
            HttpServletRequest request,
            String name,
            long expirationDuration) {

        Cookie cookie = getCookie(request, name);
        if (cookie == null) {
            return null;

        } else {
            return unsignCookieWithExpiry(name, cookie.getValue(), expirationDuration);
        }
    }

    /**
     * Returns the writer associated with the given {@code response}.
     *
     * <p>Unlike the {@link ServletResponse#getWriter}, this method won't
     * ever throw {@link IllegalStateException}.</p>
     */
    public static PrintWriter getWriter(HttpServletResponse response) throws IOException {
        try {
            return response.getWriter();
        } catch (IllegalStateException error) {
            return new PrintWriter(new OutputStreamWriter(response.getOutputStream(), StandardCharsets.UTF_8));
        }
    }

    /**
     * {@linkplain javax.servlet.RequestDispatcher#include Includes} the
     * resource at the given {@code path} and writes its output to the
     * given {@code writer}. The given {@code attributes} are set on the
     * request before execution, and any overriden values are restored
     * before this method returns.
     *
     * @return {@code false} if servlet processing shouldn't continue
     * any further (e.g. for when the resource redirects to a different
     * page).
     */
    public static boolean include(
            HttpServletRequest request,
            HttpServletResponse response,
            Writer writer,
            String path,
            Object... attributes)
            throws IOException, ServletException {

        return includeEmbedded(null, request, response, writer, path, attributes);
    }

    /**
     * Returns {@code true} if the given {@code request} is made with
     * Ajax.
     */
    public static boolean isAjaxRequest(HttpServletRequest request) {
        return "XMLHttpRequest".equals(request.getHeader("X-Requested-With"));
    }

    /**
     * Returns {@code true} if the given {@code request} and
     * {@code response} has finished and shouldn't be processed
     * any further.
     *
     * @see #finish
     */
    public static boolean isFinished(
            ServletRequest request,
            ServletResponse response) {

        return (response instanceof HttpServletResponse
                && ((HttpServletResponse) response).containsHeader("Location"))
                || request.getAttribute(IS_FINISHED_ATTRIBUTE) != null;
    }

    /**
     * Returns {@code true} if the given {@code request} is a form
     * post.
     */
    public static boolean isFormPost(HttpServletRequest request) {
        return "POST".equals(request.getMethod());
    }

    /**
     * Returns {@code true} if the given {@code request} is currently
     * handling an error.
     */
    public static boolean isError(ServletRequest request) {
        return getErrorRequestUri(request) != null;
    }

    /**
     * Use {@link #isError(ServletRequest)} instead.
     */
    public static boolean isError(HttpServletRequest request) {
        return isError((ServletRequest) request);
    }

    /**
     * Returns {@code true} if the given {@code request} is forwarded from
     * another.
     */
    public static boolean isForwarded(ServletRequest request) {
        return request.getAttribute("javax.servlet.forward.context_path") != null;
    }

    /**
     * Returns {@code true} if the given {@code request} is included from
     * another.
     */
    public static boolean isIncluded(ServletRequest request) {
        return request.getAttribute("javax.servlet.include.context_path") != null;
    }

    /**
     * Returns {@code true} if the given {@code request} is secure. This method
     * checks:
     *
     * <ul>
     * <li>{@link ServletRequest#isSecure}</li>
     * <li>{@code X-Forwarded-Proto} header</li>
     * <li>{@code CloudFront-Forwarded-Proto} header</li>
     * <li>{@code HTTPS} environment variable</li>
     * </ul>
     */
    public static boolean isSecure(HttpServletRequest request) {
        return request.isSecure()
                || "https".equalsIgnoreCase(request.getHeader("X-Forwarded-Proto"))
                || "https".equalsIgnoreCase(request.getHeader("CloudFront-Forwarded-Proto"))
                || System.getenv("HTTPS") != null;
    }

    /** @deprecated Use {@link #isSecure} instead. */
    @Deprecated
    public static boolean isSecureRequest(HttpServletRequest request) {
        return isSecure(request);
    }

    /**
     * Proxies the given {@code request} and {@code response} to the given
     * {@code url} and writes the result to the given {@code writer}. Note
     * that this method requires a HttpServletRequest implementation that
     * allows re-reading the request content.
     *
     * @see ReusableRequestFilter
     *
     * @deprecated No replacement.
     */
    @Deprecated
    public static void proxy(
            HttpServletRequest request,
            HttpServletResponse response,
            Writer writer,
            Object url,
            Object... parameters)
            throws IOException {

        String method = request.getMethod();
        String urlString = JspUtils.getAbsolutePath(request, url.toString(), parameters);

        InputStream requestStream = null;
        Reader requestReader = null;
        BufferedOutputStream connectionOut = null;
        BufferedInputStream connectionIn = null;
        HttpURLConnection connection;

        try {
            connection = (HttpURLConnection) new URL(urlString).openConnection();
            connection.setRequestMethod(method);

            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(true);

            for (Enumeration<?> e = request.getHeaderNames(); e.hasMoreElements();) {
                String headerName = e.nextElement().toString();
                String headerValue = request.getHeader(headerName);
                connection.setRequestProperty(headerName, headerValue);
            }

            connection.connect();

            if ("POST".equalsIgnoreCase(method)) {
                connectionOut = new BufferedOutputStream(connection.getOutputStream());
                // first try to get the input stream
                try {
                    requestStream = request.getInputStream();
                    int data;
                    while ((data = requestStream.read()) != -1) {
                        connectionOut.write(data);
                    }
                } catch (IllegalStateException e1) {
                    // stream is unavailable, try the reader
                    try {
                        requestReader = request.getReader();
                        int data;
                        while ((data = requestReader.read()) != -1) {
                            connectionOut.write(data);
                        }
                    } catch (IllegalArgumentException e2) {
                        // oh well, we tried
                    }
                }
                connectionOut.flush();
            }

            connectionIn = new BufferedInputStream(connection.getInputStream());
            int data;
            while ((data = connectionIn.read()) != -1) {
                writer.write(data);
            }

            connection.disconnect();

        } finally {
            if (requestStream != null) {
                requestStream.close();
            }
            if (connectionIn != null) {
                connectionIn.close();
            }
            if (connectionOut != null) {
                connectionOut.close();
            }
        }
    }

    /**
     * Redirects to the given {@code path} modified by the given
     * {@code parameters} using the {@value
     * javax.servlet.http.HttpServletResponse#SC_FOUND} status code.
     */
    public static void redirect(
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbedded(null, request, response, path, parameters);
    }

    /**
     * Redirects to the given {@code path} modified by the given
     * {@code parameters} using the {@value
     * javax.servlet.http.HttpServletResponse#SC_MOVED_PERMANENTLY}
     * status code.
     */
    public static void redirectPermanently(
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbeddedPermanently(null, request, response, path, parameters);
    }

    /**
     * Resolves the given {@code path} in context of the given
     * {@code request}. The return value includes the {@linkplain
     * #getEmbeddedContextPath embedded context path}.
     *
     * <p>The following list details the different behaviors based on the
     * given {@code path}:
     *
     * <ul>
     * <li>If {@code null} or empty, returns the servlet path.
     * <li>If it starts with {@code /}, prefixes the given {@code path}
     * with the {@linkplain #getEmbeddedContextPath embedded context path}
     * and returns it.
     * <li>If it looks like an absolute URI, returns the given {@code path}
     * without any changes.
     * <li>Otherwise, resolves the path in context of the {@linkplain
     * #getCurrentServletPath current servlet path} and returns it.
     * </ul>
     *
     * @param context If {@code null}, the embedded context path won't
     * be detected. This is only to support legacy APIs, and new
     * applications shouldn't depend on this behavior.
     *
     * @see #getAbsolutePath(ServletContext, HttpServletRequest, String, Object...)
     */
    public static String resolvePath(
            ServletContext context,
            HttpServletRequest request,
            String path) {

        if (path == null || path.isEmpty()) {
            return request.getServletPath();

        } else if (path.startsWith("/")) {
            if (context != null) {
                return getEmbeddedContextPath(context, request) + path;
            } else {
                return path;
            }

        } else if (ABSOLUTE_URI_PATTERN.matcher(path).matches()) {
            return path;

        } else {
            try {
                URI currentPath = new URI(getCurrentServletPath(request));
                return currentPath.resolve(path).toString();

            } catch (URISyntaxException ex) {
                return path;
            }
        }
    }

    /**
     * Sets all given {@code attributes} in the given {@code request}
     * and returns the previously set values.
     *
     * @param request
     *        Can't be {@code null}.
     *
     * @param attributes
     *        Alternating key and value pairs. May be {@code null}.
     *
     * @return Never {@code null}.
     */
    public static Map<String, Object> setAttributes(ServletRequest request, Object... attributes) {
        Preconditions.checkNotNull(request);

        Map<String, Object> old = new HashMap<>();

        for (int i = 0, length = attributes.length; i < length; i += 2) {
            String key = String.valueOf(attributes[i]);

            old.put(key, request.getAttribute(key));
            request.setAttribute(key, i + 1 < length ? attributes[i + 1] : null);
        }

        return old;
    }

    /**
     * Sets all given {@code attributes} in the given {@code request}
     * and returns the previously set values.
     *
     * @param request
     *        Can't be {@code null}.
     *
     * @param attributes
     *        May be {@code null}.
     *
     * @return Never {@code null}.
     */
    public static Map<String, Object> setAttributesWithMap(ServletRequest request, Map<String, Object> attributes) {
        Preconditions.checkNotNull(request);

        Map<String, Object> old = new HashMap<>();

        for (Map.Entry<String, Object> entry : attributes.entrySet()) {
            String key = entry.getKey();

            old.put(key, request.getAttribute(key));
            request.setAttribute(key, entry.getValue());
        }

        return old;
    }

    /**
     * Sets the servlet response that can be used to write the
     * headers on the given {@code request}.
     *
     * <p>SRV.8.3 in the Java servlet specification states (emphasis added):
     *
     * <p><blockquote>The {@code include} method of the
     * {@code RequestDispatcher} interface may be called at any time.
     * The target servlet of the {@code include} method has access to all
     * aspects of the request object, but its use of the response object
     * is more limited.
     *
     * <br><br>It can only write information to the
     * {@code ServletOutputStream} or {@code Writer} of the response
     * object and commit a response by writing content past the end of
     * the response buffer, or by explicitly calling the {@code flushBuffer}
     * method of the {@code ServletResponse} interface. <strong>It cannot
     * set headers or call any method that affects the headers of the
     * response. Any attempt to do so must be ignored.</strong></blockquote>
     *
     * <p>This method should be used in the parent request so that the
     * included page still has access to set the headers through the
     * parent response object.
     *
     * @see #getHeaderResponse
     * @see HeaderResponseFilter
     */
    public static void setHeaderResponse(
            ServletRequest request,
            ServletResponse response) {

        request.setAttribute(HEADER_RESPONSE_ATTRIBUTE, response);
    }

    /**
     * Signs the given {@code cookie} and sets it in the given
     * {@code response}.
     *
     * @see #getSignedCookie
     */
    public static void setSignedCookie(
            HttpServletResponse response,
            Cookie cookie) {

        cookie.setValue(signCookie(cookie.getName(), cookie.getValue()));
        response.addCookie(cookie);
    }

    /**
     * Signs the given {@code unsignedValue} that's associated to a
     * cookie with the given {@code name}.
     */
    public static String signCookie(String name, String unsignedValue) {
        long timestamp = System.currentTimeMillis();
        return unsignedValue
                + "|" + timestamp
                + "|" + createCookieSignature(name, unsignedValue, timestamp);
    }

    /**
     * Unsigns the given {@code signedValue} that's associated to a
     * cookie with the given {@code name}.
     */
    public static String unsignCookie(String name, String signedValue) {
        return unsignCookieWithExpiry(name, signedValue, 0);
    }

    /**
     * Unsigns the given {@code signedValue} that's associated to a
     * cookie with the given {@code name} so long as the
     * given {@code expirationDuration} has not been exceeded. A zero or
     * negative {@code expirationDuration} signifies that the cookie does not
     * expire.
     */
    public static String unsignCookieWithExpiry(String name, String signedValue, long expirationDuration) {
        String[] parts = StringUtils.split(signedValue, "\\|");
        if (parts.length != 3) {
            LOGGER.debug("Not a valid signed cookie! {}", signedValue);
            return null;
        }

        String unsignedValue = parts[0];
        long timestamp = ObjectUtils.to(long.class, parts[1]);
        String signature = parts[2];

        String signatureCheck = createCookieSignature(name, unsignedValue, timestamp);
        if (!signatureCheck.equals(signature)) {
            LOGGER.debug("Failed signature! {} != {}", signatureCheck, signature);
            return null;
        }

        long expiration = System.currentTimeMillis() - expirationDuration;
        if (expirationDuration > 0 && timestamp < expiration) {
            LOGGER.debug("Signature expired! {} < {}", timestamp, expiration);
            return null;
        }

        return unsignedValue;
    }

    // --- Embedded web application ---

    /**
     * Returns the context path of the embedded web application associated
     * with the given {@code context} and {@code path}. This is detected
     * by checking for the existence of {@code WEB-INF} in the common parent
     * directory.
     */
    public static String getEmbeddedContextPath(ServletContext context, String path) {
        @SuppressWarnings("unchecked")
        Map<String, String> contextPaths = (Map<String, String>) context.getAttribute(EMBEDDED_CONTEXT_PATHS);
        if (contextPaths == null) {
            contextPaths = new ConcurrentHashMap<String, String>();
            context.setAttribute(EMBEDDED_CONTEXT_PATHS, contextPaths);
        }

        String contextPath = contextPaths.get(path);
        if (contextPath == null) {
            try {
                URI pathUri = new URI(path).resolve("./");

                while (context.getResource(pathUri.resolve(WEB_INF_DIRECTORY).toString()) == null
                        && pathUri.toString().length() > 1) {
                    pathUri = pathUri.resolve("../");
                }

                String pathString = pathUri.toString();
                contextPath = pathString.substring(0, pathString.length() - 1);

            } catch (MalformedURLException error) {
                // Default context path if the given path is malformed.

            } catch (URISyntaxException error) {
                // Default context path if the resolved URI is malformed.
            }

            if (contextPath == null) {
                contextPath = "";
            }
            contextPaths.put(path, contextPath);
        }

        return contextPath;
    }

    /**
     * Returns the servlet path {@linkplain #getEmbeddedContextPath in
     * context of} the embedded web application associated with the given
     * {@code context} and {@code path}.
     */
    public static String getEmbeddedServletPath(ServletContext context, String path) {
        String contextPath = getEmbeddedContextPath(context, path);
        return path.substring(contextPath.length());
    }

    /**
     * Returns the settings for all embedded web applications associated
     * with the given {@code context}, keyed by their {@linkplain
     * #getEmbeddedContextPath context paths}.
     */
    public static Map<String, Properties> getEmbeddedSettings(ServletContext context) {
        @SuppressWarnings("unchecked")
        Map<String, Properties> all = (Map<String, Properties>) context.getAttribute(EMBEDDED_SETTINGS_ATTRIBUTE);
        if (all == null) {
            all = new CompactMap<String, Properties>();
            addEmbeddedSettings(context, all, "/" + JspUtils.WEB_INF_DIRECTORY, "/");
            context.setAttribute(EMBEDDED_SETTINGS_ATTRIBUTE, all);
        }
        return all;
    }

    private static void addEmbeddedSettings(
            ServletContext context,
            Map<String, Properties> all,
            String suffix,
            String path) {

        @SuppressWarnings("unchecked")
        Set<String> subPaths = (Set<String>) context.getResourcePaths(path);

        if (subPaths == null) {
            return;
        }

        for (String subPath : subPaths) {

            if (subPath.endsWith(suffix)) {
                Properties properties = new Properties();
                String file = subPath + "classes/settings.properties";
                InputStream input = context.getResourceAsStream(file);

                if (input != null) {
                    try {
                        try {
                            properties.load(input);
                            all.put(subPath.substring(0, subPath.length() - suffix.length()), properties);
                        } finally {
                            input.close();
                        }
                    } catch (IOException error) {
                        LOGGER.warn(String.format(
                                "Can't read [%s] settings file!", file),
                                error);
                    }
                }

            } else if (subPath.endsWith("/")) {
                addEmbeddedSettings(context, all, suffix, subPath);
            }
        }
    }

    /**
     * Returns the absolute version of the given {@code path}. The return
     * value includes the context path and is meant for final display
     * (e.g. an HTML page).
     *
     * @param path If empty, existing query parameters from the given
     * {@code request} are added in addition to the given {@code parameters}.
     *
     * @see #resolvePath
     */
    public static String getEmbeddedAbsolutePath(
            ServletContext context,
            HttpServletRequest request,
            String path,
            Object... parameters) {

        String resolved = resolvePath(context, request, path);

        if (path != null && path.isEmpty()) {
            String queryString = request.getQueryString();
            if (queryString != null && queryString.length() > 0) {
                resolved += "?" + StringUtils.replaceAll(
                        queryString,
                        "([?&])_[^=]*=[^&]*(?:&|$)", "$1",
                        "&$", "");
            }
        }

        return StringUtils.addQueryParameters(
                request.getContextPath() + resolved,
                parameters);
    }

    /** Returns the absolute version of the given {@code url}. */
    public static String getEmbeddedAbsoluteUrl(
            ServletContext context,
            HttpServletRequest request,
            String url,
            Object... parameters) {

        return getHostUrl(request) + getAbsolutePath(context, request, url, parameters);
    }

    /** Returns the absolute protocol relative version of the given {@code url}. */
    public static String getEmbeddedAbsoluteProtocolRelativeUrl(
            ServletContext context,
            HttpServletRequest request,
            String url,
            Object... parameters) {

        return getProtocolRelativeHostUrl(request) + getAbsolutePath(context, request, url, parameters);
    }

    /**
     * {@linkplain javax.servlet.RequestDispatcher#include Includes} the
     * resource at the given {@code path} and writes its output to the
     * given {@code writer}. The given {@code attributes} are set on the
     * request before execution, and any overriden values are restored
     * before this method returns.
     *
     * @return {@code false} if servlet processing shouldn't continue
     * any further (e.g. for when the resource redirects to a different
     * page).
     */
    public static boolean includeEmbedded(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Writer writer,
            String path,
            Object... attributes)
            throws IOException, ServletException {

        Map<String, Object> old = setAttributes(request, attributes);

        try {
            path = resolvePath(context, request, path);
            response = new IncludedHttpServletResponse(response, writer);
            request.getRequestDispatcher(path).include(request, response);

        } finally {
            setAttributesWithMap(request, old);
        }

        return isFinished(request, response);
    }

    /**
     * {@linkplain #include Included JSPs} need to inherit the writer
     * from the parent response for corrent rendering.
     */
    private static class IncludedHttpServletResponse extends HttpServletResponseWrapper {

        private final PrintWriter writer;

        public IncludedHttpServletResponse(
                HttpServletResponse response,
                Writer writer) {

            super(response);
            this.writer = writer instanceof PrintWriter
                    ? (PrintWriter) writer
                    : new PrintWriter(writer);
        }

        // --- HttpServletResponseWrapper support ---

        @Override
        public PrintWriter getWriter() throws IOException {
            return writer;
        }
    }

    /**
     * Redirects to the given {@code path} modified by the given
     * {@code parameters} using the {@value
     * javax.servlet.http.HttpServletResponse#SC_FOUND} status code.
     */
    public static void redirectEmbedded(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbeddedWithStatus(context, request, response, HttpServletResponse.SC_FOUND, path, parameters);
    }

    /**
     * Redirects to the given {@code path} modified by the given
     * {@code parameters} using the {@value
     * javax.servlet.http.HttpServletResponse#SC_MOVED_PERMANENTLY}
     * status code.
     */
    public static void redirectEmbeddedPermanently(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbeddedWithStatus(context, request, response, HttpServletResponse.SC_MOVED_PERMANENTLY, path, parameters);
    }

    /**
     * Redirects to the given {@code path} modified by the given
     * {@code parameters} using the given {@code status} code.
     */
    private static void redirectEmbeddedWithStatus(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            int status,
            Object path,
            Object... parameters)
            throws IOException {

        response = (HttpServletResponse) getHeaderResponse(request, response);
        response.setStatus(status);
        response.setHeader("Location", response.encodeRedirectURL(getAbsolutePath(context, request, path == null ? null : path.toString(), parameters)));
    }

    /**
     * Wraps the default JSP factory using an instance of the the given
     * {@code wrapperClass}.
     */
    public static void wrapDefaultJspFactory(Class<? extends JspFactoryWrapper> wrapperClass) {
        JspFactory factory = JspFactory.getDefaultFactory();

        for (JspFactory f = factory; f instanceof JspFactoryWrapper; f = ((JspFactoryWrapper) f).getDelegate()) {
            if (wrapperClass.isInstance(f)) {
                return;
            }
        }

        JspFactoryWrapper wrapper = TypeDefinition.getInstance(wrapperClass).newInstance();

        wrapper.setDelegate(factory);
        JspFactory.setDefaultFactory(wrapper);
    }

    /**
     * Unwraps the default JSP factory and restore the original default if
     * it's an instance of the given {@code wrapperClass}.
     */
    public static void unwrapDefaultJspFactory(Class<? extends JspFactoryWrapper> wrapperClass) {
        JspFactory factory = JspFactory.getDefaultFactory();

        if (factory instanceof JspFactoryWrapper) {
            JspFactory.setDefaultFactory(((JspFactoryWrapper) factory).getDelegate());
        }
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #getEmbeddedAbsolutePath} instead. */
    @Deprecated
    public static String getAbsolutePath(
            ServletContext context,
            HttpServletRequest request,
            String path,
            Object... parameters) {

        return getEmbeddedAbsolutePath(context, request, path, parameters);
    }

    /** @deprecated Use {@link #getEmbeddedAbsoluteUrl} instead. */
    @Deprecated
    public static String getAbsoluteUrl(
            ServletContext context,
            HttpServletRequest request,
            String url,
            Object... parameters) {

        return getEmbeddedAbsoluteUrl(context, request, url, parameters);
    }

    /** @deprecated Use {@link #getEmbeddedContextPath(ServletContext, String)} instead. */
    @Deprecated
    public static String getEmbeddedContextPath(ServletContext context, HttpServletRequest request) {
        return getEmbeddedContextPath(context, getCurrentServletPath(request));
    }

    /** @deprecated Use {@link #getEmbeddedServletPath(ServletContext, String)} instead. */
    @Deprecated
    public static String getEmbeddedServletPath(ServletContext context, HttpServletRequest request) {
        return getEmbeddedServletPath(context, getCurrentServletPath(request));
    }

    /** @deprecated Use {@link #getHost} instead. */
    @Deprecated
    public static String getFullyQualifiedDomain(HttpServletRequest request) {
        return getHost(request);
    }

    /** @deprecated Use {@link #getAbsoluteUrl} instead. */
    @Deprecated
    public static String getFullyQualifiedUrl(HttpServletRequest request) {
        String url = getAbsoluteUrl(request, "/");
        return url.substring(0, url.length() - 1);
    }

    /** @deprecated Use {@link #getAbsolutePath} instead. */
    @Deprecated
    public static String getUrl(
            HttpServletRequest request,
            String path,
            Object... parameters) {

        return getAbsolutePath(request, path, parameters);
    }

    /** @deprecated Use {@link #includeEmbedded} instead. */
    @Deprecated
    public static boolean include(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Writer writer,
            String path,
            Object... attributes)
            throws IOException, ServletException {

        return includeEmbedded(context, request, response, writer, path, attributes);
    }

    /** @deprecated Use {@link #redirectEmbedded} instead. */
    @Deprecated
    public static void redirect(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbedded(context, request, response, path, parameters);
    }

    /** @deprecated Use {@link #redirectEmbeddedPermanently} instead. */
    @Deprecated
    public static void redirectPermanently(
            ServletContext context,
            HttpServletRequest request,
            HttpServletResponse response,
            Object path,
            Object... parameters)
            throws IOException {

        redirectEmbeddedPermanently(context, request, response, path, parameters);
    }
}
