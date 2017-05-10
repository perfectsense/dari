package com.psddev.dari.util;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.lang.StringEscapeUtils;

/** String utility methods. */
public final class StringUtils {

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#US_ASCII} instead.
     */
    @Deprecated
    public static final Charset US_ASCII = Charset.forName("US-ASCII");

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#ISO_8859_1} instead.
     */
    @Deprecated
    public static final Charset ISO_8859_1 = Charset.forName("ISO-8859-1");

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#UTF_8} instead.
     */
    @Deprecated
    public static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#UTF_16BE} instead.
     */
    @Deprecated
    public static final Charset UTF_16BE = Charset.forName("UTF-16BE");

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#UTF_16LE} instead.
     */
    @Deprecated
    public static final Charset UTF_16LE = Charset.forName("UTF-16LE");

    /**
     * @deprecated Use {@link java.nio.charset.StandardCharsets#UTF_16} instead.
     */
    @Deprecated
    public static final Charset UTF_16 = Charset.forName("UTF-16");

    private static final Set<String>
            ABBREVIATIONS = new HashSet<String>(Arrays.asList(
            "cms", "css", "id", "js", "seo", "ugc", "uri", "url"));

    private static final char[] HEX_CHARACTERS = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * Converts given string into a value of given type, throwing an exception
     * if conversion was unsuccessful.
     *
     * If the return type is an array, an array is returned.
     * <code>fromString(int[].class, "1", "2")<code> =&gt; <code>int[] {1,2}</code>
     *
     * If no values are provided and the return type is an array, an empty array is returned.
     * <code>fromString(int[].class)<code> =&gt; <code>int[] {}</code>
     *
     * If no values are provided and the return type is not an array, an exception
     * appropriate to the conversion type is thrown.
     * <code>fromString(int.class)<code> =&gt; <code>NumberFormatException</code>
     *
     * If the {@code strings} contains more than one value and the {@code returnType} is not
     * an array, all but the first value are ignored.
     *
     * If the method cannot convert to the class specified, an IllegalArgumentException is thrown.
     *
     * @param <T>
     * @param returnType The class for the String input(s) to be converted to
     * @param strings the input(s) to be converted
     * @return the converted value
     * @deprecated Use {@link ObjectUtils#to(Class, Object)} instead.
     */
    @Deprecated
    public static <T> T fromString(Class<T> returnType, String... strings) {

        // heavy voodoo follows...
        // need to return an array
        Class<?> componentType = returnType.getComponentType();
        if (componentType != null) {
            if (strings == null) {
                return null;
            } else {
                int length = strings.length;
                Object typed = Array.newInstance(componentType, length);
                for (int i = 0; i < length; i++) {
                    Array.set(typed, i, fromString(componentType, strings[i]));
                }
                return (T) typed;
            }

            // single value
        } else {
            String string = strings == null || strings.length == 0 ? null : strings[0];

            // string to string
            if (String.class == returnType) {
                return (T) string;

                // primitives
                // pass null through if returnType allows it
            } else if (string == null && !returnType.isPrimitive()) {
                return null;

                // any way to avoid boxing/unboxing on primitives?
            } else if (boolean.class == returnType || Boolean.class.isAssignableFrom(returnType)) {
                return (T) Boolean.valueOf(string);
            } else if (byte.class == returnType || Byte.class.isAssignableFrom(returnType)) {
                return (T) Byte.valueOf(string);
            } else if (short.class == returnType || Short.class.isAssignableFrom(returnType)) {
                return (T) Short.valueOf(string);
            } else if (int.class == returnType || Integer.class.isAssignableFrom(returnType)) {
                return (T) Integer.valueOf(string);
            } else if (long.class == returnType || Long.class.isAssignableFrom(returnType)) {
                return (T) Long.valueOf(string);
            } else if (float.class == returnType || Float.class.isAssignableFrom(returnType)) {
                return (T) Float.valueOf(string);
            } else if (double.class == returnType || Double.class.isAssignableFrom(returnType)) {
                return (T) Double.valueOf(string);
            } else if (char.class == returnType || Character.class.isAssignableFrom(returnType)) {
                if (string.length() == 1) {
                    return (T) Character.valueOf(string.charAt(0));
                }

                // others
            } else if (Date.class.isAssignableFrom(returnType)) {
                return (T) DateUtils.fromString(string);
            }
            throw new IllegalArgumentException(String.format(
                    "Cannot convert [%s] string to [%s] type!",
                    string,
                    returnType.getName()
            ));
        }
    }

    // Splits a string into a list words based on either case changes or
    // special delimiters.
    protected static List<String> splitString(String string) {
        List<String> words = new ArrayList<String>();
        char[] letters = string.toCharArray();
        int length = letters.length;
        int marker = 0;

        for (int i = 0; i < length; ++ i) {
            char c = letters[i];

            if (" -_.$".indexOf(c) > -1) {
                words.add(string.substring(marker, i).toLowerCase(Locale.ENGLISH));
                for (++ i; i < length && " -_.$".indexOf(letters[i]) > -1; ++ i) {
                    // Skip past all special delimiters.
                }

                marker = i;

            } else if (Character.isUpperCase(c) && i > 0 && Character.isLowerCase(letters[i - 1])) {
                words.add(string.substring(marker, i).toLowerCase(Locale.ENGLISH));

                marker = i;
            }
        }

        if (marker < length) {
            words.add(string.substring(marker).toLowerCase(Locale.ENGLISH));
        }

        return words;
    }

    /**
     * Tries to detect words within a given string and join them with the given delimiter.
     */
    public static String toDelimited(String string, String delimiter) {
        StringBuilder nb = new StringBuilder();
        for (String word : splitString(string)) {
            nb.append(word).append(delimiter);
        }
        if (nb.length() > 0) {
            nb.setLength(nb.length() - delimiter.length());
        }
        return nb.toString();
    }

    /**
     * Converts the given string into a-hyphenated-string.
     */
    public static String toHyphenated(String string) {
        return toDelimited(string, "-");
    }

    /**
     * Converts the given string into a_underscored_string.
     */
    public static String toUnderscored(String string) {
        return toDelimited(string, "_");
    }

    /**
     * Converts the given string into APascalCaseString.
     */
    public static String toPascalCase(String string) {
        StringBuilder nb = new StringBuilder();
        for (String word : splitString(string)) {
            if (!word.isEmpty()) {
                nb.append(Character.toUpperCase(word.charAt(0))).append(word.substring(1));
            }
        }
        return nb.toString();
    }

    /**
     * Converts the given string into aCamelCaseString.
     */
    public static String toCamelCase(String string) {
        string = toPascalCase(string);
        return Character.toLowerCase(string.charAt(0)) + string.substring(1);
    }

    /**
     * Converts the string to one suitable for use as "a label"?
     *
     * Splits {@code string} into words, joining it back together "In Title Case"
     * with known {@code ABBREVIATIONS} replaced in all caps. If the first word of
     * {@code string} is "is", that word is removed and a question mark is
     * added to the end of the resulting string.
     *
     * @param string
     */
    public static String toLabel(String string) {

        if (string == null) {
            return null;
        }

        boolean isQuestion = false;
        List<String> words = splitString(string);
        if (!words.isEmpty() && "is".equals(words.get(0))) {
            isQuestion = true;
            words.remove(0);
        }

        StringBuilder nb = new StringBuilder();
        for (String word : words) {
            if (word.length() == 0) {
                continue;
            }

            if (ABBREVIATIONS.contains(word)) {
                nb.append(word.toUpperCase(Locale.ENGLISH));
            } else {
                nb.append(Character.toUpperCase(word.charAt(0)));
                nb.append(word.substring(1));
            }
            nb.append(' ');
        }
        if (nb.length() > 0) {
            nb.setLength(nb.length() - 1);
        }

        if (isQuestion) {
            nb.append('?');
        }

        return nb.toString();
    }

    /**
     * Normalizes a string, removing or replacing non-alphanumeric characters and lowercasing
     *
     * - Removes all accented characters.
     * - Removes single quotes
     * - Replaces non-alphanumeric characters remaining with a dash
     * - Removes dashes
     *
     * Lowercases the result
     */
    public static String toNormalized(CharSequence string) {
        return string == null ? null : replaceAll(
                Normalizer.normalize(string, Normalizer.Form.NFD),
                "[^\\p{ASCII}]", "",
                "'", "",
                "\\.", "",
                "[^a-zA-Z0-9]+", "-",
                "^-+|-+$", ""
        ).toLowerCase(Locale.ENGLISH);
    }

    /** Splits the given string by commas, and returns each part unescaped.
     *  Calling {@link #toCsv(String...)} on an array of Strings and then
     *  passing the result to this method will always return the same array
     *  as specified by the <code>Arrays.equals()</code> method. */
    public static String[] fromCsv(String string) {
        if (string == null) {
            return null;
        } else {
            // replaces a call to string.split(",")
            char[] charArr = string.toCharArray();
            int commaCount = 0;
            for (char c : charArr) {
                if (c == ',') {
                    commaCount++;
                }
            }
            String[] escaped = new String[commaCount + 1];
            int index = 0;
            int offset = 0;
            int count = 0;
            for (char c : charArr) {
                if (c == ',') {
                    escaped[index ++] = new String(charArr, offset, count);
                    offset += count + 1;
                    count = 0;
                } else {
                    count++;
                }
            }
            if (index == commaCount) {
                escaped[index] = new String(charArr, offset, count);
            }
            // end string.split(",") replacement code

            int length = escaped.length;
            List<String> unescaped = new ArrayList<String>();
            for (int i = 0; i < length; i ++) {
                String value = escaped[i];
                if (value.startsWith("\"")) { // there are commas and/or double quotes escaped within
                    StringBuilder builder = new StringBuilder();
                    int quoteCount = 0;
                    do {
                        builder.append(value);
                        for (char c : value.toCharArray()) {
                            if (c == '"') {
                                quoteCount++;
                            }
                        }
                        if (quoteCount % 2 == 1) {
                            value = escaped[++i];
                            builder.append(',');
                        }
                    } while (quoteCount % 2 == 1);
                    value = builder.toString();
                }
                unescaped.add(StringUtils.unescapeCsv(value));
            }
            return unescaped.toArray(new String[unescaped.size()]);
        }
    }

    /** Converts an array of Strings to a single String in comma separated
     *  values format, escaping each string as necessary. */
    public static String toCsv(String... strings) {
        if (strings == null) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        for (String string : strings) {
            builder.append(escapeCsv(string)).append(',');
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    /**
     * Escapes given string so that it's usable in HTML and breaks it apart so that it can wrap.
     */
    public static String escapeHtmlAndBreak(String string, int maxWordLength) {
        if (string == null) {
            return null;
        }
        Matcher matcher = getMatcher(string, String.format("(\\S{%d,})", maxWordLength));
        StringBuilder output = new StringBuilder();
        int marker = 0;
        while (matcher.find()) {
            String longWord = matcher.group(1);

            // similar to Matcher.appendReplacement
            // re-implemented so that $ and \ does not have to be escaped
            if (matcher.start() > marker) {
                output.append(string.substring(marker, matcher.start()));
            }
            int i = 0;
            for (; i < longWord.length() - maxWordLength; i += maxWordLength) {
                output.append(escapeHtml(longWord.substring(i, i + maxWordLength)));
                output.append("<wbr />");
            }
            output.append(escapeHtml(longWord.substring(i)));
            marker = matcher.end();
        }

        // similar to Matcher.appendTail
        output.append(string.substring(marker));
        return output.toString();
    }

    /**
     * Escapes given string so that it's usable in HTML and breaks it apart so that it can wrap.
     */
    public static String escapeHtmlAndBreak(String string) {
        return escapeHtmlAndBreak(string, 20);
    }

    /** Converts the given {@code bytes} into a hex string. */
    public static String hex(byte[] bytes) {
        if (bytes == null) {
            return null;
        }

        int bytesLength = bytes.length;
        byte currentByte;
        char[] hex = new char[bytesLength * 2];

        for (int byteIndex = 0, hexIndex = 0;
                byteIndex < bytesLength;
                ++ byteIndex, hexIndex += 2) {

            currentByte = bytes[byteIndex];
            hex[hexIndex] = HEX_CHARACTERS[(currentByte & 0xf0) >> 4];
            hex[hexIndex + 1] = HEX_CHARACTERS[(currentByte & 0x0f)];
        }

        return new String(hex);
    }

    /** Hashes the given {@code string} using the given {@code algorithm}. */
    public static byte[] hash(String algorithm, String string) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalArgumentException(String.format("[%s] isn't a valid hash algorithm!", algorithm), ex);
        }

        byte[] bytes;
        try {
            bytes = string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException(ex);
        }

        return digest.digest(bytes);
    }

    /** Hashes the given {@code string} using the MD5 algorithm. */
    public static byte[] md5(String string) {
        return hash("MD5", string);
    }

    /** Hashes the given {@code string} using the SHA-1 algorithm. */
    public static byte[] sha1(String string) {
        return hash("SHA-1", string);
    }

    /** Hashes the given {@code string} using the SHA-512 algorithm. */
    public static byte[] sha512(String string) {
        return hash("SHA-512", string);
    }

    /**
     * Hashes the given {@code string} using the given HMAC {@code algorithm}
     * and {@code key}.
     */
    public static byte[] hmac(String algorithm, String key, String string) {
        try {
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key.getBytes(StringUtils.UTF_8), algorithm));
            return mac.doFinal(string.getBytes(StringUtils.UTF_8));

        } catch (NoSuchAlgorithmException error) {
            throw new IllegalArgumentException(String.format("[%s] isn't a valid HMAC algorithm!", algorithm), error);

        } catch (InvalidKeyException error) {
            throw new IllegalArgumentException(String.format("[%s] isn't a valid key!", key), error);
        }
    }

    /**
     * Hashes the given {@code string} using the HMAC SHA-1 algorithm
     * and the given {@code key}.
     */
    public static byte[] hmacSha1(String key, String string) {
        return hmac("HmacSHA1", key, string);
    }

    // --- URL/URI ---

    /**
     * Encodes the given UTF-8 {@code string} so that it's safe for use
     * within an URI.
     */
    public static String encodeUri(String string) {
        if (string == null) {
            return null;
        }
        try {
            return URLEncoder.encode(string, "UTF-8").replace("+", "%20");
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /** Decodes the given URI-encoded, UTF-8 {@code string}. */
    public static String decodeUri(String string) {
        if (string == null) {
            return null;
        }
        try {
            return URLDecoder.decode(string, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Adds the given {@code parameters} as a query string to the given
     * {@code uri}.
     */
    public static String addQueryParameters(String uri, Object... parameters) {
        if (uri == null) {
            return null;
        }

        // Convert "path?a=b&c=d" to "&a=b&c=d".
        StringBuilder query = new StringBuilder();
        int questionAt = uri.indexOf('?');
        if (questionAt > -1) {

            String queryString = uri.substring(questionAt + 1);
            int beginAt = 0;

            // make sure all the query parameters are encoded
            while (true) {
                int ampIndex = queryString.indexOf('&', beginAt);

                String param = queryString.substring(beginAt, ampIndex > -1 ? ampIndex : queryString.length());

                if (!param.isEmpty() || ampIndex > -1) {
                    query.append('&');

                    int equalsIndex = param.indexOf('=');
                    if (equalsIndex > -1) {
                        query.append(encodeUri(decodeUri(param.substring(0, equalsIndex))));
                        query.append('=');
                        query.append(encodeUri(decodeUri(param.substring(equalsIndex + 1))));

                    } else {
                        query.append(encodeUri(decodeUri(param)));
                    }
                }

                if (ampIndex > -1) {
                    beginAt = ampIndex + 1;
                } else {
                    break;
                }
            }

            uri = uri.substring(0, questionAt);
        }

        int parametersLength = parameters != null ? parameters.length : 0;

        for (int i = 0; i < parametersLength; i += 2) {

            // Remove all occurrences of "&name=".
            String name = parameters[i].toString();
            String prefix = "&" + name + "=";
            int prefixLength = prefix.length();
            int beginAt = 0;
            int endAt;
            while (true) {

                beginAt = query.indexOf(prefix, beginAt);
                if (beginAt < 0) {
                    break;
                }

                endAt = query.indexOf("&", beginAt + prefixLength);
                if (endAt > -1) {
                    query.delete(beginAt, endAt);

                } else {
                    query.delete(beginAt, query.length());
                    break;
                }
            }

            // Append "&name=value".
            if (i + 1 < parametersLength) {
                Object value = parameters[i + 1];
                if (value != null) {
                    for (Object item : ObjectUtils.to(Iterable.class, value)) {
                        if (item != null) {
                            query.append('&');
                            query.append(encodeUri(name));
                            query.append('=');
                            query.append(encodeUri(item instanceof Enum
                                    ? ((Enum<?>) item).name()
                                    : item.toString()));
                        }
                    }
                }
            }
        }

        // Reconstruct the URI.
        if (query.length() <= 1) {
            return uri;

        } else {
            query.delete(0, 1);
            query.insert(0, "?");
            query.insert(0, uri);
            return query.toString();
        }
    }

    /**
     * Returns the first query parameter value associated with the given
     * {@code name} from the given {@code uri}.
     */
    public static String getQueryParameterValue(String uri, String name) {
        for (String value : getQueryParameterValues(uri, name)) {
            return value;
        }
        return null;
    }

    /**
     * Returns a list of query parameter values associated with the given
     * {@code name} from the given {@code uri}.
     */
    public static List<String> getQueryParameterValues(String uri, String name) {
        List<String> values = new ArrayList<String>();
        if (uri != null) {

            // Strip out the path before the query string.
            int questionAt = uri.indexOf('?');
            if (questionAt > -1) {
                uri = uri.substring(questionAt + 1);
            }
            uri = "&" + uri;

            // Find all occurences of "&name=".
            String prefix = "&" + encodeUri(name) + "=";
            int prefixLength = prefix.length();
            for (int nameAt = 0; (nameAt = uri.indexOf(prefix, nameAt)) > -1;) {
                nameAt += prefixLength;
                int andAt = uri.indexOf('&', nameAt);
                values.add(decodeUri(andAt > -1
                        ? uri.substring(nameAt, andAt)
                        : uri.substring(nameAt)));
            }
        }

        return values;
    }

    /** Returns a map of query parameters from the given {@code url}. */
    public static Map<String, List<String>> getQueryParameterMap(String url) {
        Map<String, List<String>> map = new CompactMap<String, List<String>>();

        if (url != null) {
            int questionAt = url.indexOf('?');

            if (questionAt > -1) {
                url = url.substring(questionAt + 1);
            }

            int lastAndAt = 0;

            for (int andAt;
                    (andAt = url.indexOf('&', lastAndAt)) > -1;
                    lastAndAt = andAt + 1) {
                addParameter(map, url.substring(lastAndAt, andAt));
            }

            addParameter(map, url.substring(lastAndAt));
        }

        return map;
    }

    private static void addParameter(Map<String, List<String>> map, String pair) {
        if (pair == null || pair.length() == 0) {
            return;
        }

        int equalAt = pair.indexOf('=');
        String name;
        String value;

        if (equalAt > -1) {
            name = decodeUri(pair.substring(0, equalAt));
            value = decodeUri(pair.substring(equalAt + 1));

        } else {
            name = decodeUri(pair);
            value = null;
        }

        List<String> parameters = map.get(name);

        if (parameters == null) {
            parameters = new ArrayList<String>();
            map.put(name, parameters);
        }

        parameters.add(value);
    }

    /** @deprecated Use {@link #addQueryParameters instead}. */
    @Deprecated
    public static String transformUri(String uri, Object... parameters) {
        return addQueryParameters(uri, parameters);
    }

    /** @deprecated Use {@link #getQueryParameterValue instead}. */
    @Deprecated
    public static String getParameter(String uri, String name) {
        return getQueryParameterValue(uri, name);
    }

    /** @deprecated Use {@link #getQueryParameterValues instead}. */
    @Deprecated
    public static String[] getParameterValues(String uri, String name) {
        List<String> values = getQueryParameterValues(uri, name);
        return values.toArray(new String[values.size()]);
    }

    // --- Pattern bridge ---
    private static final LoadingCache<String, Pattern> PATTERNS = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build(new CacheLoader<String, Pattern>() {

            @Override
            public Pattern load(String pattern) {
                return Pattern.compile(pattern);
            }
        });

    /**
     * Gets a cached regular expression pattern object based on the given string.
     */
    public static Pattern getPattern(String pattern) {
        return PATTERNS.getUnchecked(pattern);
    }

    /**
     * Gets a regular expression matcher based on the given string and pattern.
     *
     * @see #getPattern(String)
     */
    public static Matcher getMatcher(CharSequence string, String pattern) {
        return getPattern(pattern).matcher(string);
    }

    /**
     * Helper function that finds all groups in a regex and returns them in a result. find() is called automatically.
     */
    public static MatchResult getMatcherResult(CharSequence string, String pattern) {
        Matcher matcher = getMatcher(string, pattern);
        matcher.find();
        final MatchResult result = matcher.toMatchResult();

        return new MatchResult() {
            @Override
            public int start() {
                return result.start();
            }

            @Override
            public int start(int group) {
                return result.start(group);
            }

            @Override
            public int end() {
                return result.end();
            }

            @Override
            public int end(int group) {
                return result.end(group);
            }

            @Override
            public String group() {
                return result.group();
            }

            @Override
            public String group(int group) {
                try {
                    return result.group(group);
                } catch (IllegalStateException e) {
                    return null;
                } catch (IndexOutOfBoundsException e) {
                    return null;
                }
            }

            @Override
            public int groupCount() {
                return result.groupCount();
            }
        };
    }

    /**
     * Compiles the given regular expression pattern and attempts to match the given string against it.
     *
     * @see #getMatcher(CharSequence, String).
     */
    public static boolean matches(CharSequence string, String pattern) {
        return getMatcher(string, pattern).matches();
    }

    public static boolean matchAll(CharSequence string, String... patterns) {
        for (String pattern : patterns) {
            if (!matches(string, pattern)) {
                return false;
            }
        }

        return true;
    }

    public static boolean matchAny(CharSequence string, String... patterns) {
        for (String pattern : patterns) {
            if (!matches(string, pattern)) {
                return true;
            }
        }

        return false;
    }

    public static int matchCount(CharSequence string, String... patterns) {
        int count = 0;
        for (String pattern : patterns) {
            if (matches(string, pattern)) {
                ++count;
            }
        }

        return count;
    }

    /**
     * Replaces each substring of the given string that matches the given regular expression pattern with the given replacement.
     *
     * @see #getMatcher(CharSequence, String).
     */
    public static String replaceAll(CharSequence string, String pattern, String replacement) {
        return getMatcher(string, pattern).replaceAll(replacement);
    }

    /**
     * Replaces each substring of the given string that matches the given regular expression pattern with the given replacement.
     *
     * @see #replaceAll(CharSequence, String, String).
     */
    public static String replaceAll(CharSequence string, String pattern, String replacement, String... more) {
        String r = replaceAll(string, pattern, replacement);
        for (int i = 0, l = more.length; i < l; i += 2) {
            r = replaceAll(r, more[i], i + 1 < l ? more[i + 1] : "");
        }
        return r;
    }

    /**
     * Removes a without using regex *
     */
    public static String removeAll(CharSequence string, CharSequence pattern) {
        if (string == null) {
            throw new IllegalArgumentException("String input is null");
        }
        if (pattern != null) {
            StringBuilder builder = new StringBuilder(string);
            String p = pattern.toString();
            int l = pattern.length();
            for (int index; (index = builder.indexOf(p)) > -1;) {
                builder.delete(index, index + l);
            }
            return builder.toString();
        } else {
            return string.toString();
        }
    }

    /**
     * Splits the given string around matches of the given regular expression pattern.
     *
     * @see #getPattern(String)
     */
    public static String[] split(CharSequence string, String pattern, int limit) {
        return getPattern(pattern).split(string, limit);
    }

    /**
     * Splits the given string around matches of the given regular expression pattern.
     *
     * @see #getPattern(String)
     */
    public static String[] split(CharSequence string, String pattern) {
        return getPattern(pattern).split(string);
    }

    // --- StringEscapeUtils bridge ---

    public static String escapeCsv(String string) {
        return string == null ? null : StringEscapeUtils.escapeCsv(string);
    }

    public static String escapeHtml(String string) {
        return string == null ? null : StringUtils.replaceAll(
                org.apache.commons.lang3.StringEscapeUtils.escapeHtml4(string),
                "\\x22", "&#34;",  // double quote
                "\\x27", "&#39;"); // single quote
    }

    public static String escapeJava(String string) {
        return string == null ? null : StringEscapeUtils.escapeJava(string);
    }

    /**
     * Escapes the input string so that the resulting output can be used
     * inside a JavaScript string AND none of the characters in the output
     * need to be HTML escaped.
     * @param string
     * @return the escaped string, or null if the input was null
     * This is not the same as HTML escaping a JavaScript escaped string,
     * since the output can be used directly in a JavaScript string even
     * when HTML-unescaping won't happen.
     */
    public static String escapeJavaScript(String string) {
        if (string == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0, s = string.length(); i < s; ++ i) {
            char c = string.charAt(i);
            if (0x30 <= c && c <= 0x39
                    || 0x41 <= c && c <= 0x5A
                    || 0x61 <= c && c <= 0x7A) {
                sb.append(c);
            } else {
                String hex = Integer.toHexString(c);
                int hexLen = hex.length();
                if (c < 256) {
                    sb.append("\\x").append("00".substring(hexLen));
                } else {
                    sb.append("\\u").append("0000".substring(hexLen));
                }
                sb.append(hex);
            }
        }
        return sb.toString();
    }

    public static String escapeQuotes(String string) {
        return string == null ? null : string.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public static String escapeSql(String string) {
        return string == null ? null : StringEscapeUtils.escapeSql(string);
    }

    public static String escapeXml(String string) {
        return string == null ? null : StringEscapeUtils.escapeXml(string);
    }

    public static String unescapeCsv(String string) {
        return string == null ? null : StringEscapeUtils.unescapeCsv(string);
    }

    public static String unescapeHtml(String string) {
        return string == null ? null : StringEscapeUtils.unescapeHtml(string);
    }

    public static String unescapeJava(String string) {
        return string == null ? null : StringEscapeUtils.unescapeJava(string);
    }

    public static String unescapeJavaScript(String string) {
        return string == null ? null : StringEscapeUtils.unescapeJavaScript(string);
    }

    public static String unescapeXml(String string) {
        return string == null ? null : StringEscapeUtils.unescapeXml(string);
    }

    // --- StringUtils bridge ---

    /**
     * Joins the given list of strings with the given delimiter in between.
     */
    public static String join(List<String> strings, String delimiter) {
        return strings == null ? null : org.apache.commons.lang.StringUtils.join(strings, delimiter);
    }

    /**
     * Joins the given array of strings with the given delimiter in between.
     */
    public static String join(String[] strings, String delimiter) {
        return strings == null ? null : org.apache.commons.lang.StringUtils.join(strings, delimiter);
    }

    /**
     * Checks if a String is empty ("") or null.
     */
    public static boolean isEmpty(String string) {
        return org.apache.commons.lang.StringUtils.isEmpty(string);
    }

    /**
     * Checks if a String is whitespace, empty ("") or null.
     */
    public static boolean isBlank(String string) {
        return org.apache.commons.lang.StringUtils.isBlank(string);
    }

    /**
     * Null-safe comparison of two Strings, returning true if they are equal.
     */
    public static boolean equals(String str1, String str2) {
        return org.apache.commons.lang.StringUtils.equals(str1, str2);
    }

    /**
     * Null-safe, case-insensitive comparison of two Strings, returning true if they are equal.
     */
    public static boolean equalsIgnoreCase(String str1, String str2) {
        return org.apache.commons.lang.StringUtils.equalsIgnoreCase(str1, str2);
    }

    public static String stripHtml(CharSequence sequence) {
        Matcher m = StringUtils.getPattern("<[^>]*>").matcher(sequence);
        return m.replaceAll("");
    }

    /**
     * Ensures that the given {@code string} starts with the given
     * {@code delimiter}, adding it if necessary.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, does nothing.
     */
    public static String ensureStart(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else if (!string.startsWith(delimiter)) {
            string = delimiter + string;
        }
        return string;
    }

    /**
     * Ensures that the given {@code string} ends with the given
     * {@code delimiter}, adding it if necessary.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, does nothing.
     */
    public static String ensureEnd(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else if (!string.endsWith(delimiter)) {
            string = string + delimiter;
        }
        return string;
    }

    /**
     * Ensures that the given {@code string} starts and ends with
     * the given {@code delimiter}, adding them if necessary.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, does nothing.
     */
    public static String ensureSurrounding(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else {
            if (!string.startsWith(delimiter)) {
                string = delimiter + string;
            }
            if (!string.endsWith(delimiter)) {
                string = string + delimiter;
            }
            return string;
        }
    }

    /**
     * Removes the given {@code delimiter} if the given {@code string}
     * starts with it.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, returns the given {@code string}
     * as is.
     */
    public static String removeStart(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else if (string.startsWith(delimiter)) {
            string = string.substring(delimiter.length());
        }
        return string;
    }

    /**
     * Removes the given {@code delimiter} if the given {@code string}
     * ends with it.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, returns the given {@code string}
     * as is.
     */
    public static String removeEnd(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else if (string.endsWith(delimiter)) {
            string = string.substring(0, string.length() - delimiter.length());
        }
        return string;
    }

    /**
     * Removes the given {@code delimiter} if the given {@code string}
     * starts or ends with it.
     *
     * @param string If {@code null}, returns {@code null}.
     * @param delimiter If {@code null}, returns the given {@code string}
     * as is.
     */
    public static String removeSurrounding(String string, String delimiter) {
        if (string == null) {
            return null;
        } else if (delimiter == null) {
            return string;
        } else {
            if (string.startsWith(delimiter)) {
                string = string.substring(delimiter.length());
            }
            if (string.endsWith(delimiter)) {
                string = string.substring(0, string.length() - delimiter.length());
            }
            return string;
        }
    }

    /**
     * @param path If {@code null}, returns {@code null}.
     * @param servletPath If {@code null}, returns {@code null}.
     */
    public static String getPathInfo(String path, String servletPath) {
        if (path != null && servletPath != null) {
            path = ensureStart(path, "/");
            servletPath = removeEnd(ensureStart(servletPath, "/"), "/");

            if (path.startsWith(servletPath)) {
                String pathInfo = path.substring(servletPath.length());

                if (pathInfo.length() == 0) {
                    return "/";

                } else if (pathInfo.startsWith("/")) {
                    return pathInfo;
                }
            }
        }

        return null;
    }

    private static final int ASCII_SIZE = 256;
    private static final char[] LOWER_CASE_ASCII;

    static {
        LOWER_CASE_ASCII = new char[ASCII_SIZE];

        for (int i = 0; i < ASCII_SIZE; ++ i) {
            LOWER_CASE_ASCII[i] = (char) Character.toLowerCase(i);
        }
    }

    /**
     * @param string Can't be {@code null}.
     */
    public static String toLowerCaseAscii(String string) {
        char[] letters = string.toCharArray();
        char letter;

        for (int i = 0, length = letters.length; i < length; ++ i) {
            letter = letters[i];

            if (letter < ASCII_SIZE) {
                letters[i] = LOWER_CASE_ASCII[letter];
            }
        }

        return new String(letters);
    }

    /**
     * @param string Can't be {@code null}.
     */
    public static String trimAndCollapseWhitespaces(String string) {
        char[] letters = string.toCharArray();
        char letter;

        for (int i = 0, length = letters.length; i < length; ++ i) {
            letter = letters[i];

            if (Character.isWhitespace(letter) || Character.isSpaceChar(letter)) {
                letters[i] = Character.SPACE_SEPARATOR;
            }
        }

        return new String(letters).trim().replaceAll("\\s+", " ");
    }

    /**
     * Based on the <code>org.apache.commons.io.FilenameUtils#getName</code> signature
     *
     * Returns the file name of the given file path supporting both Windows and UNIX file separators
     * examples:
     * C:\foo\bar.txt -> bar.txt
     * \foo\bar.txt -> bar.txt
     * http://foo/bar.txt -> bar.txt
     * /foo/bar.txt -> bar.txt
     * ~/foo/bar.txt -> bar.txt
     * bar.txt -> bar.txt
     *
     * @param filePath
     */
    public static String getFileName(String filePath) {
        if (filePath == null) {
            return null;
        }

        int lastUnixPos = filePath.lastIndexOf('/');
        int lastWindowsPos = filePath.lastIndexOf('\\');
        int index = Math.max(lastUnixPos, lastWindowsPos);

        return filePath.substring(index + 1);
    }
}
