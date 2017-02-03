package com.psddev.dari.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;

/**
 * For writing invisible {@code <span>} blocks without disrupting the natural
 * page layout by delaying the writes until the appropriate points in the HTML.
 */
public class LazyWriter extends Writer {

    private static final String ATTRIBUTE_PREFIX = LazyWriter.class.getName() + ".";
    private static final String IN_BODY_ATTRIBUTE = ATTRIBUTE_PREFIX + "inBody";

    private final HttpServletRequest request;
    private final Writer delegate;
    private final StringBuilder lazy = new StringBuilder();
    private final StringBuilder pending = new StringBuilder();

    private boolean inString;
    private boolean stringEscaping;
    private char stringStartLetter;

    private boolean commentStarting;
    private boolean inSingleComment;
    private boolean inMultiComment;
    private boolean multiCommentEnding;

    private boolean inTag;
    private final StringBuilder tagName = new StringBuilder();
    private boolean tagNameFound;
    private boolean inScriptOrStyle;
    private boolean inTextarea;
    private boolean inBody;

    public LazyWriter(HttpServletRequest request, Writer delegate) {
        this.request = request;
        this.delegate = delegate;
    }

    public LazyWriter(Writer delegate, boolean inBody) {
        this.request = null;
        this.delegate = delegate;
        this.inBody = inBody;
    }

    private boolean isInBody() {
        return inBody
                || (request != null
                && Boolean.TRUE.equals(request.getAttribute(IN_BODY_ATTRIBUTE)));
    }

    @Override
    public void write(char[] buffer, int offset, int length) throws IOException {
        for (length += offset; offset < length; ++ offset) {
            char letter = buffer[offset];

            pending.append(letter);

            if (inString) {
                if (stringEscaping) {
                    stringEscaping = false;

                } else if (letter == '\\') {
                    stringEscaping = true;

                } else if (letter == stringStartLetter) {
                    inString = false;
                }

            } else if (commentStarting) {
                commentStarting = false;

                if (letter == '/') {
                    inSingleComment = true;

                } else if (letter == '*') {
                    inMultiComment = true;
                }

            } else if (inSingleComment) {
                if (letter == '\r' || letter == '\n') {
                    inSingleComment = false;
                }

            } else if (inMultiComment) {
                if (letter == '*') {
                    multiCommentEnding = true;

                } else if (multiCommentEnding && letter == '/') {
                    inMultiComment = false;
                    multiCommentEnding = false;

                } else {
                    multiCommentEnding = false;
                }

            } else if (letter == '<') {
                inTag = true;
                tagName.setLength(0);
                tagNameFound = false;

            } else if (inTag) {
                boolean endTag = letter == '>';

                if (endTag || Character.isWhitespace(letter)) {
                    tagNameFound = true;

                } else if (!tagNameFound) {
                    tagName.append(letter);
                }

                if (endTag) {
                    String tagNameLc = tagName.toString().toLowerCase(Locale.ENGLISH);

                    if ("script".equals(tagNameLc) || "style".equals(tagNameLc)) {
                        inScriptOrStyle = true;

                    } else if ("/script".equals(tagNameLc) || "/style".equals(tagNameLc)) {
                        inScriptOrStyle = false;

                    } else if ("textarea".equals(tagNameLc)) {
                        inTextarea = true;

                    } else if ("/textarea".equals(tagNameLc)) {
                        inTextarea = false;

                    } else if ("body".equals(tagNameLc)) {
                        inBody = true;

                        if (request != null) {
                            request.setAttribute(IN_BODY_ATTRIBUTE, Boolean.TRUE);
                        }
                    }

                    if (!inScriptOrStyle) {
                        writePending();
                    }

                    if (isInBody() && lazy.length() > 0) {
                        delegate.append(lazy);
                        lazy.setLength(0);
                    }

                    inTag = false;

                } else if (letter == '\'' || letter == '"') {
                    inString = true;
                    stringStartLetter = letter;
                }

            } else if (inScriptOrStyle) {
                if (letter == '\'' || letter == '"') {
                    inString = true;
                    stringStartLetter = letter;

                } else if (letter == '/') {
                    commentStarting = true;
                }
            }
        }
    }

    public void writeLazily(String string) throws IOException {
        if (!isInBody() || inTag || inScriptOrStyle || inTextarea) {
            lazy.append(string);

        } else {
            delegate.write(string);
        }
    }

    public void writePending() throws IOException {
        if (pending.length() == 0) {
            return;
        }

        delegate.append(pending);
        pending.setLength(0);
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        writePending();
        delegate.close();
    }
}
