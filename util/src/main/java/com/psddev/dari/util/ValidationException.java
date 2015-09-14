package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

// CHECKSTYLE:OFF
/**
 * For when there are multiple illegal arguments.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class ValidationException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final List<String> messages;

    /**
     * Creates an exception without any messages.
     */
    public ValidationException() {
        super();

        this.messages = Collections.emptyList();
    }

    /**
     * Creates an exception with the given {@code messages}.
     *
     * @param messages May be {@code null}.
     */
    public ValidationException(List<String> messages) {
        super();

        this.messages = messages != null ?
                Collections.unmodifiableList(new ArrayList<String>(messages)) :
                Collections.<String>emptyList();
    }

    /**
     * Creates an exception with the given {@code messages} and {@code cause}.
     *
     * @param messages May be {@code null}.
     * @param cause May be {@code null}.
     */
    public ValidationException(List<String> messages, Throwable cause) {
        super(cause);

        this.messages = messages != null ?
                Collections.unmodifiableList(new ArrayList<String>(messages)) :
                Collections.<String>emptyList();
    }

    /**
     * Creates an exception with the given {@code cause}.
     *
     * @param cause May be {@code null}.
     */
    public ValidationException(Throwable cause) {
        super(cause);

        this.messages = Collections.emptyList();
    }

    /**
     * Returns the messages.
     *
     * @param Never {@code null}. Immutable.
     */
    public List<String> getMessages() {
        return messages;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();

        for (String m : getMessages()) {
            if (!ObjectUtils.isBlank(m)) {
                message.append(m);
                message.append('\n');
            }
        }

        return message.toString();
    }
}
