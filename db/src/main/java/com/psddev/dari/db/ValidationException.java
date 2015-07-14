package com.psddev.dari.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Thrown when there are invalid states that are about to be saved
 * to the database.
 */
public class ValidationException extends IllegalArgumentException {

    private static final long serialVersionUID = 1L;

    private final List<State> states;

    /** Creates an instance with the given invalid {@code states}. */
    public ValidationException(List<State> states) {
        this.states = states == null
                ? Collections.<State>emptyList()
                : Collections.unmodifiableList(states);
    }

    /** Returns an unmodifiable list of invalid states. */
    public List<State> getStates() {
        return states;
    }

    // --- Throwable support ---

    @Override
    public String getMessage() {
        StringBuilder message = new StringBuilder();
        for (State state : getStates()) {

            message.append('#');
            message.append(state.getId());
            message.append(':');

            for (ObjectField field : state.getErrorFields()) {
                String label = field.getLabel();
                for (String error : state.getErrors(field)) {
                    message.append(" [");
                    message.append(label);
                    message.append("] [");
                    message.append(error);
                    message.append("],");
                }
            }

            // Handle embedded objects.
            appendEmbeddedErrors(message, null, state, state.getType().getFields());

            message.setLength(message.length() - 1);
            message.append("; ");
        }

        if (message.length() > 0) {
            message.setLength(message.length() - 2);
            return message.toString();

        } else {
            return null;
        }
    }

    private void appendEmbeddedErrors(StringBuilder message, String messagePrefix, State state, Collection<ObjectField> fields) {
        if (messagePrefix == null) {
            messagePrefix = "";
        }
        for (ObjectField field : fields) {
            if (ObjectField.RECORD_TYPE.equals(field.getInternalItemType())) {
                boolean embedded = field.isEmbedded();
                for (ObjectType type : field.getTypes()) {
                    if (type != null && type.isEmbedded()) {
                        embedded = true;
                    }
                }
                if (embedded) {
                    Object fieldObj = state.getByPath(field.getInternalName());
                    String embeddedFieldLabel = field.getLabel();
                    Collection<Object> objects = new ArrayList<Object>();
                    if (fieldObj instanceof Collection<?>) {
                        for (Object obj : (Collection<?>) fieldObj) {
                            objects.add((Recordable) obj);
                        }
                    } else {
                        objects.add(fieldObj);
                    }
                    for (Object obj : objects) {
                        State embeddedState = State.getInstance(obj);
                        if (embeddedState != null) {
                            String embeddedMessagePrefix = embeddedFieldLabel + " #" + embeddedState.getId() + ": ";
                            for (ObjectField embeddedStateField : embeddedState.getErrorFields()) {
                                if (embeddedStateField != null) {
                                    String label = embeddedStateField.getLabel();
                                    for (String error : new LinkedHashSet<String>(embeddedState.getErrors(embeddedStateField))) {
                                        message.append(" [");
                                        message.append(messagePrefix);
                                        message.append(embeddedMessagePrefix);
                                        message.append(label);
                                        message.append("] [");
                                        message.append(error);
                                        message.append("],");
                                    }
                                }
                            }
                            appendEmbeddedErrors(message, messagePrefix + embeddedMessagePrefix, embeddedState, embeddedState.getType().getFields());
                        }
                    }
                }
            }
        }
    }

}
