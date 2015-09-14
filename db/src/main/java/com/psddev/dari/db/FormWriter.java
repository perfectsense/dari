package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;

import com.psddev.dari.util.HtmlWriter;

/**
 * Writer that specializes in processing HTML form inputs.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class FormWriter extends HtmlWriter {

    private FormLabelRenderer labelRenderer;
    private FormInputProcessor defaultInputProcessor;
    private Map<String, FormInputProcessor> inputProcessors;

    /** Creates an instance that writes to the given {@code writer}. */
    public FormWriter(Writer writer) {
        super(writer);
    }

    /**
     * Returns the label renderer. If {@code null}, it's set to the
     * {@linkplain FormLabelRenderer.Default default} before returning.
     *
     * @return Can't be {@code null}.
     */
    public FormLabelRenderer getLabelRenderer() {
        if (labelRenderer == null) {
            setLabelRenderer(new FormLabelRenderer.Default());
        }
        return labelRenderer;
    }

    /** Sets the label renderer. */
    public void setLabelRenderer(FormLabelRenderer labelRenderer) {
        this.labelRenderer = labelRenderer;
    }

    /**
     * Returns the default form input processor. If {@code null}, it's set to
     * the {@linkplain FormInputProcessor.Default default} before returning.
     *
     * @return Can't be {@code null}.
     */
    public FormInputProcessor getDefaultInputProcessor() {
        if (defaultInputProcessor == null) {
            setDefaultInputProcessor(new FormInputProcessor.Default());
        }
        return defaultInputProcessor;
    }

    /** Sets the default form input processor. */
    public void setDefaultInputProcessor(FormInputProcessor defaultInputProcessor) {
        this.defaultInputProcessor = defaultInputProcessor;
    }

    /**
     * Returns the map of all input processors.
     *
     * @return Can't be {@code null}.
     */
    public Map<String, FormInputProcessor> getInputProcessors() {
        if (inputProcessors == null) {
            setInputProcessors(new HashMap<String, FormInputProcessor>());
        }
        return inputProcessors;
    }

    /** Sets the map of all input processors. */
    public void setInputProcessors(Map<String, FormInputProcessor> inputProcessors) {
        this.inputProcessors = inputProcessors;
    }

    /** Puts all standard input processors. */
    public void putAllStandardInputProcessors() {
        Map<String, FormInputProcessor> inputProcessors = getInputProcessors();
        inputProcessors.put(ObjectField.BOOLEAN_TYPE, new FormInputProcessor.ForBoolean());
        inputProcessors.put(ObjectField.DATE_TYPE, new FormInputProcessor.ForDate());
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor.ForListRecord(this));
        inputProcessors.put(ObjectField.LIST_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor.ForListText());
        inputProcessors.put(ObjectField.NUMBER_TYPE, new FormInputProcessor.ForText());
        inputProcessors.put(ObjectField.RECORD_TYPE, new FormInputProcessor.ForRecord(this));
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.RECORD_TYPE, new FormInputProcessor.ForSetRecord(this));
        inputProcessors.put(ObjectField.SET_TYPE + "/" + ObjectField.TEXT_TYPE, new FormInputProcessor.ForSetText());
        inputProcessors.put(ObjectField.TEXT_TYPE, new FormInputProcessor.ForText());
    }

    // Finds the type associated with the given state, throwing
    // errors if there isn't one.
    private ObjectType findType(State state) {
        com.psddev.dari.util.ErrorUtils.errorIfNull(state, "state");

        ObjectType type = state.getType();
        if (type == null) {
            throw new IllegalStateException("Given state isn't typed!");
        }

        return type;
    }

    // Finds the field associated with the given fieldName in the
    // given type, throwing errors if there isn't one.
    private ObjectField findField(ObjectType type, String fieldName) {
        ObjectField field = type.getState().getDatabase().getEnvironment().getField(fieldName);

        if (field == null) {
            field = type.getField(fieldName);

            if (field == null) {
                throw new IllegalArgumentException(String.format(
                        "[%s] doesn't contain [%s]!", type.getLabel(), fieldName));
            }
        }

        return field;
    }

    // Finds the processor associated with the given field, returning
    // null if there isn't one.
    protected FormInputProcessor findInputProcessor(ObjectField field) {
        String type = field.getInternalType();

        if (type != null) {
            Map<String, FormInputProcessor> processors = getInputProcessors();

            while (true) {
                FormInputProcessor processor = processors.get(type);
                if (processor != null) {
                    return processor;
                }

                int slashAt = type.lastIndexOf('/');
                if (slashAt < 0) {
                    break;
                } else {
                    type = type.substring(0, slashAt);
                }
            }
        }

        return null;
    }

    /**
     * Writes the given {@code field} in the given {@code state}. This
     * method is the underlying implementation for both {@link #inputs}
     * and {@link #allInputs}.
     */
    protected void writeField(State state, ObjectField field, FormInputProcessor processor) throws IOException {
        String fieldName = field.getInternalName();
        String inputId = "i" + UUID.randomUUID().toString().replace("-", "");
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor();
        }

        write(getLabelRenderer().display(inputId, inputName, field));
        write(processor.display(inputId, inputName, field, state));
    }

    /**
     * Writes the inputs for the given {@code fieldNames} in the given
     * {@code state}.
     */
    public HtmlWriter inputs(State state, String... fieldNames) throws IOException {
        if (fieldNames != null) {
            ObjectType type = findType(state);

            for (String fieldName : fieldNames) {
                ObjectField field = findField(type, fieldName);

                writeField(state, field, findInputProcessor(field));
            }
        }

        return this;
    }

    /**
     * Writes all inputs in the given {@code state}.
     */
    public HtmlWriter allInputs(State state) throws IOException {
        ObjectType type = findType(state);

        for (ObjectField field : type.getFields()) {
            writeField(state, field, findInputProcessor(field));
        }

        return this;
    }

    /**
     * Updates the given {@code field} in the given {@code state}
     * using the given {@code request}. This method is the underlying
     * implementation for both {@link #update} and {@link #updateAll}.
     */
    protected void updateField(State state, HttpServletRequest request, ObjectField field, FormInputProcessor processor) {
        String fieldName = field.getInternalName();
        String inputName = state.getId() + "/" + fieldName;

        if (processor == null) {
            processor = getDefaultInputProcessor();
        }

        state.put(fieldName, processor.update(inputName, field, request));
    }

    /**
     * Updates the given {@code fieldNames} in the given {@code state}
     * using the given {@code request}.
     */
    public void update(State state, HttpServletRequest request, String... fieldNames) {
        com.psddev.dari.util.ErrorUtils.errorIfNull(request, "request");

        if (fieldNames != null) {
            ObjectType type = findType(state);

            for (String fieldName : fieldNames) {
                ObjectField field = findField(type, fieldName);

                updateField(state, request, field, findInputProcessor(field));
            }
        }
    }

    /**
     * Updates all fields in the given {@code state} using the given
     * {@code request}.
     */
    public void updateAll(State state, HttpServletRequest request) {
        com.psddev.dari.util.ErrorUtils.errorIfNull(request, "request");

        ObjectType type = findType(state);
        for (ObjectField field : type.getFields()) {
            updateField(state, request, field, findInputProcessor(field));
        }
    }
}
