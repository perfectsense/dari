package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @deprecated No replacement.
 */
@Deprecated
public abstract class AbstractForm extends FormWriter implements com.psddev.dari.util.FormProcessor {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractForm.class);

    public AbstractForm() {
        this(null);
    }

    public AbstractForm(Writer writer) {
        super(writer);
    }

    @Override
    public final Object process(HttpServletRequest request, HttpServletResponse response) throws IOException {

        State state = getState(request);

        if (com.psddev.dari.util.FormFilter.Static.isFormSubmitted(this, request)) {
            try {
                InputTag.Static.updateObject(this, state, request);

                processState(state, request, response);
                return state;

            } catch (IOException e) {
                throw e;

            } catch (Throwable e) {
                LOGGER.debug(e.getMessage(), e);
            }
        }

        Writer originalDelegate = getDelegate();
        try {
            setDelegate(response.getWriter());
            writeState(state);

        } finally {
            setDelegate(originalDelegate);
        }

        return state;
    }

    protected abstract State getState(HttpServletRequest request);

    protected abstract void processState(State state, HttpServletRequest request, HttpServletResponse response) throws IOException;

    protected abstract void writeState(State state) throws IOException;
}
