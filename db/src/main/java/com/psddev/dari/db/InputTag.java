package com.psddev.dari.db;

import java.io.IOException;
import java.io.Writer;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.jsp.JspException;
import javax.servlet.jsp.tagext.Tag;
import javax.servlet.jsp.tagext.TagSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.TypeDefinition;

// CHECKSTYLE:OFF
/**
 * @deprecated No replacement.
 */
@Deprecated
@SuppressWarnings("serial")
public class InputTag extends TagSupport {

    protected static final Logger LOGGER = LoggerFactory.getLogger(InputTag.class);

    private static final String PARAMETER_PREFIX = "_i.";
    public static final String FIELDS_PARAMETER = PARAMETER_PREFIX + "f";

    private static final String ATTRIBUTE_PREFIX = InputTag.class.getName() + ".";
    private static final String WRITER_ATTRIBUTE_PREFIX = ATTRIBUTE_PREFIX + "writer/";

    private Class<? extends FormWriter> writerClass;
    private State state;
    private String name;

    @SuppressWarnings("unchecked")
    public void setWriter(Object writer) {

        String writerClassName = String.valueOf(writer);
        Class<?> wc = ObjectUtils.getClassByName(writerClassName);

        com.psddev.dari.util.ErrorUtils.errorIf(wc == null, writerClassName, "isn't a valid class name!");
        com.psddev.dari.util.ErrorUtils.errorIf(!FormWriter.class.isAssignableFrom(wc),
                wc.getName(), "doesn't extend [" + FormWriter.class.getName() + "]!");

        this.writerClass = (Class<? extends FormWriter>) wc;
    }

    public void setObject(Object object) {
        com.psddev.dari.util.ErrorUtils.errorIf(object instanceof State || object instanceof Recordable,
                object != null ? object.getClass().getName() : null,
                        "doesn't implement [" + Recordable.class.getName() + "]!");

        this.state = State.getInstance(object);
    }

    public void setName(String name) {
        com.psddev.dari.util.ErrorUtils.errorIf(ObjectUtils.isBlank(name), name, "isn't a valid field namme!");

        this.name = name;
    }

    @Override
    public int doStartTag() throws JspException {
        try {
            if (name != null) {

                FormWriter formWriter = null;
                State state = null;

                // if the writer is set directly
                if (writerClass != null) {
                    // get an instance
                    formWriter = Static.getWriter(writerClass, (HttpServletRequest) pageContext.getRequest());

                } else {
                    // grab it from parent form tag
                    com.psddev.dari.util.FormTag formTag = getParentFormTag();
                    if (formTag != null) {
                        com.psddev.dari.util.FormProcessor processor = formTag.getProcessorInstance();
                        if (processor instanceof FormWriter) {
                            formWriter = (FormWriter) processor;
                        }
                    }
                }

                // if the state is set directly
                if (this.state != null) {
                    state = this.state;

                } else {
                    // grab it from the parent form tag
                    com.psddev.dari.util.FormTag formTag = getParentFormTag();
                    if (formTag != null) {
                        Object object = formTag.getResult();
                        state = State.getInstance(object);
                    }
                }

                if (formWriter != null && state != null) {

                    Writer originalDelegate = formWriter.getDelegate();
                    try {
                        formWriter.setDelegate(pageContext.getOut());

                        formWriter.inputs(state, name);
                        formWriter.writeElement("input",
                                "type", "hidden",
                                "name", FIELDS_PARAMETER,
                                "value", name);

                    } finally {
                        formWriter.setDelegate(originalDelegate);
                    }

                } else {
                    String formId = null;
                    com.psddev.dari.util.FormTag formTag = getParentFormTag();
                    if (formTag != null) {
                        formId = formTag.getId();
                    }

                    LOGGER.debug("Could not write input field [{}] for form [{}]" +
                            " because the writer or object is null",
                            new Object[] {name, formId});
                }

            }

        } catch (IOException error) {
            throw new JspException(error);

        }

        return SKIP_BODY;
    }

    private com.psddev.dari.util.FormTag getParentFormTag() {
        Tag parent = getParent();

        while (parent != null) {
            if (parent instanceof com.psddev.dari.util.FormTag) {
                return (com.psddev.dari.util.FormTag) parent;
            }
            parent = parent.getParent();
        }

        return null;
    }

    /**
     * @deprecated No replacement.
     */
    @Deprecated
    public static final class Static {

        /**
         * Updates all the fields for {@code object} that were submitted on the
         * {@code request} using the given {@code writer}.
         *
         * @param writer
         * @param object
         * @param request
         */
        public static void updateObject(FormWriter writer, Object object, HttpServletRequest request) {

            String[] fieldNames = request.getParameterValues(FIELDS_PARAMETER);
            State state = State.getInstance(object);

            if (writer != null && state != null && fieldNames != null) {
                writer.update(state, request, fieldNames);
            }
        }

        private static FormWriter getWriter(Class<? extends FormWriter> writerClass, HttpServletRequest request) {
            if (writerClass == null) {
                return null;
            }

            FormWriter writer = (FormWriter) request.getAttribute(WRITER_ATTRIBUTE_PREFIX + writerClass.getName());

            if (writer == null) {
                writer = TypeDefinition.getInstance(writerClass).newInstance();
                request.setAttribute(WRITER_ATTRIBUTE_PREFIX + writerClass.getName(), writer);
            }

            return writer;
        }
    }
}
