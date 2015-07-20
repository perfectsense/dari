package com.psddev.dari.db;

import java.awt.Image;
import java.beans.BeanDescriptor;
import java.beans.BeanInfo;
import java.beans.EventSetDescriptor;
import java.beans.MethodDescriptor;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.SettingsException;
import com.psddev.dari.util.StringUtils;
import com.psddev.dari.util.TypeDefinition;

/** Represents a generic record. */
public class Record implements BeanInfo, Cloneable, Comparable<Record>, HtmlObject, Recordable {

    /**
     * Creates a blank instance. This method is protected so that the class
     * can behave like an abstract class, and still allow instantiation
     * through reflection when necessary.
     */
    protected Record() {
        try {
            if (ObjectType.Static.hasAfterCreate(getClass())) {
                getState().fireTrigger(new AfterCreateTrigger());
            }

        } catch (SettingsException error) {
            // Ignore the error caused by non-configured default database.
        }
    }

    private static class AfterCreateTrigger extends TriggerOnce {

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record) {
                ((Record) object).afterCreate();
            }
        }

        @Override
        public boolean isMissing(Class<?> cls) {
            return !hasMethod(cls, "afterCreate");
        }
    }

    /**
     * Triggers right after this record is created. Default implementation
     * of this method doesn't do anything.
     */
    protected void afterCreate() {
    }

    /**
     * Triggers when this record is being validated. Default implementation
     * of this method doesn't do anything.
     */
    protected void onValidate() {
    }

    /**
     * Triggers when there's a duplicate value on the given unique
     * {@code index}. Default implementation of this method doesn't do
     * anything and always returns {@code false}.
     *
     * @return {@code true} if this method did anything to correct the error.
     */
    protected boolean onDuplicate(ObjectIndex index) {
        return false;
    }

    /**
     * Triggers right before this record is saved to the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void beforeSave() {
        beforeSave(getState().getDatabase());
    }

    /**
     * Triggers right after this record is saved to the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void afterSave() {
        afterSave(getState().getDatabase());
    }

    /**
     * Triggers right before this record is deleted in the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void beforeDelete() {
        beforeDelete(getState().getDatabase());
    }

    /**
     * Triggers right after this record is deleted in the given
     * {@code database}. Default implementation of this method
     * doesn't do anything.
     */
    protected void afterDelete() {
        afterDelete(getState().getDatabase());
    }

    // --- BeanInfo support ---

    @Override
    public BeanInfo[] getAdditionalBeanInfo() {
        State state = getState();
        return state.getDatabase().getEnvironment().getAdditionalBeanInfoByType(state.getType());
    }

    @Override
    public BeanDescriptor getBeanDescriptor() {
        return null;
    }

    @Override
    public int getDefaultEventIndex() {
        return -1;
    }

    @Override
    public int getDefaultPropertyIndex() {
        return -1;
    }

    @Override
    public EventSetDescriptor[] getEventSetDescriptors() {
        return null;
    }

    @Override
    public Image getIcon(int iconKind) {
        return null;
    }

    @Override
    public MethodDescriptor[] getMethodDescriptors() {
        return null;
    }

    @Override
    public PropertyDescriptor[] getPropertyDescriptors() {
        return null;
    }

    private Object getDynamicProperty(int index) {
        ObjectType type = getState().getDatabase().getEnvironment().getTypeByDynamicPropertyIndex(index);

        if (type != null) {
            Class<?> objectClass = type.getObjectClass();

            if (objectClass != null) {
                return as(objectClass);
            }
        }

        return null;
    }

    // CHECKSTYLE:OFF
    @SuppressWarnings("unused") private Object getDynamicProperty0() { return getDynamicProperty(0); }
    @SuppressWarnings("unused") private Object getDynamicProperty1() { return getDynamicProperty(1); }
    @SuppressWarnings("unused") private Object getDynamicProperty2() { return getDynamicProperty(2); }
    @SuppressWarnings("unused") private Object getDynamicProperty3() { return getDynamicProperty(3); }
    @SuppressWarnings("unused") private Object getDynamicProperty4() { return getDynamicProperty(4); }
    @SuppressWarnings("unused") private Object getDynamicProperty5() { return getDynamicProperty(5); }
    @SuppressWarnings("unused") private Object getDynamicProperty6() { return getDynamicProperty(6); }
    @SuppressWarnings("unused") private Object getDynamicProperty7() { return getDynamicProperty(7); }
    @SuppressWarnings("unused") private Object getDynamicProperty8() { return getDynamicProperty(8); }
    @SuppressWarnings("unused") private Object getDynamicProperty9() { return getDynamicProperty(9); }
    @SuppressWarnings("unused") private Object getDynamicProperty10() { return getDynamicProperty(10); }
    @SuppressWarnings("unused") private Object getDynamicProperty11() { return getDynamicProperty(11); }
    @SuppressWarnings("unused") private Object getDynamicProperty12() { return getDynamicProperty(12); }
    @SuppressWarnings("unused") private Object getDynamicProperty13() { return getDynamicProperty(13); }
    @SuppressWarnings("unused") private Object getDynamicProperty14() { return getDynamicProperty(14); }
    @SuppressWarnings("unused") private Object getDynamicProperty15() { return getDynamicProperty(15); }
    @SuppressWarnings("unused") private Object getDynamicProperty16() { return getDynamicProperty(16); }
    @SuppressWarnings("unused") private Object getDynamicProperty17() { return getDynamicProperty(17); }
    @SuppressWarnings("unused") private Object getDynamicProperty18() { return getDynamicProperty(18); }
    @SuppressWarnings("unused") private Object getDynamicProperty19() { return getDynamicProperty(19); }
    @SuppressWarnings("unused") private Object getDynamicProperty20() { return getDynamicProperty(20); }
    @SuppressWarnings("unused") private Object getDynamicProperty21() { return getDynamicProperty(21); }
    @SuppressWarnings("unused") private Object getDynamicProperty22() { return getDynamicProperty(22); }
    @SuppressWarnings("unused") private Object getDynamicProperty23() { return getDynamicProperty(23); }
    @SuppressWarnings("unused") private Object getDynamicProperty24() { return getDynamicProperty(24); }
    @SuppressWarnings("unused") private Object getDynamicProperty25() { return getDynamicProperty(25); }
    @SuppressWarnings("unused") private Object getDynamicProperty26() { return getDynamicProperty(26); }
    @SuppressWarnings("unused") private Object getDynamicProperty27() { return getDynamicProperty(27); }
    @SuppressWarnings("unused") private Object getDynamicProperty28() { return getDynamicProperty(28); }
    @SuppressWarnings("unused") private Object getDynamicProperty29() { return getDynamicProperty(29); }
    @SuppressWarnings("unused") private Object getDynamicProperty30() { return getDynamicProperty(30); }
    @SuppressWarnings("unused") private Object getDynamicProperty31() { return getDynamicProperty(31); }
    @SuppressWarnings("unused") private Object getDynamicProperty32() { return getDynamicProperty(32); }
    @SuppressWarnings("unused") private Object getDynamicProperty33() { return getDynamicProperty(33); }
    @SuppressWarnings("unused") private Object getDynamicProperty34() { return getDynamicProperty(34); }
    @SuppressWarnings("unused") private Object getDynamicProperty35() { return getDynamicProperty(35); }
    @SuppressWarnings("unused") private Object getDynamicProperty36() { return getDynamicProperty(36); }
    @SuppressWarnings("unused") private Object getDynamicProperty37() { return getDynamicProperty(37); }
    @SuppressWarnings("unused") private Object getDynamicProperty38() { return getDynamicProperty(38); }
    @SuppressWarnings("unused") private Object getDynamicProperty39() { return getDynamicProperty(39); }
    @SuppressWarnings("unused") private Object getDynamicProperty40() { return getDynamicProperty(40); }
    @SuppressWarnings("unused") private Object getDynamicProperty41() { return getDynamicProperty(41); }
    @SuppressWarnings("unused") private Object getDynamicProperty42() { return getDynamicProperty(42); }
    @SuppressWarnings("unused") private Object getDynamicProperty43() { return getDynamicProperty(43); }
    @SuppressWarnings("unused") private Object getDynamicProperty44() { return getDynamicProperty(44); }
    @SuppressWarnings("unused") private Object getDynamicProperty45() { return getDynamicProperty(45); }
    @SuppressWarnings("unused") private Object getDynamicProperty46() { return getDynamicProperty(46); }
    @SuppressWarnings("unused") private Object getDynamicProperty47() { return getDynamicProperty(47); }
    @SuppressWarnings("unused") private Object getDynamicProperty48() { return getDynamicProperty(48); }
    @SuppressWarnings("unused") private Object getDynamicProperty49() { return getDynamicProperty(49); }
    // CHECKSTYLE:ON

    // --- Cloneable support ---

    @Override
    public Object clone() {
        Object clone = TypeDefinition.getInstance(getClass()).newInstance();
        State cloneState = State.getInstance(clone);
        cloneState.setDatabase(state.getDatabase());
        cloneState.setStatus(state.getStatus());
        cloneState.setValues(state.getSimpleValues());
        return clone;
    }

    // --- Comparable support ---

    @Override
    public int compareTo(Record other) {
        return ObjectUtils.compare(getLabel(), other.getLabel(), true);
    }

    // --- HtmlObject support ---

    @Override
    public void format(HtmlWriter writer) throws IOException {
        writer.writeStart("pre");

            State state = getState();
            ObjectType type = state.getType();
            if (type != null) {
                writer.writeHtml(type.getInternalName());
                writer.writeHtml(": ");
            }

            writer.writeStart("a",
                    "target", "_blank",
                    "href", StringUtils.addQueryParameters(
                            "/_debug/query",
                            "where", "id = " + state.getId(),
                            "action", "Run"));
                writer.writeHtml(getLabel());
            writer.writeEnd();
            writer.writeElement("br");

            writer.writeHtml(ObjectUtils.toJson(state.getSimpleValues(), true));

        writer.writeEnd();
    }

    // --- Recordable support ---

    private transient State state;

    @Override
    public State getState() {
        if (state == null) {
            setState(new State());
        }
        return state;
    }

    @Override
    public void setState(State state) {
        if (this.state != null) {
            this.state.unlinkObject(this);
        }
        this.state = state;
        this.state.linkObject(this);
    }

    @Override
    public <T> T as(Class<T> modificationClass) {
        return getState().as(modificationClass);
    }

    // --- Object support ---

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;

        } else if (object != null) {
            try {
                return getState().equals(State.getInstance(object));

            } catch (IllegalArgumentException error) {
                return false;
            }

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return getState().hashCode();
    }

    @Override
    public String toString() {
        return getState().toString();
    }

    // --- PredicateParser bridge ---

    /** @see PredicateParser.Static#evaluate(Object, Predicate) */
    public boolean is(Predicate predicate) {
        return PredicateParser.Static.evaluate(this, predicate);
    }

    /** @see PredicateParser.Static#evaluate(Object, String, Object...) */
    public boolean is(String predicateString, Object... parameters) {
        return PredicateParser.Static.evaluate(this, predicateString, parameters);
    }

    // --- Query bridge ---

    /** @see Query#from */
    public <T> Query<T> queryFrom(Class<T> objectClass) {
        Query<T> query = Query.from(objectClass);
        query.setDatabase(getState().getDatabase());
        return query;
    }

    // --- State bridge ---

    /** @see State#beginWrites() */
    public boolean beginWrites() {
        return getState().beginWrites();
    }

    /** @see State#commitWrites() */
    public boolean commitWrites() {
        return getState().commitWrites();
    }

    /** @see State#delete() */
    public void delete() {
        getState().delete();
    }

    /** @see State#endWrites() */
    public boolean endWrites() {
        return getState().endWrites();
    }

    /** @see State#getId() */
    public UUID getId() {
        return getState().getId();
    }

    /** @see State#getExtra(String) */
    public Object getExtra(String name) {
        return getState().getExtra(name);
    }

    /**
     * Returns a descriptive label for this record.
     *
     * @see State#getDefaultLabel
     */
    public String getLabel() {
        return getState().getDefaultLabel();
    }

    /** @see State#save() */
    public void save() {
        getState().save();
    }

    /** @see State#saveImmediately */
    public void saveImmediately() {
        getState().saveImmediately();
    }

    /** @see State#saveEventually */
    public void saveEventually() {
        getState().saveEventually();
    }

    public void saveUniquely() {
        getState().saveUniquely();
    }

    // --- JSTL support ---

    public Map<String, Object> getAs() {
        return getState().getAs();
    }

    // --- Deprecated ---

    /** @deprecated Use {@link #beforeSave()} instead. */
    @Deprecated
    protected void beforeSave(Database database) {
    }

    /** @deprecated Use {@link #afterSave} instead. */
    @Deprecated
    protected void afterSave(Database database) {
    }

    /** @deprecated Use {@link #beforeDelete()} instead. */
    @Deprecated
    protected void beforeDelete(Database database) {
    }

    /** @deprecated Use {@link #afterDelete} instead. */
    @Deprecated
    protected void afterDelete(Database database) {
    }

    /** @deprecated Use {@link #getAs} instead. */
    @Deprecated
    public Map<String, Object> getModifications() {
        return getAs();
    }
}
