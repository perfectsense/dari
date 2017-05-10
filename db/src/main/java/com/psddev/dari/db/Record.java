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
     * Triggers right before this record is committed to the database.
     *
     * <p>Default implementation doesn't do anything.</p>
     */
    protected void beforeCommit() {
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

    /**
     * Fire {@link MessageTrigger} with the supplied parameters.
     */
    public final void sendMessage(String key, Object... args) {
        getState().fireTrigger(new MessageTrigger(key, args));
    }

    /**
     * Triggers with the parameters of {@link #sendMessage}. Default
     * implementation of this method doesn't do anything.
     */
    protected void receiveMessage(String key, Object... args) {
    }

    private static class MessageTrigger extends TriggerOnce {

        private final String key;
        private final Object[] args;

        MessageTrigger(String key, Object[] args) {
            this.key = key;
            this.args = args;
        }

        @Override
        protected void executeOnce(Object object) {
            if (object instanceof Record && key != null) {
                if (args == null || args.length == 0) {
                    ((Record) object).receiveMessage(key);
                } else {
                    ((Record) object).receiveMessage(key, args);
                }
            }
        }
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
    @SuppressWarnings("unused") private Object getDynamicProperty50() { return getDynamicProperty(50); }
    @SuppressWarnings("unused") private Object getDynamicProperty51() { return getDynamicProperty(51); }
    @SuppressWarnings("unused") private Object getDynamicProperty52() { return getDynamicProperty(52); }
    @SuppressWarnings("unused") private Object getDynamicProperty53() { return getDynamicProperty(53); }
    @SuppressWarnings("unused") private Object getDynamicProperty54() { return getDynamicProperty(54); }
    @SuppressWarnings("unused") private Object getDynamicProperty55() { return getDynamicProperty(55); }
    @SuppressWarnings("unused") private Object getDynamicProperty56() { return getDynamicProperty(56); }
    @SuppressWarnings("unused") private Object getDynamicProperty57() { return getDynamicProperty(57); }
    @SuppressWarnings("unused") private Object getDynamicProperty58() { return getDynamicProperty(58); }
    @SuppressWarnings("unused") private Object getDynamicProperty59() { return getDynamicProperty(59); }
    @SuppressWarnings("unused") private Object getDynamicProperty60() { return getDynamicProperty(60); }
    @SuppressWarnings("unused") private Object getDynamicProperty61() { return getDynamicProperty(61); }
    @SuppressWarnings("unused") private Object getDynamicProperty62() { return getDynamicProperty(62); }
    @SuppressWarnings("unused") private Object getDynamicProperty63() { return getDynamicProperty(63); }
    @SuppressWarnings("unused") private Object getDynamicProperty64() { return getDynamicProperty(64); }
    @SuppressWarnings("unused") private Object getDynamicProperty65() { return getDynamicProperty(65); }
    @SuppressWarnings("unused") private Object getDynamicProperty66() { return getDynamicProperty(66); }
    @SuppressWarnings("unused") private Object getDynamicProperty67() { return getDynamicProperty(67); }
    @SuppressWarnings("unused") private Object getDynamicProperty68() { return getDynamicProperty(68); }
    @SuppressWarnings("unused") private Object getDynamicProperty69() { return getDynamicProperty(69); }
    @SuppressWarnings("unused") private Object getDynamicProperty70() { return getDynamicProperty(70); }
    @SuppressWarnings("unused") private Object getDynamicProperty71() { return getDynamicProperty(71); }
    @SuppressWarnings("unused") private Object getDynamicProperty72() { return getDynamicProperty(72); }
    @SuppressWarnings("unused") private Object getDynamicProperty73() { return getDynamicProperty(73); }
    @SuppressWarnings("unused") private Object getDynamicProperty74() { return getDynamicProperty(74); }
    @SuppressWarnings("unused") private Object getDynamicProperty75() { return getDynamicProperty(75); }
    @SuppressWarnings("unused") private Object getDynamicProperty76() { return getDynamicProperty(76); }
    @SuppressWarnings("unused") private Object getDynamicProperty77() { return getDynamicProperty(77); }
    @SuppressWarnings("unused") private Object getDynamicProperty78() { return getDynamicProperty(78); }
    @SuppressWarnings("unused") private Object getDynamicProperty79() { return getDynamicProperty(79); }
    @SuppressWarnings("unused") private Object getDynamicProperty80() { return getDynamicProperty(80); }
    @SuppressWarnings("unused") private Object getDynamicProperty81() { return getDynamicProperty(81); }
    @SuppressWarnings("unused") private Object getDynamicProperty82() { return getDynamicProperty(82); }
    @SuppressWarnings("unused") private Object getDynamicProperty83() { return getDynamicProperty(83); }
    @SuppressWarnings("unused") private Object getDynamicProperty84() { return getDynamicProperty(84); }
    @SuppressWarnings("unused") private Object getDynamicProperty85() { return getDynamicProperty(85); }
    @SuppressWarnings("unused") private Object getDynamicProperty86() { return getDynamicProperty(86); }
    @SuppressWarnings("unused") private Object getDynamicProperty87() { return getDynamicProperty(87); }
    @SuppressWarnings("unused") private Object getDynamicProperty88() { return getDynamicProperty(88); }
    @SuppressWarnings("unused") private Object getDynamicProperty89() { return getDynamicProperty(89); }
    @SuppressWarnings("unused") private Object getDynamicProperty90() { return getDynamicProperty(90); }
    @SuppressWarnings("unused") private Object getDynamicProperty91() { return getDynamicProperty(91); }
    @SuppressWarnings("unused") private Object getDynamicProperty92() { return getDynamicProperty(92); }
    @SuppressWarnings("unused") private Object getDynamicProperty93() { return getDynamicProperty(93); }
    @SuppressWarnings("unused") private Object getDynamicProperty94() { return getDynamicProperty(94); }
    @SuppressWarnings("unused") private Object getDynamicProperty95() { return getDynamicProperty(95); }
    @SuppressWarnings("unused") private Object getDynamicProperty96() { return getDynamicProperty(96); }
    @SuppressWarnings("unused") private Object getDynamicProperty97() { return getDynamicProperty(97); }
    @SuppressWarnings("unused") private Object getDynamicProperty98() { return getDynamicProperty(98); }
    @SuppressWarnings("unused") private Object getDynamicProperty99() { return getDynamicProperty(99); }
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

            writer.writeHtml(getLabel());
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
