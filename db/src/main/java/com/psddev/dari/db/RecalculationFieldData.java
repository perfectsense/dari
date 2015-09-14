package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Set;

import com.psddev.dari.db.Recordable.FieldInternalNamePrefix;

@FieldInternalNamePrefix("dari.recalculation.")
public class RecalculationFieldData extends Modification<ObjectMethod> {

    private String delayClassName;

    private boolean immediate;

    private String metricFieldName;

    private Set<String> groups;

    private transient RecalculationDelay delay;

    public void setDelayClass(Class<? extends RecalculationDelay> delayClass) {

        this.delayClassName = delayClass != null ? delayClass.getName() : null;
        this.delay = null;

    }

    @SuppressWarnings("unchecked")
    public RecalculationDelay getRecalculationDelay() {
        if (delay == null) {
            if (delayClassName == null) {
                return null;
            } else {
                try {
                    Class<RecalculationDelay> cls = (Class<RecalculationDelay>) Class.forName(delayClassName);
                    delay = cls.newInstance();
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (InstantiationException e) {
                    throw new RuntimeException(e);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return delay;
    }

    public String getDelayClassName() {
        return delayClassName;
    }

    public boolean isImmediate() {
        return immediate;
    }

    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    public String getMetricFieldName() {
        return metricFieldName;
    }

    public void setMetricFieldName(String metricFieldName) {
        this.metricFieldName = metricFieldName;
    }

    public Set<String> getGroups() {
        if (groups == null) {
            groups = new HashSet<String>();
        }
        return groups;
    }

    public void setGroups(Set<String> groups) {
        this.groups = groups;
    }

}
