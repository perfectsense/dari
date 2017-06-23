package com.psddev.dari.elasticsearch;

/**
 * Thrown when a field does not exist when indexing a type ahead field
 */
@SuppressWarnings("serial")
public class NoTypeAheadFieldException extends RuntimeException {

    private final String group;
    private final String key;

    public NoTypeAheadFieldException(String group, String key) {
        super(String.format("Can't index [%s] type ahead due to missing dependent field [%s]!", group, key));
        this.group = group;
        this.key = key;
    }

    public String getGroup() {
        return group;
    }

    public String getKey() {
        return key;
    }
}
