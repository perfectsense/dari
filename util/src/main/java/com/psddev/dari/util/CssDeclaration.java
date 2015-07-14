package com.psddev.dari.util;

/**
 * @deprecated No replacement.
 */
@Deprecated
public class CssDeclaration {

    private final String property;
    private final String value;

    public CssDeclaration(String property, String value) {
        this.property = property;
        this.value = value;
    }

    public String getProperty() {
        return property;
    }

    public String getValue() {
        return value;
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder css = new StringBuilder();

        css.append(property);
        css.append(':');

        css.append(value);
        css.append(';');

        return css.toString();
    }
}
