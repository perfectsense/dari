package com.psddev.dari.util;

import java.util.Collections;
import java.util.List;

/**
 * @deprecated No replacement.
 */
@Deprecated
public class CssRule {

    private final String selector;
    private final int atRulesCount;
    private final List<CssDeclaration> declarations;

    public CssRule(String selector, int atRulesCount, List<CssDeclaration> declarations) {
        this.selector = selector;
        this.atRulesCount = atRulesCount;
        this.declarations = Collections.unmodifiableList(declarations);
    }

    public String getSelector() {
        return selector;
    }

    public int getAtRulesCount() {
        return atRulesCount;
    }

    public List<CssDeclaration> getDeclarations() {
        return declarations;
    }

    public String getValue(String property) {
        List<CssDeclaration> declarations = getDeclarations();

        if (declarations != null) {
            for (int i = declarations.size() - 1; i >= 0; -- i) {
                CssDeclaration declaration = declarations.get(i);

                if (declaration.getProperty().equals(property)) {
                    return declaration.getValue();
                }
            }
        }

        return null;
    }

    // --- Object support ---

    @Override
    public String toString() {
        StringBuilder css = new StringBuilder();

        css.append(getSelector());
        css.append(" {\n");

        for (CssDeclaration decl : getDeclarations()) {
            css.append("  ");
            css.append(decl.toString());
            css.append('\n');
        }

        css.append('}');

        for (int i = 0, count = getAtRulesCount(); i < count; ++ i) {
            css.append(" }");
        }

        css.append('\n');

        return css.toString();
    }
}
