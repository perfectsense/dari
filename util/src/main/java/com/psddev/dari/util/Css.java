package com.psddev.dari.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @deprecated No replacement.
 */
@Deprecated
public class Css {

    private final char[] css;
    private final int cssLength;
    private int cssIndex;

    private final Map<String, List<CssDeclaration>> rulesMap = new CompactMap<String, List<CssDeclaration>>();
    private final List<CssRule> rules;

    public Css(String css) throws IOException {
        this.css = css.toCharArray();
        this.cssLength = css.length();

        while (readRule(null)) {
            // Until all the rules are read.
        }

        List<CssRule> rules = new ArrayList<CssRule>();

        for (Map.Entry<String, List<CssDeclaration>> entry : rulesMap.entrySet()) {
            String selector = entry.getKey();
            int atRulesCount = 0;

            for (int lastBraceAt = 0, braceAt;
                    (braceAt = selector.indexOf('{', lastBraceAt)) > -1;
                    lastBraceAt = braceAt + 1) {
                ++ atRulesCount;
            }

            rules.add(new CssRule(selector, atRulesCount, entry.getValue()));
        }

        this.rules = Collections.unmodifiableList(rules);
    }

    public List<CssRule> getRules() {
        return rules;
    }

    public String getValue(String selector, String property) {
        List<CssDeclaration> declarations = rulesMap.get(selector);

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

    private void readComments() throws IOException {
        for (; cssIndex < cssLength; ++ cssIndex) {
            if (!Character.isWhitespace(css[cssIndex])) {
                break;
            }
        }

        boolean started = false;
        boolean inSingle = false;
        boolean inMulti = false;
        boolean multiEnding = false;

        for (; cssIndex < cssLength; ++ cssIndex) {
            char letter = css[cssIndex];

            if (letter == '/') {
                if (multiEnding) {
                    ++ cssIndex;
                    readComments();
                    break;

                } else if (started) {
                    inSingle = true;

                } else {
                    started = true;
                    multiEnding = false;
                }

            } else if (started && letter == '*') {
                if (inMulti) {
                    multiEnding = true;

                } else {
                    inMulti = true;
                }

            } else if (inSingle && (letter == '\r' || letter == '\n')) {
                ++ cssIndex;
                readComments();
                break;

            } else if (!(inSingle || inMulti)) {
                if (started) {
                    -- cssIndex;
                }
                break;
            }
        }
    }

    private boolean readRule(Set<String> parents) throws IOException {
        readComments();

        if (cssIndex < cssLength) {
            if (css[cssIndex] != '@') {
                return readSelector(parents);
            }

            StringBuilder atRule = new StringBuilder();
            atRule.append('@');

            for (++ cssIndex; cssIndex < cssLength; ++ cssIndex) {
                char letter = css[cssIndex];

                if (letter == '{') {
                    String atRuleString = atRule.toString().trim() + " {";
                    Set<String> atRuleParents = new LinkedHashSet<String>();

                    if (parents == null) {
                        atRuleParents.add(atRuleString);

                    } else {
                        for (String parent : parents) {
                            atRuleParents.add(atRuleString + " " + parent);
                        }
                    }

                    ++ cssIndex;
                    readDeclarations(atRuleParents);
                    return true;

                } else if (letter == ';') {
                    ++ cssIndex;
                    return true;

                } else {
                    atRule.append(letter);
                }
            }
        }

        return false;
    }

    private boolean readSelector(Set<String> parents) throws IOException {
        Set<String> selectors = null;
        StringBuilder newSelector = new StringBuilder();

        while (cssIndex < cssLength) {
            char letter = css[cssIndex];
            boolean brace = letter == '{';

            if (brace || letter == ',') {
                if (selectors == null) {
                    selectors = new LinkedHashSet<String>();
                }

                String newSelectorString = newSelector.toString().trim();

                if (parents == null) {
                    selectors.add(newSelectorString);

                } else {
                    for (String parent : parents) {
                        selectors.add(parent + " " + newSelectorString);
                    }
                }

                newSelector.setLength(0);

                if (brace) {
                    ++ cssIndex;
                    break;

                } else {
                    ++ cssIndex;
                    readComments();
                }

            } else {
                newSelector.append(letter);
                ++ cssIndex;
            }
        }

        if (selectors == null) {
            return false;
        }

        List<CssDeclaration> declarations = readDeclarations(selectors);

        for (String selector : selectors) {
            List<CssDeclaration> selectorDeclarations = rulesMap.get(selector);

            if (selectorDeclarations == null) {
                rulesMap.put(selector, new ArrayList<CssDeclaration>(declarations));

            } else {
                selectorDeclarations.addAll(declarations);
            }
        }

        return true;
    }

    private List<CssDeclaration> readDeclarations(Set<String> selectors) throws IOException {
        readComments();

        boolean readingProperty = true;
        List<CssDeclaration> declarations = new ArrayList<CssDeclaration>();
        StringBuilder property = new StringBuilder();
        StringBuilder value = new StringBuilder();
        StringBuilder current = property;
        int lastDeclaration = cssIndex;

        while (cssIndex < cssLength) {
            char letter = css[cssIndex];

            if (letter == '{') {
                cssIndex = lastDeclaration;
                property.setLength(0);
                readRule(selectors);
                lastDeclaration = cssIndex;

            } else if (readingProperty && letter == ':') {
                readingProperty = false;
                current = value;
                ++ cssIndex;
                readComments();

            } else if (letter == ';') {
                readingProperty = true;
                declarations.add(new CssDeclaration(property.toString().trim(), value.toString().trim()));
                property.setLength(0);
                value.setLength(0);
                current = property;
                ++ cssIndex;
                lastDeclaration = cssIndex;
                readComments();

            } else if (letter == '}') {
                String p = property.toString().trim();
                if (p.length() > 0) {
                    declarations.add(new CssDeclaration(p, value.toString().trim()));
                }
                ++ cssIndex;
                break;

            } else {
                current.append(letter);
                ++ cssIndex;
            }
        }

        return declarations;
    }
}
