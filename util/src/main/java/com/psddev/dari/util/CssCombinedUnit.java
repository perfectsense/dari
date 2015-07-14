package com.psddev.dari.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// CHECKSTYLE:OFF
/**
 * @deprecated No replacement.
 */
@Deprecated
public class CssCombinedUnit {

    private final Map<String, CssUnit> combined = new HashMap<String, CssUnit>();

    public CssCombinedUnit(Iterable<CssUnit> values) {
        for (CssUnit value : values) {
            String unit = value.getUnit();
            CssUnit old = combined.get(unit);

            if (old == null) {
                combined.put(unit, value);

            } else {
                combined.put(unit, new CssUnit(old.getNumber() + value.getNumber(), unit));
            }
        }

        for (Iterator<Map.Entry<String, CssUnit>> i = combined.entrySet().iterator(); i.hasNext();) {
            CssUnit value = i.next().getValue();

            if (!"auto".equals(value.getUnit()) && value.getNumber() == 0.0) {
                i.remove();
            }
        }

        if (combined.isEmpty()) {
            combined.put("px", new CssUnit(0, "px"));
        }
    }

    public CssCombinedUnit(CssUnit... values) {
        this(values != null ?
                Arrays.asList(values) :
                Collections.<CssUnit>emptyList());
    }

    public Collection<CssUnit> getAll() {
        return combined.values();
    }

    /**
     * Returns a single CSS unit that can represent this combined unit if
     * possible.
     *
     * @return May be {@code null} if the combined unit can't be represented
     * as a single CSS unit.
     */
    public CssUnit getSingle() {
        if (combined.size() != 1) {
            return null;

        } else {
            CssUnit value = combined.values().iterator().next();
            return "fr".equals(value.getUnit()) ? null : value;
        }
    }

    public boolean hasAuto() {
        return combined.keySet().contains("auto");
    }
}
