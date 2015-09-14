package com.psddev.dari.util;

// CHECKSTYLE:OFF
/**
 * @deprecated No replacement.
 */
@Deprecated
public class CssUnit {

    private final double number;
    private final String unit;

    public CssUnit(double number, String unit) {
        this.number = number;
        this.unit = unit;
    }

    public double getNumber() {
        return number;
    }

    public String getUnit() {
        return unit;
    }

    public CssUnit(String value) {
        char[] letters = value.toCharArray();
        int index = 0;

        for (int length = letters.length; index < length; ++ index) {
            char letter = letters[index];

            if (!(letter == '.' || Character.isDigit(letter))) {
                break;
            }
        }

        this.number = ObjectUtils.to(double.class, value.substring(0, index));
        this.unit = value.substring(index);
    }

    // --- Object support ---

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(getNumber(), getUnit());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;

        } else if (other instanceof CssUnit) {
            CssUnit otherUnit = (CssUnit) other;
            return getNumber() == otherUnit.getNumber() &&
                    ObjectUtils.equals(getUnit(), otherUnit.getUnit());

        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "auto".equals(unit) ? unit : number + unit;
    }
}
