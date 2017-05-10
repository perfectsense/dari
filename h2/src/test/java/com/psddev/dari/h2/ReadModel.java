package com.psddev.dari.h2;

import com.psddev.dari.db.Record;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReadModel extends Record {

    @Indexed(unique = true)
    @Required
    public String text;

    @Indexed
    public String firstLetter;

    @Indexed
    public Set<String> letters;

    @Override
    protected void beforeSave() {
        if (text != null) {
            int textLength = text.length();

            if (textLength > 0) {
                firstLetter = text.substring(0, 1);
                letters = IntStream.range(0, textLength)
                        .mapToObj(i -> text.substring(i, i + 1))
                        .collect(Collectors.toSet());
            }
        }
    }
}
