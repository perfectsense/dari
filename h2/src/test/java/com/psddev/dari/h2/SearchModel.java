package com.psddev.dari.h2;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SearchModel extends Record {

    @Indexed
    public String one;

    @Indexed
    public final Set<String> set = new HashSet<>();

    @Indexed
    public final List<String> list = new ArrayList<>();

    @Indexed
    public final Map<String, String> map = new HashMap<>();
}
