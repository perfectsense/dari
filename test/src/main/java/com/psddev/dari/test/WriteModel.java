package com.psddev.dari.test;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class WriteModel extends Record {

    public int number;
    public String string;
    public final List<String> list = new ArrayList<>();

    public Map<String, UUID> currentItems;

}
