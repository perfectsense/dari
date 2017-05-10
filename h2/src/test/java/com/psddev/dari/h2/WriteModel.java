package com.psddev.dari.h2;

import com.psddev.dari.db.Record;

import java.util.ArrayList;
import java.util.List;

public class WriteModel extends Record {

    public int number;
    public String string;
    public final List<String> list = new ArrayList<String>();
}
