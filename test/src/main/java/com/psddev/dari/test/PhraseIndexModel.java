package com.psddev.dari.test;

import com.psddev.dari.db.Database;
import com.psddev.dari.db.Record;

class PhraseIndexModel extends Record {

    public static PhraseIndexModel getInstance(Database db) {
        PhraseIndexModel tr = new PhraseIndexModel();
        tr.getState().setDatabase(db);
        return tr;
    }

    @Indexed String testString;
}

