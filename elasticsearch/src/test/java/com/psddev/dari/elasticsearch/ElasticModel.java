package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Record;
import com.psddev.dari.db.Recordable;
import com.psddev.dari.elasticsearch.ElasticsearchDatabase.TypeAheadFields;

import java.util.List;

@TypeAheadFields(mappings={
        @ElasticsearchDatabase.TypeAheadFieldsMapping(field = "fromTypeAhead", fields = {"typeAhead", "typeAhead2"})
}, value={"name", "desc"})
public class ElasticModel extends Record {
    @Recordable.Indexed
    String name;

    @Recordable.Indexed
    @ElasticsearchDatabase.ExcludeFromAny
    String desc;

    @Recordable.Indexed
    String fromTypeAhead;

    // placeholders for TypeAheadFields Might want to set Visibility too must be List<String>
    @Recordable.Indexed
    List<String> suggestField;

    // placeholders for TypeAheadFields Might want to set Visibility too
    @Recordable.Indexed
    List<String> typeAhead;

    // placeholders for TypeAheadFields Might want to set Visibility too
    @Recordable.Indexed
    List<String> typeAhead2;
}
