package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Record;
import com.psddev.dari.elasticsearch.ElasticsearchDatabase.TypeAheadFields;

@TypeAheadFields(mappings={
        @ElasticsearchDatabase.TypeAheadFieldsMapping(field = "fromTypeAhead", fields = {"typeAhead", "typeAhead2"})
}, value={"name", "desc"})
public class ElasticTypeModel extends Record {
    @Indexed
    String name;

    @Indexed
    @ElasticsearchDatabase.ExcludeFromAny
    String desc;

    @Indexed
    String fromTypeAhead;

    // placeholders are missing for tests
}
