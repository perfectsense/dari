package com.psddev.dari.elasticsearch;

import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.ArrayList;
import java.util.List;

class ElasticFacet {
    Filter filter = null;
    final List<Terms> terms = new ArrayList<>();
    final List<String> fieldNames = new ArrayList<>();
    final List<String> aggsTermsNames = new ArrayList<>();
    final List<Range> ranges = new ArrayList<>();
    final List<String> rangeNames = new ArrayList<>();
    final List<String> aggsRangeNames = new ArrayList<>();
}
