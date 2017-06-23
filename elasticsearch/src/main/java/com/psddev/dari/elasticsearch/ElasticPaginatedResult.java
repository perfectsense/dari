
package com.psddev.dari.elasticsearch;

import com.psddev.dari.db.Query;
import com.psddev.dari.db.Record;
import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Paginated result for Elastic that provides access to
 * faceted results.
 *
 */
public class ElasticPaginatedResult<E> extends PaginatedResult<E> implements HtmlObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticPaginatedResult.class);

    final Class<?> klass;
    final List<Terms> facetedFields;
    final List<String> facetedFieldsNames;
    final List<Range> rangeFacets;
    final List<String> rangeFacetsNames;
    final Filter queryFacet;

    /**
     * The paginated result with Field Facet
     */
    public ElasticPaginatedResult(
            long offset, int limit, long count, List<E> items, List<Terms> facetedFields,
            List<String> facetedFieldsNames,
            Class<?> klass) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.facetedFieldsNames = facetedFieldsNames;
        this.rangeFacets = null;
        this.rangeFacetsNames = null;
        this.queryFacet = null;
    }

    /**
     * The paginated result with Field Facet and debugging
     */
    public ElasticPaginatedResult(
            long offset, int limit, long count, List<E> items, List<Terms> facetedFields,
            List<String> facetedFieldsNames,
            Class<?> klass, SearchRequestBuilder srb) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.facetedFieldsNames = facetedFieldsNames;
        this.rangeFacets = null;
        this.rangeFacetsNames = null;
        this.srb = srb;
        this.queryFacet = null;
    }

    /**
     * The paginated result with Field, Range Facets and debugging
     */
    public ElasticPaginatedResult(
            long offset, int limit, long count, List<E> items, List<Terms> facetedFields,
            List<String> facetedFieldsNames, List<Range> rangeFacets, List<String> rangeFacetsNames,
            Class<?> klass, SearchRequestBuilder srb) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.facetedFieldsNames = facetedFieldsNames;
        this.rangeFacets = rangeFacets;
        this.rangeFacetsNames = rangeFacetsNames;
        this.srb = srb;
        this.queryFacet = null;
    }

    /**
     * The paginated result with all facets and debugging
     */
    public ElasticPaginatedResult(
            long offset, int limit, long count, List<E> items, List<Terms> facetedFields,
            List<String> facetedFieldsNames, List<Range> rangeFacets, List<String> rangeFacetsNames,
            Filter queryFacet,
            Class<?> klass, SearchRequestBuilder srb) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.facetedFieldsNames = facetedFieldsNames;
        this.rangeFacets = rangeFacets;
        this.rangeFacetsNames = rangeFacetsNames;
        this.queryFacet = queryFacet;
        this.srb = srb;
    }

    /**
     * Field Terms Facet
     */
    public List<DariFacetField> getFacetedFields() {
        List<DariFacetField> fields = new ArrayList<>();
        if (this.facetedFields != null) {
            for (int i = 0; i < this.facetedFields.size(); i++) {
                Terms field = this.facetedFields.get(i);
                String fieldName = this.facetedFieldsNames.get(i);
                fields.add(new DariFacetField(this.klass, field, fieldName));
            }
        }

        return fields;
    }

    /**
     * Range Facet
     */
    public List<DariRangeFacet> getRangeFacets() {
        List<DariRangeFacet> ranges = new ArrayList<>();
        if (this.rangeFacets != null) {
            for (int i = 0; i < this.rangeFacets.size(); i++) {
                Range rangeFacet = this.rangeFacets.get(i);
                String fieldName = this.rangeFacetsNames.get(i);
                ranges.add(new DariRangeFacet(this.klass, rangeFacet, fieldName));
            }
        }

        return ranges;
    }

    /**
     * The Query Facet
     */
    public Filter getQueryFacet() {
        return queryFacet;
    }

    public Long getQueryFacetCount() {
        if (this.queryFacet != null) {
            return this.queryFacet.getDocCount();
        }

        return null;
    }

    private transient SearchRequestBuilder srb;

    public SearchRequestBuilder getElasticQuery() {
        return srb;
    }

    public void setElasticQuery(SearchRequestBuilder srb) {
        this.srb = srb;
    }

    @Override
    public void format(HtmlWriter writer) throws IOException {
        writer.writeStart("p");
            writer.writeStart("code").writeHtml(this.getClass().getName()).writeEnd();
            writer.writeHtml(' ');
            writer.writeStart("strong").writeHtml(this.getFirstItemIndex()).writeEnd();
            writer.writeHtml(" to ");
            writer.writeStart("strong").writeHtml(this.getLastItemIndex()).writeEnd();
            writer.writeHtml(" of ");
            writer.writeStart("strong").writeHtml(this.getCount()).writeEnd();
        writer.writeEnd();

        if (Settings.isDebug() && this.getElasticQuery() != null) {
            String solrFullQuery = this.getElasticQuery().toString();

            writer.writeHtml("Elastic Query: ");
            writer.writeHtml(StringUtils.decodeUri(solrFullQuery));
        }

        writer.writeStart("ol");
            for (Object item : this.getItems()) {
                writer.writeStart("li").writeObject(item).writeHtml(" Elastic Score: " + ElasticsearchDatabase.getScore(item)).writeEnd();
            }
        writer.writeEnd();
    }

    /**
     * The class for Facet Field
     */
    public static class DariFacetField {

        private final Class<?> klass;
        private final Terms field;
        private final String fieldName;
        private Long numTerms;

        public DariFacetField(Class<?> klass, Terms field, String fieldName) {
            this.klass = klass;
            this.field = field;
            this.fieldName = fieldName;
            this.numTerms = 0L;
            if (field != null) {
                if (field.getBuckets() != null) {
                    this.numTerms = (long) field.getBuckets().size();
                }
            }
        }

        /**
         * The fieldName
         */
        public String getName() {
            return this.fieldName;
        }

        /**
         * The number of Terms
         */
        public Long getCount() {
            return this.numTerms;
        }

        /**
         * Each term has a count, return this Map
         */
        public Map<String, Long> getTermValue() {
            Map<String, Long> index = new HashMap<>();

            for (Terms.Bucket entry : this.field.getBuckets()) {
                String key = entry.getKeyAsString();    // Term
                long docCount = entry.getDocCount();    // Doc count
                LOGGER.debug("key [{}], doc_count [{}]", key, docCount);
                index.put(key, docCount);
            }
            return index;
        }

        /**
         * For faceting on UUID (get unique Id) - add count to State Extras
         */
        public <T> List<T> getObjects() {
            Map<String, Long> index = new HashMap<>();
            List<String> ids = new ArrayList<>();

            for (Terms.Bucket entry : this.field.getBuckets()) {
                String key = entry.getKeyAsString();    // Term
                long docCount = entry.getDocCount();    // Doc count
                LOGGER.debug("key [{}], doc_count [{}]", key, docCount);
                index.put(key, docCount);
                if (ElasticsearchDatabase.isUUID(key)) {
                    ids.add(key);
                }
            }

            @SuppressWarnings("unchecked")
            List<T> objects = (List<T>) (this.klass == null || this.klass == Query.class
                    ? Query.fromAll().where("id = ?", ids).selectAll()
                    : Query.from(this.klass).where("id = ?", ids).selectAll());

            if (objects != null) {
                for (Object o : objects) {
                    Record record = (Record) o;
                    Long c = index.get(record.getId().toString());
                    record.getState().getExtras().put("count", c);
                }
            }

            return objects;
        }
    }

    /**
     * The class for Facet range per Numeric field.
     */
    public static class DariRangeFacet {

        private final Class<?> klass;
        private final Range rangeFacet;
        private final String fieldName;

        public DariRangeFacet(Class<?> klass, Range rangeFacet, String fieldName) {
            this.klass = klass;
            this.rangeFacet = rangeFacet;
            this.fieldName = fieldName;
        }

        public String getName() {
            return fieldName;
        }

        public Range getRangeFacet() {
            return rangeFacet;
        }

        /**
         * The range and counts for each
         */
        public Map<String, Long> getRangeValues() {
            Map<String, Long> index = new HashMap<>();

            for (Range.Bucket entry : rangeFacet.getBuckets()) {
                String key = entry.getKeyAsString();             // Range as key
                Number from = (Number) entry.getFrom();          // Bucket from
                Number to = (Number) entry.getTo();              // Bucket to
                long docCount = entry.getDocCount();             // Doc count
                index.put(key, docCount);
            }
            return index;
        }
    }
}
