
package com.psddev.dari.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.FacetField;

import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;
import com.psddev.dari.util.PaginatedResult;
import com.psddev.dari.util.Settings;
import com.psddev.dari.util.StringUtils;
import org.apache.solr.client.solrj.response.RangeFacet;

/**
 * Paginated result for Solr that provides access to
 * faceted results.
 *
 * @author jcollins
 */
public class SolrPaginatedResult<E> extends PaginatedResult<E> implements HtmlObject {

    final Class<?> klass;
    final List<FacetField> facetedFields;
    final List<RangeFacet> rangeFacets;

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields,
            Class<?> klass) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.rangeFacets = null;
    }

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields,
            Class<?> klass, SolrQuery solrQuery) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.rangeFacets = null;
        this.solrQuery = solrQuery;
    }

    public SolrPaginatedResult(
            long offset, int limit, long count, List<E> items, List<FacetField> facetedFields, List<RangeFacet> rangeFacets,
            Class<?> klass, SolrQuery solrQuery) {
        super(offset, limit, count, items);

        this.klass = klass;
        this.facetedFields = facetedFields;
        this.rangeFacets = rangeFacets;
        this.solrQuery = solrQuery;
    }

    public List<DariFacetField> getFacetedFields() {
        List<DariFacetField> fields = new ArrayList<DariFacetField>();
        if (this.facetedFields != null) {
            for (FacetField field : this.facetedFields) {
                fields.add(new DariFacetField(this.klass, field));
            }
        }

        return fields;
    }

    public List<DariRangeFacet> getRangeFacets() {
        List<DariRangeFacet> ranges = new ArrayList<DariRangeFacet>();
        if (this.rangeFacets != null) {
            for (RangeFacet rangeFacet : this.rangeFacets) {
                ranges.add(new DariRangeFacet(this.klass, rangeFacet));
            }
        }

        return ranges;
    }

    private transient SolrQuery solrQuery;

    public SolrQuery getSolrQuery() {
        return solrQuery;
    }

    public void setSolrQuery(SolrQuery solrQuery) {
        this.solrQuery = solrQuery;
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

        if (Settings.isDebug() && this.getSolrQuery() != null) {
            String solrFullQuery = this.getSolrQuery().toString();

            writer.writeHtml("Solr Query: ");
            writer.writeHtml(StringUtils.decodeUri(solrFullQuery));
        }

        writer.writeStart("ol");
            for (Object item : this.getItems()) {
                writer.writeStart("li").writeObject(item).writeHtml(" Solr Score: " + SolrDatabase.Static.getScore(item)).writeEnd();
            }
        writer.writeEnd();
    }

    public static class DariFacetField {

        private final Class<?> klass;
        private final FacetField field;

        public DariFacetField(Class<?> klass, FacetField field) {
            this.klass = klass;
            this.field = field;
        }

        public String getName() {
            return this.field.getName();
        }

        public Long getCount() {
            return Long.valueOf(this.field.getValueCount());
        }

        public <T> List<T> getObjects() {
            Map<String, FacetField.Count> index = new HashMap<String, FacetField.Count>();

            List<String> ids = new ArrayList<String>();
            for (FacetField.Count c : this.field.getValues()) {
                index.put(c.getName(), c);
                ids.add(c.getName());
            }

            @SuppressWarnings("unchecked")
            List<T> objects = (List<T>) (this.klass == null || this.klass == Query.class
                    ? Query.fromAll().where("id = ?", ids).selectAll()
                    : Query.from(this.klass).where("id = ?", ids).selectAll());

            if (objects != null) {
                for (Object o : objects) {
                    Record record = (Record) o;
                    FacetField.Count c = index.get(record.getId().toString());
                    record.getState().getExtras().put("count", Long.valueOf(c.getCount()));
                }
            }

            return objects;
        }
    }

    public static class DariRangeFacet {

        private final Class<?> klass;
        private final RangeFacet rangeFacet;

        public DariRangeFacet(Class<?> klass, RangeFacet rangeFacet) {
            this.klass = klass;
            this.rangeFacet = rangeFacet;
        }

        public String getName() {
            return rangeFacet.getName();
        }

        public RangeFacet getRangeFacet() {
            return rangeFacet;
        }
    }
}
