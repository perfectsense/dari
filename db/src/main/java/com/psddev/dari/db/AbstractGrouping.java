package com.psddev.dari.db;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.HtmlObject;
import com.psddev.dari.util.HtmlWriter;

public abstract class AbstractGrouping<T> implements Grouping<T>, HtmlObject {

    private final List<Object> keys;
    protected final Query<T> query;
    protected final String[] fields;
    private final Map<String, Aggregate> aggregates = new CompactMap<String, Aggregate>();

    protected AbstractGrouping(List<Object> keys, Query<T> query, String[] fields) {
        this.keys = keys;
        this.query = query;
        this.fields = fields;
    }

    @Override
    public List<Object> getKeys() {
        return keys;
    }

    @Override
    public Query<T> createItemsQuery() {
        Query<T> itemsQuery = Query.fromQuery(query);
        for (int i = 0, length = fields.length; i < length; ++ i) {
            itemsQuery.and(fields[i] + " = ?", keys.get(i));
        }
        return itemsQuery;
    }

    protected abstract Aggregate createAggregate(String field);

    private Aggregate getAggregate(String field) {
        Aggregate aggregate = aggregates.get(field);
        if (aggregate == null) {
            aggregate = createAggregate(field);
            aggregates.put(field, aggregate);
        }
        return aggregate;
    }

    @Override
    public Object getMaximum(String field) {
        return getAggregate(field).getMaximum();
    }

    @Override
    public Object getMinimum(String field) {
        return getAggregate(field).getMinimum();
    }

    @Override
    public long getNonNullCount(String field) {
        return getAggregate(field).getNonNullCount();
    }

    @Override
    public double getSum(String field) {
        return getAggregate(field).getSum();
    }

    // --- HtmlObject support ---

    @Override
    public void format(HtmlWriter writer) throws IOException {
        writer.writeHtml(getCount());
        writer.writeHtml(' ');

        Iterator<Object> keysIterator = getKeys().iterator();
        if (keysIterator.hasNext()) {
            writer.writeHtml("[ ");
            writer.writeObject(keysIterator.next());
            while (keysIterator.hasNext()) {
                writer.writeHtml(", ");
                writer.writeObject(keysIterator.next());
            }
            writer.writeHtml(" ]");
        }
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {

        if (this == other) {
            return true;

        } else if (other instanceof Grouping) {
            Grouping<?> otherGrouping = (Grouping<?>) other;
            return keys.equals(otherGrouping.getKeys());

        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder html = new StringBuilder();
        html.append("{keys=").append(keys);
        html.append(", count=").append(getCount());
        html.append(", aggregates=").append(aggregates);
        html.append('}');
        return html.toString();
    }

    protected static class Aggregate {

        private Object maximum;
        private Object minimum;
        private long nonNullCount;
        private double sum;

        public Object getMaximum() {
            return maximum;
        }

        public void setMaximum(Object maximum) {
            this.maximum = maximum;
        }

        public Object getMinimum() {
            return minimum;
        }

        public void setMinimum(Object minimum) {
            this.minimum = minimum;
        }

        public long getNonNullCount() {
            return nonNullCount;
        }

        public void setNonNullCount(long nonNullCount) {
            this.nonNullCount = nonNullCount;
        }

        public double getSum() {
            return sum;
        }

        public void setSum(double sum) {
            this.sum = sum;
        }

        // --- Object support ---

        @Override
        public String toString() {
            StringBuilder html = new StringBuilder();
            html.append("{maximum=").append(getMaximum());
            html.append(", minimum=").append(getMinimum());
            html.append(", nonNullCount=").append(getNonNullCount());
            html.append(", sum=").append(getSum());
            html.append('}');
            return html.toString();
        }
    }
}
