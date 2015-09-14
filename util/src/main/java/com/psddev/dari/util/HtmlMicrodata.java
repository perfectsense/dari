package com.psddev.dari.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.MoreObjects;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

/**
 * @see <a href="http://en.wikipedia.org/wiki/Microdata_(HTML)">Wikipedia</a>
 * @see <a href="http://www.whatwg.org/specs/web-apps/current-work/multipage/microdata.html">WHATWG HTML specification</a>
 */
public class HtmlMicrodata {

    private Set<String> types;
    private String id;
    private Map<String, List<Object>> properties;

    public HtmlMicrodata() {
    }

    public HtmlMicrodata(URL url, Element item) {
        URI uri;

        try {
            uri = url.toURI();
        } catch (URISyntaxException error) {
            uri = null;
        }

        Splitter whitespaceSplitter = Splitter.on(CharMatcher.WHITESPACE).omitEmptyStrings().trimResults();
        Map<String, List<Object>> properties = getProperties();
        String types = item.attr("itemtype");

        if (!ObjectUtils.isBlank(types)) {
            for (String type : whitespaceSplitter.split(types)) {
                getTypes().add(type);
            }
        }

        setId(item.attr("itemid"));

        for (Element prop : item.select("[itemprop]")) {
            if (item.equals(prop)) {
                continue;

            } else if (!item.equals(closestItemScope(prop))) {
                continue;
            }

            String names = prop.attr("itemprop");
            String tagName = " " + prop.tagName() + " ";
            Object value;

            if (prop.hasAttr("itemscope")) {
                value = new HtmlMicrodata(url, prop);

            } else if (" meta ".contains(tagName)) {
                value = prop.attr("content");

            } else if (" audio embed iframe img source track video ".contains(tagName)) {
                try {
                    value = uri.resolve(prop.attr("src")).toString();
                } catch (IllegalArgumentException error) {
                    value = null;
                } catch (NullPointerException error) {
                    value = null;
                }

            } else if (" a area link ".contains(tagName)) {
                try {
                    value = uri.resolve(prop.attr("href")).toString();
                } catch (IllegalArgumentException error) {
                    value = null;
                } catch (NullPointerException error) {
                    value = null;
                }

            } else if (" object ".contains(tagName)) {
                value = prop.attr("data");

            } else if (" data meter ".contains(tagName)) {
                value = prop.attr("value");

            } else if (" time ".contains(tagName)) {
                if (prop.hasAttr("datetime")) {
                    value = prop.attr("datetime");
                } else if (prop.hasAttr("content")) {
                    value = prop.attr("content");
                } else {
                    value = prop.text();
                }
                // this older version was returning empty string... prop.attr returning non-null?
                // value = ObjectUtils.firstNonNull(prop.attr("datetime"), prop.attr("content"), prop.text());

            } else {
                if (prop.hasAttr("content")) {
                    value = prop.attr("content");
                } else {
                    value = prop.text();
                }
            }

            if (!ObjectUtils.isBlank(names)) {
                for (String name : whitespaceSplitter.split(names)) {
                    List<Object> values = properties.get(name);

                    if (values == null) {
                        values = new ArrayList<Object>();
                        properties.put(name, values);
                    }

                    values.add(value);
                }
            }
        }
    }

    protected static Element closestItemScope(Element element) {
        for (Element p : element.parents()) {
            if (p.hasAttr("itemscope")) {
                return p;
            }
        }

        return null;
    }

    /**
     * @return Never {@code null}. Mutable.
     */
    public Set<String> getTypes() {
        if (types == null) {
            types = new LinkedHashSet<String>();
        }
        return types;
    }

    /**
     * @param types May be {@code null} to clear the set.
     */
    public void setTypes(Set<String> types) {
        this.types = types;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * @return Never {@code null}. Mutable.
     */
    public Map<String, List<Object>> getProperties() {
        if (properties == null) {
            properties = new CompactMap<String, List<Object>>();
        }
        return properties;
    }

    /**
     * @param properties May be {@code null} to clear the map.
     */
    public void setProperties(Map<String, List<Object>> properties) {
        this.properties = properties;
    }

    /**
     * Returns the first {@code itemtype}.
     * @return May be {@code null}.
     */
    public String getFirstType() {
        return types != null && !types.isEmpty() ? types.iterator().next() : null;
    }

    /**
     * Returns the first {@code itemprop} associated with the given
     * {@code name}.
     *
     * @return May be {@code null}.
     */
    public Object getFirstProperty(String name) {
        if (properties != null) {
            List<Object> values = properties.get(name);

            if (values != null && !values.isEmpty()) {
                return values.get(0);
            }
        }

        return null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("types", getTypes())
                .add("id", getId())
                .add("properties", getProperties())
                .toString();
    }

    /**
     * {@link HtmlMicrodata} utility methods.
     */
    public static final class Static {

        /**
         * Returns all microdata items in the given {@code document},
         * resolving all relative URLs against the given {@code url}.
         *
         * @param url If {@code null}, relative URLs won't be resolved.
         * @param document If {@code null}, returns an empty list.
         * @return Never {@code null}.
         */
        public static List<HtmlMicrodata> parseDocument(URL url, Document document) {
            List<HtmlMicrodata> datas = new ArrayList<HtmlMicrodata>();

            if (document != null) {
                for (Element item : document.select("[itemscope]")) {
                    if (closestItemScope(item) == null
                            || !item.hasAttr("itemprop")) {
                        datas.add(new HtmlMicrodata(url, item));
                    }
                }
            }

            return datas;
        }

        /**
         * Returns all microdata items in the given {@code html}, resolving
         * all relative URLs against the given {@code url}.
         *
         * @param url If {@code null}, relative URLs won't be resolved.
         * @param html If {@code null}, returns an empty list.
         * @return Never {@code null}.
         */
        public static List<HtmlMicrodata> parseString(URL url, String html) {
            if (ObjectUtils.isBlank(html)) {
                return new ArrayList<HtmlMicrodata>();

            } else {
                return parseDocument(url, Jsoup.parse(html));
            }
        }

        /**
         * Returns all microdata items in the HTML output from the given
         * {@code url}.
         *
         * @param url Can't be {@code null}.
         */
        public static List<HtmlMicrodata> parseUrl(URL url) throws IOException {
            return parseString(url, IoUtils.toString(url));
        }

        /**
         * Returns the first HtmlMicrodata instance in the list that matches one of the supplied schema types, or null if none match
         * @param htmlMicrodatas
         * @param allowedSchemaTypes
         * @return
         */
        public static HtmlMicrodata getFirstType(List<HtmlMicrodata> htmlMicrodatas, Collection<String> allowedSchemaTypes) {
            for (HtmlMicrodata htmlMicrodata : htmlMicrodatas) {
                for (String allowedSchemaType : allowedSchemaTypes) {
                    if (htmlMicrodata.getTypes().contains(allowedSchemaType)) {
                        return htmlMicrodata;
                    }
                }
            }
            return null;
        }
    }
}
