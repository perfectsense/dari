package com.psddev.dari.db;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.parser.Tag;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.StringUtils;

/** Contains strings and references to other objects. */
public class ReferentialText extends AbstractList<Object> {

    private static final Pattern ENHANCEMENT_PATTERN = Pattern.compile("(?is)<(\\S+)[^>]*class\\s*=[^>]*enhancement[^>]*>.*?</\\1>");
    private static final Pattern DUPLICATE_PROTOCOL_PATTERN = Pattern.compile("^(?:(?:https?:/?/?)*(https?://))?");

    private static final Tag BR_TAG = Tag.valueOf("br");
    private static final Tag DIV_TAG = Tag.valueOf("div");
    private static final Tag P_TAG = Tag.valueOf("p");

    private final List<Object> list = new ArrayList<Object>();
    private boolean resolveInvisible;

    /**
     * Creates an empty instance.
     */
    public ReferentialText() {
    }

    private static void addByBoundary(
            List<Object> list,
            String html,
            String boundary,
            List<Reference> references) {

        int previousBoundaryAt = 0;

        for (int boundaryAt = previousBoundaryAt;
                (boundaryAt = html.indexOf(boundary, previousBoundaryAt)) >= 0;
                previousBoundaryAt = boundaryAt + boundary.length()) {
            list.add(html.substring(previousBoundaryAt, boundaryAt));
            list.add(references.get(list.size() / 2));
        }

        list.add(html.substring(previousBoundaryAt));
    }

    /**
     * Parses the given {@code html} and adds all HTML strings and references
     * found within.
     *
     * @param html If {@code null}, does nothing.
     */
    public void addHtml(String html) {
        if (html == null) {
            return;
        }

        // Look for anything that looks like enhancement markup.
        Matcher enhancementMatcher = ENHANCEMENT_PATTERN.matcher(html);
        int lastEnhancementEnd = 0;

        while (enhancementMatcher.find()) {
            addString(html.substring(lastEnhancementEnd, enhancementMatcher.start()));

            // Parse the markup to verify that it really is an enhancement.
            lastEnhancementEnd = enhancementMatcher.end();
            String enhancement = enhancementMatcher.group(0);
            Document enhancementDocument = Jsoup.parseBodyFragment(enhancement);
            Element enhancementElement = enhancementDocument.getElementsByClass("enhancement").first();

            if (enhancementElement != null) {

                // Don't do anything if enhancement is flagged to be removed.
                if (enhancementElement.hasClass("state-removing")) {
                    continue;
                }

                Reference reference = null;
                String referenceData = enhancementElement.dataset().remove("reference");

                if (!StringUtils.isBlank(referenceData)) {
                    Map<?, ?> referenceMap = (Map<?, ?>) ObjectUtils.fromJson(referenceData);
                    UUID id = ObjectUtils.to(UUID.class, referenceMap.get("_id"));
                    UUID typeId = ObjectUtils.to(UUID.class, referenceMap.get("_type"));
                    ObjectType type = Database.Static.getDefault().getEnvironment().getTypeById(typeId);

                    if (type != null) {
                        Object referenceObject = type.createObject(id);

                        if (referenceObject instanceof Reference) {
                            reference = (Reference) referenceObject;
                        }
                    }

                    if (reference == null) {
                        reference = new Reference();
                    }

                    reference.getState().setResolveInvisible(isResolveInvisible());

                    for (Map.Entry<?, ?> entry : referenceMap.entrySet()) {
                        reference.getState().put(entry.getKey().toString(), entry.getValue());
                    }
                }

                if (reference != null) {
                    list.add(reference);
                    continue;
                }
            }

            // Looks like an enhancement but it really isn't.
            addString(enhancement);
        }

        // Trailing markup.
        addString(html.substring(lastEnhancementEnd));
    }

    private void addString(String string) {
        if (string != null && string.length() > 0) {
            list.add(string);
        }
    }

    /**
     * Creates an instance from the given {@code html}.
     *
     * @param html If {@code null}, creates an empty instance.
     */
    public ReferentialText(String html, boolean finalDraft) {
        addHtml(html);
    }

    public boolean isResolveInvisible() {
        return resolveInvisible;
    }

    public void setResolveInvisible(boolean resolveInvisible) {
        this.resolveInvisible = resolveInvisible;
    }

    /**
     * Returns a mixed list of well-formed HTML strings and object references
     * that have been converted to publishable forms.
     *
     * @return Never {@code null}.
     */
    public List<Object> toPublishables() {
        return toPublishables(false, null);
    }

    /**
     * Returns a mixed list of well-formed HTML strings and object references
     * that have been converted to publishable forms.
     *
     * @param cleaner May be {@code null}.
     * @return Never {@code null}.
     */
    public List<Object> toPublishables(Cleaner cleaner) {
        return toPublishables(false, cleaner);
    }

    /**
     * Returns a mixed list of well-formed HTML strings and object references
     * that have been converted to publishable forms.
     *
     * @param inline If {@code true}, doesn't try to add {@code <p>} elements.
     * @param cleaner May be {@code null}.
     * @return Never {@code null}.
     */
    public List<Object> toPublishables(boolean inline, Cleaner cleaner) {

        // Concatenate the items so that it can be fed into an HTML parser.
        StringBuilder html = new StringBuilder();
        String enhancementClass = UUID.randomUUID().toString();
        String boundary = "<span class=\"" + enhancementClass + "\"></span>";
        List<Reference> references = new ArrayList<Reference>();

        for (Object item : this) {
            if (item != null) {
                if (item instanceof Reference) {
                    html.append(boundary);
                    references.add((Reference) item);

                } else {
                    html.append(item.toString());
                }
            }
        }

        Document document = Jsoup.parseBodyFragment(html.toString());
        Element body = document.body();

        document.outputSettings().prettyPrint(false);

        if (cleaner != null) {
            cleaner.before(body);
        }

        // Find mistakes in <a> hrefs.
        for (Element a : body.getElementsByTag("a")) {
            String href = a.attr("href");

            if (!ObjectUtils.isBlank(href)) {
                a.attr("href", DUPLICATE_PROTOCOL_PATTERN.matcher(href).replaceAll("$1"));
            }
        }

        // Remove editorial markups.
        body.getElementsByTag("del").remove();
        body.getElementsByTag("ins").unwrap();
        body.getElementsByClass("rte").remove();
        body.select("code[data-annotations]").remove();

        if (!inline) {
            body.select(".cms-textAlign-left, .cms-textAlign-center, .cms-textAlign-right, ol, ul").forEach(element -> {
                Element next = element.nextElementSibling();

                if (next != null && BR_TAG.equals(next.tag())) {
                    next.remove();
                }
            });

            body.select(".cms-textAlign-left, .cms-textAlign-center, .cms-textAlign-right")
                    .forEach(div -> div.tagName(P_TAG.getName()));

            // Convert 'text<br><br>' to '<p>text</p>'.
            for (Element br : body.getElementsByTag("br")) {
                Element previousBr = null;

                // Find the closest previous <br> without any intervening content.
                for (Node previousNode = br;
                        (previousNode = previousNode.previousSibling()) != null;
                        ) {
                    if (previousNode instanceof Element) {
                        Element previousElement = (Element) previousNode;

                        if (BR_TAG.equals(previousElement.tag())) {
                            previousBr = previousElement;
                        }

                        break;

                    } else if (previousNode instanceof TextNode
                            && !((TextNode) previousNode).isBlank()) {
                        break;
                    }
                }

                if (previousBr == null) {
                    continue;
                }

                List<Node> paragraphChildren = new ArrayList<Node>();

                for (Node previous = previousBr;
                        (previous = previous.previousSibling()) != null;
                        ) {
                    if (previous instanceof Element
                            && ((Element) previous).isBlock()) {
                        break;

                    } else {
                        paragraphChildren.add(previous);
                    }
                }

                Element paragraph = new Element(P_TAG, "");

                for (Node child : paragraphChildren) {
                    child.remove();
                    paragraph.prependChild(child.clone());
                }

                br.before(paragraph);
                br.remove();
                previousBr.remove();
            }

            // Convert inline text first in body and after block elements into
            // paragraphs.
            if (body.childNodeSize() > 0) {
                Node next = body.childNode(0);

                do {
                    if (!(next instanceof TextNode
                            && ((TextNode) next).isBlank())) {
                        break;
                    }
                } while ((next = next.nextSibling()) != null);

                Element lastParagraph = inlineTextToParagraph(next);

                if (lastParagraph != null) {
                    body.prependChild(lastParagraph);
                }
            }

            for (Element paragraph : body.getAllElements()) {
                if (!paragraph.isBlock()) {
                    continue;
                }

                Node next = paragraph;

                while ((next = next.nextSibling()) != null) {
                    if (!(next instanceof TextNode
                            && ((TextNode) next).isBlank())) {
                        break;
                    }
                }

                Element lastParagraph = inlineTextToParagraph(next);

                if (lastParagraph != null) {
                    paragraph.after(lastParagraph);
                }
            }

            // Convert '<div>text<div><div><br></div>' to '<p>text</p>'
            List<Element> divs = new ArrayList<Element>();

            DIV: for (Element div : body.getElementsByTag("div")) {
                Element brDiv = nextTag(DIV_TAG, div);

                if (brDiv == null) {
                    continue;
                }

                // '<div><br></div>'?
                boolean sawBr = false;

                for (Node child : brDiv.childNodes()) {
                    if (child instanceof TextNode) {
                        if (!((TextNode) child).isBlank()) {
                            continue DIV;
                        }

                    } else if (child instanceof Element
                            && BR_TAG.equals(((Element) child).tag())) {
                        if (sawBr) {
                            continue DIV;

                        } else {
                            sawBr = true;
                        }

                    } else {
                        continue DIV;
                    }
                }

                divs.add(div);
                div.tagName("p");
                brDiv.remove();
            }

            for (Element div : divs) {
                div = nextTag(DIV_TAG, div);

                if (div != null) {
                    div.tagName("p");
                }
            }

            // Unwrap nested '<p>'s.
            for (Element paragraph : body.getElementsByTag(P_TAG.getName())) {
                if (paragraph.getElementsByTag(P_TAG.getName()).size() > 1) {
                    paragraph.unwrap();
                }
            }

            // <p>before [enh] after</p> -> <p>before</p> [enh] <p>after</p>
            for (Element enhancement : body.getElementsByClass(enhancementClass)) {
                Element paragraph = enhancement.parent();

                if (P_TAG.equals(paragraph.tag())) {
                    Element before = new Element(P_TAG, "");
                    List<Node> beforeChildren = new ArrayList<Node>();

                    for (Node previous = enhancement.previousSibling();
                            previous != null;
                            previous = previous.previousSibling()) {
                        beforeChildren.add(previous);
                    }

                    for (int i = beforeChildren.size() - 1; i >= 0; -- i) {
                        before.appendChild(beforeChildren.get(i));
                    }

                    if (!before.childNodes().isEmpty()) {
                        before.attributes().addAll(paragraph.attributes());
                        paragraph.before(before);
                    }

                    paragraph.before(enhancement);
                }
            }
        }

        if (cleaner != null) {
            cleaner.after(body);
        }

        // Remove empty paragraphs and stringify.
        StringBuilder cleaned = new StringBuilder();

        for (Node child : body.childNodes()) {
            if (child instanceof Element) {
                Element childElement = (Element) child;

                if (P_TAG.equals(childElement.tag())
                        && !childElement.hasText()
                        && childElement.children().isEmpty()) {
                    continue;
                }
            }

            cleaned.append(child.toString());
        }

        List<Object> publishables = new ArrayList<Object>();

        addByBoundary(publishables, cleaned.toString(), boundary, references);
        return publishables;
    }

    // Find the closest next tag without any intervening content.
    private Element nextTag(Tag tag, Element current) {
        Element nextTag = null;

        for (Node nextNode = current;
                (nextNode = nextNode.nextSibling()) != null;
                ) {
            if (nextNode instanceof Element) {
                Element nextElement = (Element) nextNode;

                if (tag.equals(nextElement.tag())) {
                    nextTag = nextElement;
                }

                break;

            } else if (nextNode instanceof TextNode
                    && !((TextNode) nextNode).isBlank()) {
                break;
            }
        }

        return nextTag;
    }

    private Element inlineTextToParagraph(Node next) {
        if (next == null) {
            return null;
        }

        List<Node> paragraphChildren = new ArrayList<Node>();

        do {
            if (next instanceof Element
                    && ((Element) next).isBlock()) {
                break;

            } else {
                paragraphChildren.add(next);
            }
        } while ((next = next.nextSibling()) != null);

        if (paragraphChildren.isEmpty()) {
            return null;
        }

        Element lastParagraph = new Element(P_TAG, "");

        for (Node child : paragraphChildren) {
            child.remove();
            lastParagraph.appendChild(child.clone());
        }

        return lastParagraph;
    }

    // --- AbstractList support ---

    private Object checkItem(Object item) {
        Preconditions.checkNotNull(item);

        if (item instanceof Reference) {
            return item;

        } else if (item instanceof Map) {
            Reference ref = null;
            UUID id = ObjectUtils.to(UUID.class, ((Map<?, ?>) item).get("_id"));
            UUID typeId = ObjectUtils.to(UUID.class, ((Map<?, ?>) item).get("_type"));
            ObjectType type = Database.Static.getDefault().getEnvironment().getTypeById(typeId);

            if (type != null) {
                Object object = type.createObject(id);

                if (object instanceof Reference) {
                    ref = (Reference) object;
                }
            }

            if (ref == null) {
                ref = new Reference();
            }

            ref.getState().setResolveInvisible(isResolveInvisible());

            for (Map.Entry<?, ?> entry : ((Map<?, ?>) item).entrySet()) {
                Object key = entry.getKey();

                ref.getState().put(key != null ? key.toString() : null, entry.getValue());
            }

            return ref;

        } else {
            return item.toString();
        }
    }

    @Override
    public void add(int index, Object item) {
        list.add(index, item);
    }

    @Override
    public Object get(int index) {
        Object item = checkItem(list.get(index));

        list.set(index, item);

        return item;
    }

    @Override
    public Object remove(int index) {
        return list.remove(index);
    }

    @Override
    public Object set(int index, Object item) {
        return list.set(index, item);
    }

    @Override
    public int size() {
        return list.size();
    }

    public static interface Cleaner {

        public void before(Element body);

        public void after(Element body);
    }
}
