package com.psddev.dari.test;

import com.psddev.dari.db.Query;
import com.psddev.dari.util.TypeDefinition;
import org.hamcrest.beans.HasPropertyWithValue;
import org.hamcrest.core.Every;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ModificationEmbeddedTest extends AbstractTest {

    static final String VALUE = "foo";
    static final String JUNKVALUE = "junk";
    static final List<String> VALUES = Arrays.asList("foo", "bar", "baz");
    static final List<String> JUNKVALUES = Arrays.asList("abe", "lincoln", "president");

    protected Query<ModificationEmbeddedModel> query() {
        return Query.from(ModificationEmbeddedModel.class);
    }

    @After
    public void deleteModels() {
        Query.from(ModificationEmbeddedModel.class).deleteAll();
        Query.from(IndexTag.class).deleteAll();
    }

    @Test
    public void testBasic() {
        ModificationEmbeddedModel test = new ModificationEmbeddedModel();
        test.setName("Test");
        test.save();

        List<ModificationEmbeddedModel> m  = Query.from(ModificationEmbeddedModel.class).selectAll();
        long c = Query.from(ModificationEmbeddedModel.class).count();

        assertThat(c, is(1L));
        assertThat(m, (Every.everyItem(HasPropertyWithValue.hasProperty("name", Is.is("Test")))));
    }

    private List<IndexTag> buildList(List<String> values) {
        List<IndexTag> eList = new ArrayList<>();
        for (String v : values) {
            eList.add(buildValue(v));
        }
        return eList;
    }

    private Set<IndexTag> buildSet(List<String> values) {
        Set<IndexTag> eList = new HashSet<>();
        for (String v : values) {
            eList.add(buildValue(v));
        }
        return eList;
    }

    @Test
    public void testSingleString() {

        List<IndexTag> eList = buildList(VALUES);
        Set<IndexTag> eSet = buildSet(VALUES);
        IndexTag eValue = buildValue(VALUE);

        List<IndexTag> eListJunk = buildList(JUNKVALUES);
        Set<IndexTag> eSetJunk = buildSet(JUNKVALUES);
        IndexTag eValueJunk = buildValue(JUNKVALUE);

        ModificationEmbeddedModel test = new ModificationEmbeddedModel();
        test.setName(VALUE);
        test.as(TaggableEmbeddedModification.class).setOtherTags(eList);
        test.as(TaggableEmbeddedModification.class).setName(VALUE);
        test.as(TaggableEmbeddedModification.class).setPrimaryTag(eValue);
        test.as(TaggableEmbeddedModification.class).setOtherTagsSet(eSet);
        test.save();

        ModificationEmbeddedModel test2 = new ModificationEmbeddedModel();
        test2.setName(JUNKVALUE);
        test2.as(TaggableEmbeddedModification.class).setOtherTags(eListJunk);
        test2.as(TaggableEmbeddedModification.class).setName(JUNKVALUE);
        test2.as(TaggableEmbeddedModification.class).setPrimaryTag(eValueJunk);
        test2.as(TaggableEmbeddedModification.class).setOtherTagsSet(eSetJunk);
        test2.save();

        List<ModificationEmbeddedModel> junk = Query.from(ModificationEmbeddedModel.class).where("tgd.otherTags/name = ?", "junk").selectAll();
        assertThat(junk, hasSize(0));

        List<ModificationEmbeddedModel> foo1 = Query.from(ModificationEmbeddedModel.class).where("tgd.otherTags/name = ?", VALUE).selectAll();
        assertThat(foo1, hasSize(1));
        assertThat(foo1.get(0).getName(), is(VALUE));
        List<IndexTag> foo1Other = foo1.get(0).as(TaggableEmbeddedModification.class).getOtherTags();
        assertThat(foo1Other.get(0).getName(), is(VALUES.get(0)));
        assertThat(foo1Other.get(1).getName(), is(VALUES.get(1)));
        assertThat(foo1Other.get(2).getName(), is(VALUES.get(2)));

        List<ModificationEmbeddedModel> fooResult = Query.from(ModificationEmbeddedModel.class).where("tgd.primaryTag/name = ?", VALUE).selectAll();
        assertThat(fooResult, hasSize(1));
        assertThat(foo1.get(0).getName(), is(VALUE));

        List<ModificationEmbeddedModel> foo2Result = Query.from(ModificationEmbeddedModel.class).where("tgd.otherTagsSet/name = ?", VALUES).selectAll();
        assertThat(foo2Result, hasSize(1));
        assertThat(foo1.get(0).getName(), is(VALUE));
    }

    @Test(expected = Query.NoIndexException.class)
    public void testNotIndexedModification() {
        ModificationEmbeddedModel test = new ModificationEmbeddedModel();
        test.save();

        Query.from(ModificationEmbeddedModel.class).where("tgd.name = ?", VALUE).selectAll();
    }

    private IndexTag buildValue(String value) {
        IndexTag e = TypeDefinition.getInstance(IndexTag.class).newInstance();
        e.setName(value);
        return e;
    }
}

