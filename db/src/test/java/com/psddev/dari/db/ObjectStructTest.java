package com.psddev.dari.db;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ObjectStructTest {

    @Test(expected = NullPointerException.class)
    public void findIndexedFieldsNull() {
        ObjectStruct.findIndexedFields(null);
    }

    @Test
    public void findIndexedFields() {
        List<ObjectField> fields = new ArrayList<>();

        for (int i = 0; i < 3; ++ i) {
            ObjectField field = mock(ObjectField.class);

            when(field.getInternalName()).thenReturn("field" + i);
            fields.add(field);
        }

        ObjectIndex index = mock(ObjectIndex.class);

        when(index.getUniqueName()).thenReturn("index0");

        ObjectField field0 = fields.get(0);
        ObjectField field1 = fields.get(1);

        when(index.getFields()).thenReturn(Arrays.asList("field0", "field1"));

        ObjectStruct struct = new TestObjectStruct();

        struct.setFields(fields);
        struct.setIndexes(Collections.singletonList(index));

        assertThat(
                ObjectStruct.findIndexedFields(struct),
                containsInAnyOrder(field0, field1));
    }

    private static class TestObjectStruct implements ObjectStruct {

        private DatabaseEnvironment environment = mock(DatabaseEnvironment.class);
        private List<ObjectField> fields;
        private List<ObjectIndex> indexes;

        @Override
        public DatabaseEnvironment getEnvironment() {
            return environment;
        }

        @Override
        public List<ObjectField> getFields() {
            return new ArrayList<>(fields);
        }

        @Override
        public ObjectField getField(String name) {
            return fields.stream()
                    .filter(field -> field.getInternalName().equals(name))
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public void setFields(List<ObjectField> fields) {
            this.fields = fields != null ? fields : new ArrayList<>();
        }

        @Override
        public List<ObjectIndex> getIndexes() {
            return new ArrayList<>(indexes);
        }

        @Override
        public ObjectIndex getIndex(String name) {
            return indexes.stream()
                    .filter(index -> index.getUniqueName().equals(name))
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public void setIndexes(List<ObjectIndex> indexes) {
            this.indexes = indexes != null ? indexes : new ArrayList<>();
        }
    }
}
