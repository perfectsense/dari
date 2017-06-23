package com.psddev.dari.db;

import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.Settings;

public interface Recordable {

    /**
     * Returns the state {@linkplain State#linkObject linked} to this
     * instance.
     */
    public State getState();

    /**
     * Sets the state {@linkplain State#linkObject linked} to this
     * instance. This method must also {@linkplain State#unlinkObject
     * unlink} the state previously set.
     */
    public void setState(State state);

    /**
     * Returns an instance of the given {@code modificationClass} linked
     * to this object.
     */
    public <T> T as(Class<T> modificationClass);

    // --- Annotations ---

    /**
     * Specifies whether the target type is abstract and can't be used
     * to create a concrete instance.
     */
    @Documented
    @ObjectType.AnnotationProcessorClass(AbstractProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Abstract {
        boolean value() default true;
    }

    /**
     * Specifies the JavaBeans property name that can be used to access an
     * instance of the target type as a modification.
     *
     * <p>For example, given the following modification:</p>
     *
     * <blockquote><pre><code data-type="java">
     *{@literal @}Modification.BeanProperty("css")
     *class CustomCss extends Modification&lt;Object&gt; {
     *    public String getBodyClass() {
     *        return getOriginalObject().getClass().getName().replace('.', '_');
     *    }
     *}
     * </code></pre></blockquote>
     *
     *
     * <p>The following becomes valid and will invoke the {@code getBodyClass}
     * above, even if the {@code content} object doesn't define a
     * {@code getCss} method.</p>
     *
     * <blockquote><pre><code data-type="jsp">
     *${content.css.bodyClass}
     * </code></pre></blockquote>
     */
    @Documented
    @ObjectType.AnnotationProcessorClass(BeanPropertyProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface BeanProperty {
        String value();
    }

    @ObjectField.AnnotationProcessorClass(BootstrapFollowReferencesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface BootstrapFollowReferences {
    }

    @ObjectType.AnnotationProcessorClass(BootstrapPackagesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface BootstrapPackages {
        String[] value();
        Class<?>[] depends() default { };
    }

    @ObjectType.AnnotationProcessorClass(BootstrapTypeMappableProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface BootstrapTypeMappable {
        Class<?>[] groups();
        String uniqueKey();
    }

    /** Specifies the maximum number of items allowed in the target field. */
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CollectionMaximum {
        int value();
    }

    /** Specifies the minimum number of items required in the target field. */
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface CollectionMinimum {
        int value();
    }

    /**
     * Specifies whether the target field is always denormalized within
     * another instance.
     */
    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(DenormalizedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE })
    public @interface Denormalized {
        boolean value() default true;
        String[] fields() default { };
    }

    /** Specifies the target's display name. */
    @Documented
    @ObjectField.AnnotationProcessorClass(DisplayNameProcessor.class)
    @ObjectType.AnnotationProcessorClass(DisplayNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE, ElementType.METHOD })
    public @interface DisplayName {
        String value();
    }

    /**
     * Specifies whether the target data is always embedded within
     * another instance.
     */
    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(EmbeddedProcessor.class)
    @ObjectType.AnnotationProcessorClass(EmbeddedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE, ElementType.METHOD })
    public @interface Embedded {
        boolean value() default true;
    }

    /**
     * Specifies the prefix for the internal names of all fields in the
     * target type.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface FieldInternalNamePrefix {
        String value();
    }

    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(GroupsProcessor.class)
    @ObjectType.AnnotationProcessorClass(GroupsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE })
    public @interface Groups {
        String[] value();
    }

    /** Specifies whether the target field is ignored. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    public @interface Ignored {
        boolean value() default true;
    }

    /**
     * Specifies whether the target field should be ignored when the object
     * is embedded in another.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(IgnoredIfEmbeddedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    @interface IgnoredIfEmbedded {
        boolean value() default true;
    }

    /** Specifies whether the target field value is indexed. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    public @interface Indexed {
        String[] extraFields() default { };
        boolean unique() default false;
        boolean caseSensitive() default false;
        boolean visibility() default false;

        /** @deprecated Use {@link #unique} instead. */
        @Deprecated
        boolean isUnique() default false;
    }

    /** Specifies the target's internal name. */
    @Documented
    @ObjectType.AnnotationProcessorClass(InternalNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.TYPE, ElementType.METHOD })
    public @interface InternalName {
        String value();
    }

    /**
     * Specifies the name of the field in the junction query that should be
     * used to populate the target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(JunctionFieldProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface JunctionField {
        String value();
    }

    /**
     * Specifies the name of the position field in the junction query that
     * should be used to order the collection in the target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(JunctionPositionFieldProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface JunctionPositionField {
        String value();
    }

    /**
     * Specifies the field names that are used to retrieve the
     * labels of the objects represented by the target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(LabelFieldsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface LabelFields {
        String[] value();
    }

    /**
     * Specifies either the maximum numeric value or string length of the
     * target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(MaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Maximum {
        double value();
    }

    /** Specifies the field the metric is recorded in. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @ObjectField.AnnotationProcessorClass(MetricValueProcessor.class)
    @Target(ElementType.FIELD)
    public @interface MetricValue {
        Class<? extends MetricInterval> interval() default MetricInterval.Hourly.class;
        String intervalSetting() default "";
    }

    /**
     * Specifies either the minimum numeric value or string length of the
     * target field.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(MinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Minimum {
        double value();
    }

    /**
     * Specifies the field name used to retrieve the previews of the
     * objects represented by the target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(PreviewFieldProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface PreviewField {
        String value();
    }

    /**
     * Specifies that the values in the target collection field should
     * remain raw (not reference resolved).
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    @ObjectField.AnnotationProcessorClass(RawProcessor.class)
    @interface Raw {
        boolean value() default true;
    }

    /** Specifies how the method index should be updated. */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.METHOD })
    @ObjectField.AnnotationProcessorClass(RecalculateProcessor.class)
    public @interface Recalculate {
        public Class<? extends RecalculationDelay> delay() default RecalculationDelay.Hour.class;
        public String metric() default "";
        public boolean immediate() default false;
    }

    /**
     * Specifies the regular expression pattern that the target field value
     * must match.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(RegexProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Regex {
        String value();
        String validationMessage() default "";
    }

    /** Specifies whether the target field value is required. */
    @Documented
    @ObjectField.AnnotationProcessorClass(RequiredProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Required {
        boolean value() default true;
    }

    /** Specifies the source database class for the target type. */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(SourceDatabaseClassProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SourceDatabaseClass {
        Class<? extends Database> value();
    }

    /** Specifies the source database name for the target type. */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(SourceDatabaseNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface SourceDatabaseName {
        String value();
    }

    /**
     * Specifies the step between the minimum and the maximum that the
     * target field must match.
     */
    @Documented
    @ObjectField.AnnotationProcessorClass(StepProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Step {
        double value();
    }

    /**
     * Specifies the type ID of the target type during initial bootstrap. It
     * has no effect if one has already been assigned.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface TypeId {
        String value();
    }

    /**
     * Specifies the processor class(es) to run after the type is initialized.
     */
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface TypePostProcessorClasses {
        Class<? extends ObjectType.PostProcessor>[] value();
    }

    /** Specifies the valid types for the target field value. */
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Types {
        Class<?>[] value();
    }

    /** Specifies the valid values for the target field value. */
    @Documented
    @ObjectField.AnnotationProcessorClass(ValuesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    public @interface Values {
        String[] value();
    }

    @Documented
    @Inherited
    @ObjectField.AnnotationProcessorClass(WhereProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface Where {
        String value();
        String validationMessage() default "";
    }

    // --- Deprecated ---

    /** @deprecated Use {@link Denormalized} instead. */
    @Deprecated
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(DenormalizedFieldsProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface DenormalizedFields {
        String[] value();
    }

    /** @deprecated Use {@link CollectionMaximum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldCollectionMaximum {
        int value();
    }

    /** @deprecated Use {@link CollectionMinimum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(CollectionMinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldCollectionMinimum {
        int value();
    }

    /** @deprecated Use {@link DisplayName} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(DisplayNameProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldDisplayName {
        String value();
    }

    /** @deprecated Use {@link Embedded} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(EmbeddedProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldEmbedded {
        boolean value() default true;
    }

    /** @deprecated Use {@link Modification} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldGlobal {
    }

    /** @deprecated Use {@link Ignored} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldIgnored {
    }

    /** @deprecated Use {@link Indexed} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldIndexed {
        String[] extraFields() default { };
        boolean isUnique() default false;
    }

    /** @deprecated Use {@link InternalName} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.FIELD, ElementType.METHOD })
    public @interface FieldInternalName {
        String value();
    }

    /** @deprecated Use {@link FieldTypes} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldItemTypes {
        Class<?>[] value();
    }

    /** @deprecated Use {@link Maximum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(MaximumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldMaximum {
        double value();
    }

    /** @deprecated Use {@link Minimum} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(MinimumProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldMinimum {
        double value();
    }

    /** @deprecated Use {@link Regex} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(RegexProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldPattern {
        String value();
    }

    /** @deprecated Use {@link Required} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(RequiredProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldRequired {
        boolean value() default true;
    }

    /** @deprecated Use {@link Step} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(StepProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldStep {
        double value();
    }

    /** @deprecated Use {@link Types} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(TypesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldTypes {
        Class<?>[] value();
    }

    /** @deprecated Use {@link FieldIndexed} with {@code isUnique} instead. */
    @Deprecated
    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldUnique {
    }

    /** @deprecated Use {@link Values} instead. */
    @Deprecated
    @Documented
    @ObjectField.AnnotationProcessorClass(ValuesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface FieldValues {
        String[] value();
    }
}

// --- Internal ---

class AbstractProcessor implements ObjectType.AnnotationProcessor<Recordable.Abstract> {
    @Override
    public void process(ObjectType type, Recordable.Abstract annotation) {
        type.setAbstract(annotation.value());
    }
}

class BeanPropertyProcessor implements ObjectType.AnnotationProcessor<Recordable.BeanProperty> {
    @Override
    public void process(ObjectType type, Recordable.BeanProperty annotation) {
        if (type.getGroups().contains(Modification.class.getName())) {
            type.setJavaBeanProperty(annotation.value());
        }
    }
}

class CollectionMaximumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        if (field.isInternalCollectionType()) {
            field.setCollectionMaximum(annotation instanceof Recordable.FieldCollectionMaximum
                    ? ((Recordable.FieldCollectionMaximum) annotation).value()
                    : ((Recordable.CollectionMaximum) annotation).value());
        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] annotation cannot be applied to a non-collection field!",
                    annotation.getClass().getName()));
        }
    }
}

class CollectionMinimumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        if (field.isInternalCollectionType()) {
            field.setCollectionMinimum(annotation instanceof Recordable.FieldCollectionMinimum
                    ? ((Recordable.FieldCollectionMinimum) annotation).value()
                    : ((Recordable.CollectionMinimum) annotation).value());
        } else {
            throw new IllegalArgumentException(String.format(
                    "[%s] annotation cannot be applied to a non-collection field!",
                    annotation.getClass().getName()));
        }
    }
}

class DenormalizedProcessor implements
        ObjectField.AnnotationProcessor<Recordable.Denormalized>,
        ObjectType.AnnotationProcessor<Recordable.Denormalized> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Denormalized annotation) {
        field.setDenormalized(annotation.value());
        Collections.addAll(field.getDenormalizedFields(), annotation.fields());
    }

    @Override
    public void process(ObjectType type, Recordable.Denormalized annotation) {
        type.setDenormalized(annotation.value());
        Collections.addAll(type.getDenormalizedFields(), annotation.fields());
    }
}

class DenormalizedFieldsProcessor implements ObjectType.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, Annotation annotation) {
        type.setDenormalized(true);
        Collections.addAll(type.getDenormalizedFields(), ((Recordable.DenormalizedFields) annotation).value());
    }
}

class DisplayNameProcessor implements
        ObjectField.AnnotationProcessor<Annotation>,
        ObjectType.AnnotationProcessor<Recordable.DisplayName> {

    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setDisplayName(annotation instanceof Recordable.FieldDisplayName
                ? ((Recordable.FieldDisplayName) annotation).value()
                : ((Recordable.DisplayName) annotation).value());
    }

    @Override
    public void process(ObjectType type, Recordable.DisplayName annotation) {
        Class<?> objectClass = type.getObjectClass();

        if (objectClass != null) {
            Recordable.DisplayName typeAnnotation = objectClass.getAnnotation(Recordable.DisplayName.class);

            // Only set the display name if the annotation came from the type
            // being modified.
            if (typeAnnotation != null) {
                String displayName = annotation.value();

                if (!ObjectUtils.isBlank(displayName) && annotation.equals(typeAnnotation)) {
                    type.setDisplayName(displayName);
                }
            }
        }
    }
}

class EmbeddedProcessor implements
        ObjectField.AnnotationProcessor<Annotation>,
        ObjectType.AnnotationProcessor<Recordable.Embedded> {

    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setEmbedded(annotation instanceof Recordable.FieldEmbedded
                ? ((Recordable.FieldEmbedded) annotation).value()
                : ((Recordable.Embedded) annotation).value());
    }

    @Override
    public void process(ObjectType type, Recordable.Embedded annotation) {
        type.setEmbedded(annotation.value());
    }
}

class GroupsProcessor implements
        ObjectField.AnnotationProcessor<Recordable.Groups>,
        ObjectType.AnnotationProcessor<Recordable.Groups> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Groups annotation) {
        Collections.addAll(field.getGroups(), annotation.value());
    }

    @Override
    public void process(ObjectType type, Recordable.Groups annotation) {
        Collections.addAll(type.getGroups(), annotation.value());
    }
}

class IgnoredIfEmbeddedProcessor implements ObjectField.AnnotationProcessor<Recordable.IgnoredIfEmbedded> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.IgnoredIfEmbedded annotation) {
        field.setIgnoredIfEmbedded(annotation.value());
    }
}

class InternalNameProcessor implements ObjectType.AnnotationProcessor<Recordable.InternalName> {
    @Override
    public void process(ObjectType type, Recordable.InternalName annotation) {
        type.setInternalName(annotation.value());
    }
}

class RawProcessor implements ObjectField.AnnotationProcessor<Recordable.Raw> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Raw annotation) {
        field.setRaw(annotation.value());
    }
}

class RecalculateProcessor implements ObjectField.AnnotationProcessor<Recordable.Recalculate> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Recalculate annotation) {

        if (field instanceof ObjectMethod) {
            RecalculationFieldData fieldData = ((ObjectMethod) field).as(RecalculationFieldData.class);

            fieldData.setDelayClass(annotation.delay());
            fieldData.setImmediate(annotation.immediate());

            if (annotation.metric() != null && !"".equals(annotation.metric())) {
                fieldData.setMetricFieldName(annotation.metric());
            } else if (annotation.immediate()) {
                throw new IllegalArgumentException("immediate = true requires a metric!");
            }
        }
    }
}

class JunctionFieldProcessor implements ObjectField.AnnotationProcessor<Recordable.JunctionField> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.JunctionField annotation) {
        field.setJunctionField(annotation.value());
    }
}

class JunctionPositionFieldProcessor implements ObjectField.AnnotationProcessor<Recordable.JunctionPositionField> {

    @Override
    public void process(ObjectType type, ObjectField field, Recordable.JunctionPositionField annotation) {
        field.setJunctionPositionField(annotation.value());
    }
}

class LabelFieldsProcessor implements ObjectType.AnnotationProcessor<Recordable.LabelFields> {
    @Override
    public void process(ObjectType type, Recordable.LabelFields annotation) {
        List<String> labelFields = type.getLabelFields();
        for (String field : annotation.value()) {
            if (!labelFields.contains(field)) {
                labelFields.add(field);
            }
        }
    }
}

class MaximumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setMaximum(annotation instanceof Recordable.FieldMaximum
                ? ((Recordable.FieldMaximum) annotation).value()
                : ((Recordable.Maximum) annotation).value());
    }
}

class MetricValueProcessor implements ObjectField.AnnotationProcessor<Recordable.MetricValue> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.MetricValue annotation) {
        SqlDatabase.FieldData fieldData = field.as(SqlDatabase.FieldData.class);
        MetricAccess.FieldData metricFieldData = field.as(MetricAccess.FieldData.class);

        fieldData.setIndexTable(MetricAccess.METRIC_TABLE);
        fieldData.setIndexTableColumnName(MetricAccess.METRIC_DATA_FIELD);
        fieldData.setIndexTableSameColumnNames(false);
        fieldData.setIndexTableSource(true);
        fieldData.setIndexTableReadOnly(true);

        metricFieldData.setEventDateProcessorClass(annotation.interval());
        if (!"".equals(annotation.intervalSetting())) {
            String settingValue = Settings.getOrDefault(String.class, annotation.intervalSetting(), null);
            if (settingValue != null) {
                metricFieldData.setEventDateProcessorClassName(settingValue);
            }
        }

        metricFieldData.setMetricValue(true);
    }
}

class MinimumProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setMinimum(annotation instanceof Recordable.FieldMinimum
                ? ((Recordable.FieldMinimum) annotation).value()
                : ((Recordable.Minimum) annotation).value());
    }
}

class PreviewFieldProcessor implements ObjectType.AnnotationProcessor<Recordable.PreviewField> {
    @Override
    public void process(ObjectType type, Recordable.PreviewField annotation) {
        type.setPreviewField(annotation.value());
    }
}

class RegexProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setPattern(annotation instanceof Recordable.FieldPattern
                ? ((Recordable.FieldPattern) annotation).value()
                : ((Recordable.Regex) annotation).value());

        if (annotation instanceof Recordable.Regex) {
            field.setPredicateValidationMessage(((Recordable.Regex) annotation).validationMessage());
        }
    }
}

class RequiredProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setRequired(annotation instanceof Recordable.FieldRequired
                ? ((Recordable.FieldRequired) annotation).value()
                : ((Recordable.Required) annotation).value());
    }
}

class SourceDatabaseClassProcessor implements ObjectType.AnnotationProcessor<Recordable.SourceDatabaseClass> {
    @Override
    public void process(ObjectType type, Recordable.SourceDatabaseClass annotation) {
        type.setSourceDatabaseClassName(annotation.value().getName());
    }
}

class SourceDatabaseNameProcessor implements ObjectType.AnnotationProcessor<Recordable.SourceDatabaseName> {
    @Override
    public void process(ObjectType type, Recordable.SourceDatabaseName annotation) {
        type.setSourceDatabaseName(annotation.value());
    }
}

class StepProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        field.setStep(annotation instanceof Recordable.FieldStep
                ? ((Recordable.FieldStep) annotation).value()
                : ((Recordable.Step) annotation).value());
    }
}

class TypesProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        Set<ObjectType> types = new LinkedHashSet<ObjectType>();
        DatabaseEnvironment environment = field.getParent().getEnvironment();
        for (Class<?> typeClass : annotation instanceof Recordable.FieldTypes
                ? ((Recordable.FieldTypes) annotation).value() : annotation instanceof Recordable.FieldItemTypes
                ? ((Recordable.FieldItemTypes) annotation).value() : ((Recordable.Types) annotation).value()) {
            types.add(environment.getTypeByClass(typeClass));
        }
        field.setTypes(types);
    }
}

class ValuesProcessor implements ObjectField.AnnotationProcessor<Annotation> {
    @Override
    @SuppressWarnings({ "all", "deprecation" })
    public void process(ObjectType type, ObjectField field, Annotation annotation) {
        Set<ObjectField.Value> values = new LinkedHashSet<ObjectField.Value>();
        for (String valueValue : annotation instanceof Recordable.FieldValues
                ? ((Recordable.FieldValues) annotation).value()
                : ((Recordable.Values) annotation).value()) {
            ObjectField.Value value = new ObjectField.Value();
            value.setValue(valueValue);
            values.add(value);
        }
        field.setValues(values);
    }
}

class WhereProcessor implements ObjectField.AnnotationProcessor<Recordable.Where> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.Where annotation) {
        field.setPredicate(annotation.value());
        field.setPredicateValidationMessage(annotation.validationMessage());
    }
}

class BootstrapPackagesProcessor implements ObjectType.AnnotationProcessor<Recordable.BootstrapPackages> {
    @Override
    public void process(ObjectType type, Recordable.BootstrapPackages annotation) {
        Set<String> packageNames = new LinkedHashSet<String>();
        for (String packageName : annotation.value()) {
            packageNames.add(packageName);
            for (Class<?> dependency : annotation.depends()) {
                ObjectType dependentType = type.getEnvironment().getTypeByClass(dependency);
                if (dependentType != null) {
                    dependentType.as(BootstrapPackage.TypeData.class).getPackageNames().add(packageName);
                }
            }
        }
        type.as(BootstrapPackage.TypeData.class).setPackageNames(packageNames);
    }
}

class BootstrapTypeMappableProcessor implements ObjectType.AnnotationProcessor<Recordable.BootstrapTypeMappable> {
    @Override
    public void process(ObjectType type, Recordable.BootstrapTypeMappable annotation) {
        Set<String> typeMappableGroups = new LinkedHashSet<String>();
        for (Class<?> group : annotation.groups()) {
            typeMappableGroups.add(group.getName());
        }
        type.as(BootstrapPackage.TypeData.class).setTypeMappableGroups(typeMappableGroups);
        type.as(BootstrapPackage.TypeData.class).setTypeMappableUniqueKey(annotation.uniqueKey());
    }
}

class BootstrapFollowReferencesProcessor implements ObjectField.AnnotationProcessor<Recordable.BootstrapFollowReferences> {
    @Override
    public void process(ObjectType type, ObjectField field, Recordable.BootstrapFollowReferences annotation) {
        type.as(BootstrapPackage.TypeData.class).getFollowReferencesFields().add(field.getInternalName());
    }
}
