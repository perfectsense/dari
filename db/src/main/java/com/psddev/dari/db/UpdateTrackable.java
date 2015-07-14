package com.psddev.dari.db;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

import com.psddev.dari.util.UuidUtils;

/**
 * Interface for tracking updates to all instances of a type.
 */
public interface UpdateTrackable extends Recordable {

    /**
     * Specifies the tracker names for target type.
     */
    @Documented
    @Inherited
    @ObjectType.AnnotationProcessorClass(Static.NamesProcessor.class)
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public @interface Names {

        String[] value();
    }

    /**
     * {@link UpdateTrackable} utility methods.
     */
    public static class Static {

        /**
         * Returns {@code true} if any instances of the types associated
         * with the given {@code name} have been updated since the given
         * {@code time}.
         */
        public static boolean isUpdated(String name, long time) {
            Tracker tracker = Query
                    .from(Tracker.class)
                    .where("_id = ?", createTrackerId(name))
                    .master()
                    .noCache()
                    .first();

            return tracker != null && tracker.getLastUpdate() > time;
        }

        private static UUID createTrackerId(String name) {
            return UuidUtils.createVersion3Uuid("dari.updateTrackable.tracker." + name);
        }

        private static class Tracker extends Record {

            private long lastUpdate;

            public long getLastUpdate() {
                return lastUpdate;
            }

            public void setLastUpdate(long lastUpdate) {
                this.lastUpdate = lastUpdate;
            }
        }

        @SuppressWarnings("unused")
        private static class Trigger extends Modification<UpdateTrackable> {

            @Override
            protected void afterSave() {
                final ObjectType type = getState().getType();

                if (type != null) {
                    (new Thread() {

                        @Override
                        public void run() {
                            for (String name : type.as(TypeData.class).getNames()) {
                                Tracker tracker = new Tracker();

                                tracker.getState().setId(createTrackerId(name));
                                tracker.setLastUpdate(System.currentTimeMillis());
                                tracker.save();
                            }
                        }
                    }).start();
                }
            }

            @Override
            protected void afterDelete() {
                afterSave();
            }
        }

        @FieldInternalNamePrefix("dari.updateTrackable.")
        private static class TypeData extends Modification<ObjectType> {

            private Set<String> names;

            public Set<String> getNames() {
                if (names == null) {
                    names = new LinkedHashSet<String>();
                }
                return names;
            }
        }

        private static class NamesProcessor implements ObjectType.AnnotationProcessor<Names> {

            @Override
            public void process(ObjectType type, Names annotation) {
                Collections.addAll(type.as(TypeData.class).getNames(), annotation.value());
            }
        }
    }
}
