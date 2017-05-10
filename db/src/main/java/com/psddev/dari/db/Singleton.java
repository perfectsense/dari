package com.psddev.dari.db;

public interface Singleton extends Recordable {

    @FieldInternalNamePrefix("dari.singleton.")
    public static final class Data extends Modification<Singleton> {

        @Indexed(unique = true)
        @Required
        private String key;

        @Override
        protected void beforeSave() {
            key = getState().getType().getInternalName();
        }
    }
}
