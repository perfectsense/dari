package com.psddev.dari.db;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

// CHECKSTYLE:OFF
/**
 * Automatically initializes all applications.
 *
 * @deprecated No replacement.
 */
@Deprecated
public class ApplicationInitializer implements Initializer {

    @Override
    public Set<Class<? extends Initializer>> dependencies() {
        Set<Class<? extends Initializer>> set = new HashSet<Class<? extends Initializer>>();
        set.add(TypeInitializer.class);
        return set;
    }

    @Override
    public void execute(Database database, Logger logger) throws Exception {
        ObjectType rootAppType = ObjectType.getInstance(Application.class);
        for (ObjectType appType : rootAppType.findConcreteTypes()) {
            Class<?> appClass = appType.getObjectClass();
            if (appClass != null &&
                    Application.class.isAssignableFrom(appClass)) {
                @SuppressWarnings("unchecked")
                Application app = Application.Static.getInstanceUsing((Class<? extends Application>) appClass, database);
                app.initialize(logger);
                logger.info("Initialized application: {}", appClass.getName());
            }
        }
    }
}
