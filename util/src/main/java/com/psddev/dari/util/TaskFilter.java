package com.psddev.dari.util;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Cleans up after any resources opened by {@link Task}. */
public class TaskFilter extends AbstractFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskFilter.class);

    private final List<Task> tasks = new ArrayList<Task>();

    @Override
    protected void doInit() {
        for (Class<? extends RepeatingTask> taskClass : ClassFinder.findClasses(RepeatingTask.class)) {
            if (taskClass.isAnnotationPresent(Ignored.class)
                    || taskClass.isAnonymousClass()
                    || Modifier.isAbstract(taskClass.getModifiers())) {
                continue;
            }

            try {
                RepeatingTask task = taskClass.newInstance();

                task.scheduleWithFixedDelay(1.0, 1.0);
                tasks.add(task);

            } catch (IllegalAccessException error) {
                LOGGER.warn(String.format("Can't create [%s] task!", taskClass.getName()), error);

            } catch (InstantiationException error) {
                LOGGER.warn(String.format("Can't create [%s] task!", taskClass.getName()), error.getCause());
            }
        }
    }

    @Override
    protected void doDestroy() {
        for (Task task : tasks) {
            task.stop();
        }

        tasks.clear();

        for (TaskExecutor executor : TaskExecutor.Static.getAll()) {
            executor.shutdownNow();
        }
    }

    @Documented
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    public static @interface Ignored { }
}
