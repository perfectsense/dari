package com.psddev.dari.util;

import java.util.concurrent.atomic.AtomicReference;

import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;

/** Task that repeatedly runs at a predictable interval. */
public abstract class RepeatingTask extends Task {

    private final AtomicReference<DateTime> previousRunTime = new AtomicReference<DateTime>(calculateRunTime(new DateTime()));

    protected RepeatingTask(String executor, String name) {
        super(executor, name);
    }

    protected RepeatingTask() {
        this(null, null);
    }

    protected abstract DateTime calculateRunTime(DateTime currentTime);

    protected DateTime every(DateTime currentTime, DateTimeFieldType unit, int offset, int interval) {
        DateTime d = currentTime.property(unit).roundFloorCopy();
        d = d.withFieldAdded(unit.getDurationType(), offset);
        return d.withField(unit, (d.get(unit) / interval) * interval);
    }

    protected DateTime everyMinute(DateTime currentTime) {
        return every(currentTime, DateTimeFieldType.minuteOfHour(), 0, 1);
    }

    protected DateTime everyHour(DateTime currentTime) {
        return every(currentTime, DateTimeFieldType.hourOfDay(), 0, 1);
    }

    protected DateTime everyDay(DateTime currentTime) {
        return every(currentTime, DateTimeFieldType.dayOfMonth(), 0, 1);
    }

    protected abstract void doRepeatingTask(DateTime runTime) throws Exception;

    @Override
    protected final void doTask() throws Exception {
        DateTime now = new DateTime();
        DateTime oldPrevious = previousRunTime.get();
        DateTime newPrevious = calculateRunTime(now);

        if (!now.isBefore(newPrevious)
                && oldPrevious.isBefore(newPrevious)
                && previousRunTime.compareAndSet(oldPrevious, newPrevious)) {
            doRepeatingTask(newPrevious);
        } else {
            skipRunCount();
        }
    }
}
