package com.psddev.dari.db;

import org.joda.time.DateTime;

/**
 * Interface used to specify how often field value recalculation should occur.
 *
 * @see Recordable.Recalculate#delay()
 */
public interface RecalculationDelay {

    /**
     * Returns {@code true} if the field value should be recalculated based
     * on the given {@code currentTime} and {@code lastRunTime}.
     *
     * @param currentTime Can't be {@code null}.
     * @param lastRunTime {@code null} if recalculation never ran.
     */
    boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime);

    /**
     * Implementation that recalculates the field value every minute.
     */
    class Minute implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(1).isAfter(lastRunTime);
        }
    }

    /**
     * Implementation that recalculates the field value every 15 minutes.
     */
    class QuarterHour implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(15).isAfter(lastRunTime);
        }
    }

    /**
     * Implementation that recalculates the field value every 30 minutes.
     */
    class HalfHour implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusMinutes(30).isAfter(lastRunTime);
        }
    }

    /**
     * Implementation that recalculates the field value every hour.
     */
    class Hour implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(1).isAfter(lastRunTime);
        }
    }

    /**
     * Implementation that recalculates the field value every 12 hours.
     */
    class HalfDay implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusHours(12).isAfter(lastRunTime);
        }
    }

    /**
     * Implementation that recalculates the field value every day.
     */
    class Day implements RecalculationDelay {

        @Override
        public boolean isUpdateDue(DateTime currentTime, DateTime lastRunTime) {
            return lastRunTime == null || currentTime.minusDays(1).isAfter(lastRunTime);
        }
    }
}
