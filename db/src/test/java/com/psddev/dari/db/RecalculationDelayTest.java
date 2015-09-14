package com.psddev.dari.db;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

public class RecalculationDelayTest {

    private DateTime now;

    @Before
    public void before() {
        now = new DateTime();
    }

    @Test
    public void minute() {
        RecalculationDelay.Minute delay = new RecalculationDelay.Minute();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusSeconds(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusHours(1)), equalTo(true));
    }

    @Test
    public void quarterHour() {
        RecalculationDelay.QuarterHour delay = new RecalculationDelay.QuarterHour();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusMinutes(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusHours(1)), equalTo(true));
    }

    @Test
    public void halfHour() {
        RecalculationDelay.HalfHour delay = new RecalculationDelay.HalfHour();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusMinutes(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusHours(1)), equalTo(true));
    }

    @Test
    public void hour() {
        RecalculationDelay.Hour delay = new RecalculationDelay.Hour();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusMinutes(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusDays(1)), equalTo(true));
    }

    @Test
    public void halfDay() {
        RecalculationDelay.HalfDay delay = new RecalculationDelay.HalfDay();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusHours(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusDays(1)), equalTo(true));
    }

    @Test
    public void day() {
        RecalculationDelay.Day delay = new RecalculationDelay.Day();

        assertThat(delay.isUpdateDue(now, null), equalTo(true));
        assertThat(delay.isUpdateDue(now, now.minusHours(1)), equalTo(false));
        assertThat(delay.isUpdateDue(now, now.minusMonths(1)), equalTo(true));
    }
}
