package com.psddev.dari.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PingTest {

    @Test
    public void pingOne() {
        assertThat(Ping.pingOne(OkPing.class), nullValue());
    }

    @Test
    public void pingOneError() {
        assertThat(Ping.pingOne(ErrorPing.class), instanceOf(ErrorPingException.class));
    }

    @Test
    public void pingAll() {
        ClassFinder finder = mock(ClassFinder.class);

        when(finder.find(ObjectUtils.getCurrentClassLoader(), Ping.class)).thenReturn(ImmutableSet.of(OkPing.class, ErrorPing.class));

        ClassFinder.getThreadDefault().with(finder, () -> {
            Map<Class<?>, Throwable> results = Ping.pingAll();

            assertThat(results.size(), equalTo(2));
            assertThat(results, hasEntry(OkPing.class, null));
            assertThat(results, hasEntry(equalTo(ErrorPing.class), instanceOf(ErrorPingException.class)));
        });
    }

    private static class OkPing implements Ping {

        @Override
        public void ping() throws Throwable {
        }
    }

    private static class ErrorPing implements Ping {

        @Override
        public void ping() throws Throwable {
            throw new ErrorPingException();
        }
    }

    private static class ErrorPingException extends Exception {
    }
}
