package com.psddev.dari.util;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(Enclosed.class)
public class JspUtilsTest {

    @RunWith(MockitoJUnitRunner.class)
    public static class GetBasicCredentialsTest {

        @Mock
        private HttpServletRequest request;

        @Test(expected = NullPointerException.class)
        public void nullRequest() {
            JspUtils.getBasicCredentials(null);
        }

        @Test
        public void noBasicPrefix() {
            when(request.getHeader("Authorization")).thenReturn("1");

            assertThat(JspUtils.getBasicCredentials(request), equalTo(null));
        }

        @Test
        public void notBase64() {
            when(request.getHeader("Authorization")).thenReturn("Basic ~");

            assertThat(JspUtils.getBasicCredentials(request), equalTo(null));
        }

        private String encodeBase64(String string, Charset charset) {
            return new String(
                    Base64.getEncoder().encode(string.getBytes(charset)),
                    charset);
        }

        @Test
        public void noColonSeparator() {
            when(request.getHeader("Authorization")).thenReturn("Basic " + encodeBase64("1", StandardCharsets.UTF_8));

            assertThat(JspUtils.getBasicCredentials(request), equalTo(null));
        }

        @Test
        public void test() {
            String username = "1";
            String password = "1";

            when(request.getHeader("Authorization")).thenReturn("Basic " + encodeBase64(username + ":" + password, StandardCharsets.UTF_8));

            assertThat(JspUtils.getBasicCredentials(request), allOf(
                    arrayWithSize(2),
                    arrayContaining(username, password)
            ));
        }

        @Test
        public void iso88591Encoding() {
            String username = "1";
            String password = "1";

            when(request.getHeader("Authorization")).thenReturn("Basic " + encodeBase64(username + ":" + password, StandardCharsets.ISO_8859_1));
            when(request.getCharacterEncoding()).thenReturn(StandardCharsets.ISO_8859_1.name());

            assertThat(JspUtils.getBasicCredentials(request), allOf(
                    arrayWithSize(2),
                    arrayContaining(username, password)
            ));
        }
    }

    @RunWith(MockitoJUnitRunner.class)
    public static class ForwardTest {

        @Mock
        private ServletRequest request;

        @Mock
        private ServletResponse response;

        @Test(expected = NullPointerException.class)
        public void nullRequest() throws IOException, ServletException {
            JspUtils.forward(null, response, "");
        }

        @Test(expected = NullPointerException.class)
        public void nullResponse() throws IOException, ServletException {
            JspUtils.forward(request, null, "");
        }

        @Test(expected = NullPointerException.class)
        public void nullPath() throws IOException, ServletException {
            JspUtils.forward(request, response, null);
        }
    }

    @RunWith(MockitoJUnitRunner.class)
    public static class SetAttributesTest {

        @Mock
        private ServletRequest request;

        @Test(expected = NullPointerException.class)
        public void nullRequest() {
            JspUtils.setAttributes(null);
        }

        @Test
        public void test() {
            when(request.getAttribute("1")).thenReturn("1");

            Map<String, Object> oldAttributes = JspUtils.setAttributes(request, "1", "change", "3");

            verify(request).setAttribute("1", "change");
            verify(request).setAttribute("3", null);

            assertThat(oldAttributes.size(), is(2));
            assertThat(oldAttributes, allOf(
                    hasEntry("1", "1"),
                    hasEntry("3", null)
            ));
        }
    }

    @RunWith(MockitoJUnitRunner.class)
    public static class SetAttributesWithMapTest {

        @Mock
        private HttpServletRequest request;

        @Test(expected = NullPointerException.class)
        public void nullRequest() {
            JspUtils.setAttributesWithMap(null, null);
        }

        @Test
        public void test() {
            when(request.getAttribute("1")).thenReturn("1");

            Map<String, Object> oldAttributes = JspUtils.setAttributesWithMap(request, ImmutableMap.of("1", "change"));

            verify(request).setAttribute("1", "change");

            assertThat(oldAttributes.size(), is(1));
            assertThat(oldAttributes, hasEntry("1", "1"));
        }
    }
}
