package com.psddev.dari.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class Utf8FilterTest {

    private static final String UTF_8 = StandardCharsets.UTF_8.name();

    private Utf8Filter filter;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain chain;

    @Before
    public void before() {
        filter = new Utf8Filter();
        doReturn(Collections.emptyList()).when(request).getAttribute(AbstractFilter.class.getName() + ".dependencies." + Utf8Filter.class.getName());
    }

    @After
    public void after() {
        filter = null;
    }

    @Test
    public void request() throws Exception {
        filter.doFilter(request, response, chain);

        verify(request).setCharacterEncoding(UTF_8);
        verify(response).setCharacterEncoding(UTF_8);
        verify(chain).doFilter(request, response);
    }

    @Test
    public void requestNonUtf8() throws Exception {

        // Correct parameters in UTF-8 encoding.
        Map<String, String> correctParameters = new HashMap<>();

        correctParameters.put(Utf8Filter.CHECK_PARAMETER, Utf8Filter.CHECK_VALUE);
        correctParameters.put("다", "리");
        correctParameters.put("då", "rî");

        // Incorrect parameters in ISO-8859-1 encoding.
        Map<String, String[]> incorrectParameters = new HashMap<>();

        correctParameters.forEach((key, value) ->
                incorrectParameters.put(
                        toIso88591(key),
                        new String[] { toIso88591(value) }));

        // Mock parameter methods on request to return the incorrect
        // parameters.
        when(request.getParameter(any())).thenAnswer(invocation -> {
            String[] values = incorrectParameters.get(invocation.getArgumentAt(0, String.class));
            return values != null && values.length > 0 ? values[0] : null;
        });

        when(request.getParameterMap()).thenReturn(incorrectParameters);
        when(request.getParameterNames()).thenReturn(Collections.enumeration(incorrectParameters.keySet()));

        when(request.getParameterValues(any())).thenAnswer(invocation ->
                incorrectParameters.get(invocation.getArgumentAt(0, String.class)));

        // Run the filter and capture the requested with corrected parameters.
        filter.doFilter(request, response, chain);

        ArgumentCaptor<HttpServletRequest> correctedRequestCaptor = ArgumentCaptor.forClass(HttpServletRequest.class);

        verify(chain).doFilter(correctedRequestCaptor.capture(), eq(response));

        HttpServletRequest correctedRequest = correctedRequestCaptor.getValue();

        // Assert that all parameters were corrected.
        Map<String, String[]> correctedRequestParameterMap = correctedRequest.getParameterMap();

        correctParameters.forEach((key, value) -> {
            assertThat(correctedRequest.getParameter(key), is(value));

            assertThat(correctedRequestParameterMap, hasEntry(
                    is(key),
                    allOf(
                            arrayWithSize(1),
                            arrayContaining(value)
                    )
            ));

            assertThat(correctedRequest.getParameterValues(key), allOf(
                    arrayWithSize(1),
                    arrayContaining(value)
            ));
        });

        int correctedParameterNamesSize = 0;

        for (Enumeration<String> e = correctedRequest.getParameterNames(); e.hasMoreElements();) {
            assertThat(correctParameters, hasKey(e.nextElement()));
            ++ correctedParameterNamesSize;
        }
    }

    private String toIso88591(String value) {
        return new String(value.getBytes(StandardCharsets.UTF_8), StandardCharsets.ISO_8859_1);
    }
}
