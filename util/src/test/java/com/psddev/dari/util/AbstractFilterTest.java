package com.psddev.dari.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AbstractFilterTest {

    @Mock
    private FilterConfig config;

    @Mock
    private ServletContext servletContext;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private FilterChain chain;

    @Before
    public void before() {
        when(config.getServletContext()).thenReturn(servletContext);
    }

    private AbstractFilter initFilter(AbstractFilter f) throws ServletException {
        f.init(config);

        return f;
    }

    private AbstractFilter createNoOverridesFilter() {
        return new AbstractFilter() {
        };
    }

    @Test
    public void getFilterConfig() throws ServletException {
        AbstractFilter filter = initFilter(createNoOverridesFilter());

        assertThat(filter.getFilterConfig(), is(config));
    }

    @Test
    public void getServletContext() throws ServletException {
        AbstractFilter filter = initFilter(createNoOverridesFilter());

        assertThat(filter.getServletContext(), is(servletContext));
    }

    @Test(expected = NullPointerException.class)
    public void initNull() throws ServletException {
        AbstractFilter filter = createNoOverridesFilter();

        filter.init(null);
    }

    @Test(expected = ServletException.class)
    public void doInitServletException() throws Exception {
        initFilter(new AbstractFilter() {
            @Override
            protected void doInit() throws Exception {
                throw new ServletException();
            }
        });
    }

    @Test(expected = RuntimeException.class)
    public void doInitException() throws Exception {
        initFilter(new AbstractFilter() {
            @Override
            protected void doInit() throws Exception {
                throw new Exception();
            }
        });
    }

    @Test
    public void destroy() throws Exception {
        AbstractFilter filter = initFilter(createNoOverridesFilter());

        filter.destroy();

        assertThat(filter.getFilterConfig(), nullValue());
        assertThat(filter.getServletContext(), nullValue());
    }

    @Test(expected = RuntimeException.class)
    public void doDestroyException() throws Exception {
        AbstractFilter filter = initFilter(new AbstractFilter() {
            @Override
            protected void doDestroy() throws Exception {
                throw new Exception();
            }
        });

        filter.destroy();
    }

    private AbstractFilter doFilter(AbstractFilter f) throws IOException, ServletException {
        initFilter(f);
        f.doFilter(request, response, chain);

        return f;
    }

    private void verifyDoFilter(AbstractFilter f) throws IOException, ServletException {
        doFilter(f);

        verify(chain).doFilter(request, response);
    }

    private void verifyDoFilterOverride(AbstractFilter f) throws IOException, ServletException {
        doFilter(f);

        verify(chain, never()).doFilter(any(), any());
    }

    @Test
    public void doDispatch() throws IOException, ServletException {
        verifyDoFilter(createNoOverridesFilter());
    }

    @Test
    public void doDispatchOverride() throws IOException, ServletException {
        verifyDoFilterOverride(new AbstractFilter() {
            @Override
            protected void doDispatch(HttpServletRequest request, HttpServletResponse response, FilterChain chain) {
            }
        });
    }

    @Test
    public void doError() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI)).thenReturn("");

        verifyDoFilter(createNoOverridesFilter());
    }

    @Test
    public void doErrorOverride() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.ERROR_REQUEST_URI)).thenReturn("");

        verifyDoFilterOverride(new AbstractFilter() {
            @Override
            protected void doError(HttpServletRequest request, HttpServletResponse response, FilterChain chain) {
            }
        });
    }

    @Test
    public void doForward() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.FORWARD_CONTEXT_PATH)).thenReturn("");

        verifyDoFilter(createNoOverridesFilter());
    }

    @Test
    public void doForwardOverride() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.FORWARD_CONTEXT_PATH)).thenReturn("");

        verifyDoFilterOverride(new AbstractFilter() {
            @Override
            protected void doForward(HttpServletRequest request, HttpServletResponse response, FilterChain chain) {
            }
        });
    }

    @Test
    public void doInclude() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.INCLUDE_CONTEXT_PATH)).thenReturn("");

        verifyDoFilter(createNoOverridesFilter());
    }

    @Test
    public void doIncludeOverride() throws IOException, ServletException {
        when(request.getAttribute(RequestDispatcher.INCLUDE_CONTEXT_PATH)).thenReturn("");

        verifyDoFilterOverride(new AbstractFilter() {
            @Override
            protected void doInclude(HttpServletRequest request, HttpServletResponse response, FilterChain chain) {
            }
        });
    }

    @Test
    public void doRequest() throws IOException, ServletException {
        verifyDoFilter(createNoOverridesFilter());
    }

    @Test
    public void doRequestOverride() throws IOException, ServletException {
        verifyDoFilterOverride(new AbstractFilter() {
            @Override
            protected void doRequest(HttpServletRequest request, HttpServletResponse response, FilterChain chain) {
            }
        });
    }
}
