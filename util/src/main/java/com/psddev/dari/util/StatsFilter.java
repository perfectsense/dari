package com.psddev.dari.util;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Filter that collects statistics around common servlet operations using
 * {@link Stats}.
 */
public class StatsFilter extends AbstractFilter {

    private static final Stats STATS_INCLUDES = new Stats("JSP Includes");
    private static final Stats STATS_RESPONSES = new Stats("HTTP Responses");

    @Override
    protected void doInclude(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Stats.Timer timer = STATS_INCLUDES.startTimer();

        try {
            chain.doFilter(request, response);

        } finally {
            timer.stop(JspUtils.getCurrentServletPath(request));
        }
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Stats.Timer timer = STATS_RESPONSES.startTimer();

        try {
            chain.doFilter(request, response);

        } finally {
            timer.stop(String.valueOf(response.getStatus()));
        }
    }
}
