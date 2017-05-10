package com.psddev.dari.db;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.psddev.dari.util.AbstractFilter;

/** Resets all components so that they're in a valid state. */
public class ResetFilter extends AbstractFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationFilter.class);

    // --- AbstractFilter support ---

    @Override
    protected void doDestroy() {
        Database.Static.getAll().forEach(database -> {
            try {
                database.close();

            } catch (Exception error) {
                LOGGER.info(
                        String.format("Can't close [%s] database!", database),
                        error);
            }
        });

        SqlDatabase.closeAll();
        SqlDatabase.Static.deregisterAllDrivers();

        // Clean up after incorrect use of Apache HttpClient 3.x.
        try {
            LOGGER.info("Shutting down [{}]", MultiThreadedHttpConnectionManager.class);
            MultiThreadedHttpConnectionManager.shutdownAll();
        } catch (NoClassDefFoundError error) {
            // Not using the library at all.
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        Database.resetThreadLocals();
        chain.doFilter(request, response);
    }
}
