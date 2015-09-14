package com.psddev.dari.db;

import java.io.IOException;
import java.util.Set;
import java.util.HashSet;
import java.util.UUID;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.Settings;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/** Enables various per-request database result caching. */
public class CachingDatabaseFilter extends AbstractFilter {

    public static final String CACHE_PARAMETER = "_cache";

    private static final Cache<String, Set<UUID>> ID_CACHE = CacheBuilder.newBuilder().maximumSize(250).build();

    // --- AbstractFilter support ---
    @Override
    protected void doForward(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {
        doRequest(request, response, chain);
    }

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        if (Settings.getOrDefault(boolean.class, "dari/isCachingFilterEnabled", true)
                && !Boolean.FALSE.toString().equals(request.getParameter(CACHE_PARAMETER))) {

            CachingDatabase caching = new CachingDatabase();

            caching.setDelegate(Database.Static.getDefault());

            Database.Static.overrideDefault(caching);

            try {
                String url = request.getServletPath() + "?" + request.getQueryString();
                boolean preload = Settings.getOrDefault(boolean.class, "dari/isCachingFilterPreloadEnabled", false);

                if (preload) {
                    Set<UUID> objectIds = ID_CACHE.getIfPresent(url);
                    if (objectIds != null) {
                        Query.from(Object.class).using(caching).where("id = ?", objectIds).selectAll();
                    }
                }

                chain.doFilter(request, response);

                if (preload) {
                    Set<UUID> objectIds = new HashSet<UUID>(caching.getIdOnlyQueryIds());

                    if (!objectIds.isEmpty()) {
                        ID_CACHE.put(url, objectIds);
                    }
                }

            } finally {
                Database.Static.restoreDefault();
            }

        } else if (Boolean.FALSE.toString().equals(request.getParameter(CACHE_PARAMETER))) {

            ForwardingDatabase db = new ForwardingDatabase() {
                @Override
                protected <T> Query<T> filterQuery(Query<T> query) {
                    return query.clone().option(Database.DISABLE_FUNNEL_CACHE_QUERY_OPTION, true).option(SqlDatabase.DISABLE_REPLICATION_CACHE_QUERY_OPTION, true);
                }
            };
            db.setDelegate(Database.Static.getDefault());
            Database.Static.overrideDefault(db);

            try {
                chain.doFilter(request, response);
            } finally {
                Database.Static.restoreDefault();
            }

        } else {
            chain.doFilter(request, response);
        }
    }
}
