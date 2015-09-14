package com.psddev.dari.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.psddev.dari.util.AbstractFilter;
import com.psddev.dari.util.CompactMap;
import com.psddev.dari.util.IoUtils;
import com.psddev.dari.util.JspUtils;
import com.psddev.dari.util.Lazy;
import com.psddev.dari.util.Once;
import com.psddev.dari.util.PeriodicCache;

public class WebResourceOverrideFilter extends AbstractFilter {

    private static final String OLD_FILE_SUFFIX = ".old";
    private static final String NEW_FILE_SUFFIX = ".new";
    private static final String DEL_FILE_SUFFIX = ".del";
    private static final String TMP_FILE_SUFFIX = ".tmp";

    private final LoadingCache<String, Restorer> restorers = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, Restorer>() {

                @Override
                public Restorer load(String path) {
                    return new Restorer(path);
                }
            });

    private final Lazy<PeriodicCache<String, Copier>> copiers = new Lazy<PeriodicCache<String, Copier>>() {

        @Override
        protected PeriodicCache<String, Copier> create() {
            return new PeriodicCache<String, Copier>() {

                @Override
                protected Map<String, Copier> update() {
                    Database.Static.overrideDefault(Database.Static.getDefaultOriginal());

                    try {
                        Date cacheUpdate = getUpdateDate();

                        if (cacheUpdate == null
                                || UpdateTrackable.Static.isUpdated(WebResourceOverride.UPDATE_TRACKING_NAME, cacheUpdate.getTime())) {
                            Map<String, Copier> copiers = new CompactMap<String, Copier>();

                            for (WebResourceOverride override : Query
                                    .from(WebResourceOverride.class)
                                    .selectAll()) {
                                String path = override.getPath();

                                restorers.invalidate(path);
                                copiers.put(path, new Copier(override));
                            }

                            return copiers;

                        } else {
                            restorers.invalidateAll();
                            return null;
                        }

                    } finally {
                        Database.Static.restoreDefault();
                    }
                }
            };
        }
    };

    @Override
    protected void doDispatch(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        String path = JspUtils.getCurrentServletPath(request);
        Copier copier = copiers.get().get(path);

        if (copier == null) {
            restorers.getUnchecked(path).ensure();

        } else {
            copier.ensure();

            WebResourceOverride override = copier.getOverride();
            Integer statusCode = override.getStatusCode();

            if (statusCode != null) {
                response.setStatus(statusCode);
            }

            for (WebResourceOverride.Header header : override.getHeaders()) {
                response.addHeader(header.getName(), header.getValue());
            }
        }

        chain.doFilter(request, response);
    }

    private class Restorer extends Once {

        private final String path;

        public Restorer(String path) {
            this.path = path;
        }

        @Override
        protected void run() throws IOException {
            String realPath = getServletContext().getRealPath(path);

            if (realPath != null) {
                File curFile = new File(realPath);
                File oldFile = new File(realPath + OLD_FILE_SUFFIX);

                if (oldFile.exists()) {
                    File tmpFile = new File(realPath + TMP_FILE_SUFFIX);

                    IoUtils.delete(tmpFile);
                    IoUtils.rename(curFile, tmpFile);
                    IoUtils.rename(oldFile, curFile);
                    IoUtils.delete(tmpFile);

                } else {
                    File delFile = new File(realPath + DEL_FILE_SUFFIX);

                    if (delFile.exists()) {
                        IoUtils.delete(curFile);
                        IoUtils.delete(delFile);
                    }
                }
            }
        }
    }

    private class Copier extends Once {

        private final WebResourceOverride override;

        public Copier(WebResourceOverride override) {
            this.override = override;
        }

        public WebResourceOverride getOverride() {
            return override;
        }

        @Override
        protected void run() throws IOException {
            String realPath = getServletContext().getRealPath(override.getPath());

            if (realPath != null) {
                File curFile = new File(realPath);
                File newFile = new File(realPath + NEW_FILE_SUFFIX);
                Writer writer = new OutputStreamWriter(new FileOutputStream(newFile), StandardCharsets.UTF_8);

                try {
                    String content = override.getContent();

                    if (content != null) {
                        writer.write(content);
                    }

                } finally {
                    writer.close();
                }

                if (curFile.exists()) {
                    File oldFile = new File(realPath + OLD_FILE_SUFFIX);

                    if (!oldFile.exists()
                            && !new File(realPath + DEL_FILE_SUFFIX).exists()) {
                        IoUtils.rename(curFile, oldFile);
                    }

                } else {
                    IoUtils.createFile(new File(realPath + DEL_FILE_SUFFIX));
                }

                IoUtils.rename(newFile, curFile);
            }
        }
    }
}
