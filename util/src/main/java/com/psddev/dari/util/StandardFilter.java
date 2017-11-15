package com.psddev.dari.util;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.Filter;

/**
 * Runs all standard filters.
 *
 * <p>This filter loads:</p>
 *
 * <ul>
 * <li>{@link Utf8Filter}</li>
 * <li>{@link SourceFilter}</li>
 * <li>{@link LogCaptureFilter}</li>
 * <li>{@link DebugFilter}</li>
 * <li>{@link ProfilerFilter}</li>
 * <li>{@link StatsFilter}</li>
 * <li>{@link HeaderResponseFilter}</li>
 * <li>{@link MultipartRequestFilter}</li>
 * <li>{@link PageContextFilter}</li>
 * <li>{@link NoSessionFilter}</li>
 * <li>{@link JspBufferFilter}</li>
 * <li>{@link HtmlApiFilter}</li>
 * <li>{@link PingFilter}</li>
 * <li>{@link ResourceFilter}</li>
 * <li>{@link TaskFilter}</li>
 * </ul>
 */
public class StandardFilter extends AbstractFilter {

    @Override
    protected Iterable<Class<? extends Filter>> dependencies() {
        List<Class<? extends Filter>> dependencies = new ArrayList<Class<? extends Filter>>();

        dependencies.add(Utf8Filter.class);
        dependencies.add(SourceFilter.class);
        dependencies.add(HeaderResponseFilter.class);
        dependencies.add(AbsoluteLocationFilter.class);
        dependencies.add(LogCaptureFilter.class);
        dependencies.add(DebugFilter.class);

        dependencies.add(ProfilerFilter.class);
        dependencies.add(StatsFilter.class);

        dependencies.add(MultipartRequestFilter.class);
        dependencies.add(PageContextFilter.class);
        dependencies.add(NoSessionFilter.class);
        dependencies.add(JspBufferFilter.class);
        dependencies.add(HtmlApiFilter.class);

        dependencies.add(PingFilter.class);
        dependencies.add(ResourceFilter.class);
        dependencies.add(TaskFilter.class);

        return dependencies;
    }
}
