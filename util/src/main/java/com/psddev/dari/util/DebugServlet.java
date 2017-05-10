package com.psddev.dari.util;

import javax.servlet.http.HttpServlet;
import java.util.List;

/**
 * HTTP servlet that's available through the debugging interface provided
 * by {@link DebugFilter}.
 */
public abstract class DebugServlet extends HttpServlet {

    /**
     * Returns the display name.
     *
     * @return If {@code null} or blank, the servlet will be accessible, but
     * it won't be displayed.
     */
    public abstract String getName();

    /**
     * Returns all paths that this servlet can serve.
     *
     * <p>First item is considered the main path and will be used as the link
     * in the debugging interface.</p>
     *
     * @return If {@code null} or empty, the servlet will not be accessible.
     */
    public abstract List<String> getPaths();
}
