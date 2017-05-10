package com.psddev.dari.util;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

class DebugSettingsOverrideFilter extends AbstractFilter {

    @Override
    protected void doRequest(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain chain)
            throws IOException, ServletException {

        try {
            Boolean production = ObjectUtils.to(Boolean.class, request.getParameter(DebugFilter.PRODUCTION_PARAMETER));

            if (production != null && !DebugFilter.authenticate(request, response)) {
                return;
            }

            Settings.setOverride(Settings.PRODUCTION_SETTING, production);

            if (ObjectUtils.to(boolean.class, request.getParameter(DebugFilter.DEBUG_PARAMETER))) {
                if (!DebugFilter.authenticate(request, response)) {
                    return;
                }

                Settings.setOverride(Settings.DEBUG_SETTING, Boolean.TRUE);

                if (production == null) {
                    Settings.setOverride(Settings.PRODUCTION_SETTING, Boolean.FALSE);
                }
            }

            chain.doFilter(request, response);

        } finally {
            Settings.setOverride(Settings.PRODUCTION_SETTING, null);
            Settings.setOverride(Settings.DEBUG_SETTING, null);
        }
    }
}
