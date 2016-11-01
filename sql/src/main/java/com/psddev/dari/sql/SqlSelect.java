package com.psddev.dari.sql;

import com.psddev.dari.db.Query;
import org.jooq.RenderContext;
import org.jooq.SelectLimitStep;

class SqlSelect {

    private final Query<?> query;
    private final RenderContext renderContext;
    private final SelectLimitStep<?> selectLimitStep;

    public SqlSelect(Query<?> query, RenderContext renderContext, SelectLimitStep<?> selectLimitStep) {
        this.query = query;
        this.renderContext = renderContext;
        this.selectLimitStep = selectLimitStep;
    }

    public String statement() {
        return SqlQuery.addComment(query, renderContext.render(selectLimitStep));
    }

    public String statement(int offset, int limit) {
        return SqlQuery.addComment(query, renderContext.render(selectLimitStep.offset(offset).limit(limit)));
    }
}
