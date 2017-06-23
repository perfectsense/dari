package com.psddev.dari.test;

import com.psddev.dari.db.Modification;

import java.util.Date;

@Modification.Classes({SearchIndexModel.class})
public final class TestModification extends Modification<Object> {

    private static final String PREFIX = "cms.content.";
    public static final String UPDATE_DATE_FIELD = PREFIX + "updateDate";

    @Indexed
    @InternalName(UPDATE_DATE_FIELD)
    private Date updateDate;

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }
}

