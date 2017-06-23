package com.psddev.dari.test;

import com.psddev.dari.db.Record;

import java.util.Date;
import java.util.UUID;

public class SearchObjectModel extends Record {

    @Indexed
    public UUID token;
    public UUID getToken() {
        return token;
    }
    public void setToken(UUID token) {
        this.token = token;
    }

    @Indexed
    public Date expireTimestamp;
    public Date getExpireTimestamp() {
        return expireTimestamp;
    }
    public void setExpireTimestamp(Date expireTimestamp) {
        this.expireTimestamp = expireTimestamp;
    }

}

