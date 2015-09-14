package com.psddev.dari.maven;

import java.io.IOException;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.maven.plugin.logging.Log;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import com.psddev.dari.db.AggregateDatabase;
import com.psddev.dari.db.Database;
import com.psddev.dari.db.SolrDatabase;
import com.psddev.dari.db.SqlDatabase;
import com.psddev.dari.util.SparseSet;
import com.psddev.dari.util.sa.JvmAnalyzer;

public class AnalyzeAllThread extends Thread {

    private final AnalyzeAllLogger logger;

    public AnalyzeAllThread(Log log) {
        this.logger = new AnalyzeAllLogger(log);
    }

    public AnalyzeAllLogger getLogger() {
        return logger;
    }

    @Override
    public void run() {
        SqlDatabase sql = new SqlDatabase();
        HikariDataSource ds = new HikariDataSource();

        ds.setJdbcUrl("jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        ds.setUsername("");
        ds.setPassword("");
        ds.setConnectionTimeout(5000L);

        sql.setName("sql");
        sql.setDataSource(ds);

        SolrDatabase solr = new SolrDatabase();

        System.setProperty("solr.solr.home", "/Users/hyoolim/solrtest");

        CoreContainer coreContainer = new CoreContainer();
        EmbeddedSolrServer solrServer = new EmbeddedSolrServer(coreContainer, "");

        solr.setName("solr");
        solr.setServer(solrServer);

        AggregateDatabase aggregate = new AggregateDatabase();

        aggregate.setName("aggregate");
        aggregate.addDelegate(sql, new SparseSet("+/"));
        aggregate.addDelegate(solr, new SparseSet("-* +cms.content.searchable"));
        aggregate.setDefaultDelegate(sql);

        try {
            Database.Static.overrideDefault(aggregate);
            JvmAnalyzer.Static.analyzeAll(logger);

        } catch (IOException error) {
            throw new RuntimeException(error);

        } finally {
            Database.Static.restoreDefault();
        }
    }
}
