package com.psddev.dari.elasticsearch;

import com.google.common.base.Preconditions;
import com.psddev.dari.db.*;
import com.psddev.dari.util.ObjectUtils;
import com.psddev.dari.util.PaginatedResult;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.search.sort.SortOrder.DESC;

//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.node.NodeBuilder;


class ElasticsearchDatabaseConnection {
    private static TransportClient client = null;
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabase.class);

    public static boolean isAlive(TransportClient client) {
        List<DiscoveryNode> nodes = client.connectedNodes();
        if (nodes.isEmpty()) {
            return false;
        } else {
            return true;
        }
    }

    public synchronized static TransportClient getClient(Settings nodeSettings, String clusterHostname, int clusterPort){
        if (nodeSettings == null) {
            LOGGER.warn("ELK openConnection No nodeSettings");
            nodeSettings = Settings.builder()
                    .put("client.transport.sniff", true).build();
        }
        if (client==null) {
            try {
                client = new PreBuiltTransportClient(nodeSettings)
                        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(clusterHostname), clusterPort));
                if (!isAlive(client)) {
                    LOGGER.warn("ELK openConnection Not Alive!");
                    return null;
                }
                return client;
            } catch (Exception error) {
                LOGGER.info(
                        String.format("ELK openConnection Cannot open ES Exception [%s: %s]",
                                error.getClass().getName(),
                                error.getMessage()),
                        error);
            }
            return null;
        } else {
            try {
                if (!isAlive(client)) {
                    LOGGER.warn("ELK openConnection Not Alive!");
                    client = new PreBuiltTransportClient(nodeSettings)
                            .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(clusterHostname), clusterPort));
                }
            } catch (Exception error) {
                LOGGER.info(
                        String.format("ELK openConnection Cannot open ES Exception [%s: %s]",
                                error.getClass().getName(),
                                error.getMessage()),
                        error);
            }
            return client;
        }
    }
}
