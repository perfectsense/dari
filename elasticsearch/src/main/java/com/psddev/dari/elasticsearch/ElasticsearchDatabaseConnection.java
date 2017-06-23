package com.psddev.dari.elasticsearch;

import com.google.common.base.Charsets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class ElasticsearchDatabaseConnection {
    private static final Map<String, TransportClient> CLIENT_CONNECTIONS = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchDatabaseConnection.class);

    /**
     * Check nodes are not empty and isAlive
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public static boolean isAlive(TransportClient client) {
        List<DiscoveryNode> nodes = client.connectedNodes();
        return !nodes.isEmpty();
    }

    /**
     * Human readable and used to hash a string
     */
    private static String getHashString(Settings nodeSettings, List<ElasticsearchNode> nodes) {
        StringBuilder hash = new StringBuilder();
        for (ElasticsearchNode n : nodes) {
            hash.append(n.getHostname()).append(" ").append(n.getPort()).append(" ").append(n.getRestPort()).append(" ");
        }
        hash.append(nodeSettings.get("cluster.name"));
        return hash.toString();
    }

    /**
     * Generate hash that is only on nodes and cluster
     */
    private static String getHash(Settings nodeSettings, List<ElasticsearchNode> nodes) {
        String hash = getHashString(nodeSettings, nodes);

        HashFunction hf = Hashing.md5();
        HashCode hc = hf.newHasher()
                .putString(hash, Charsets.UTF_8)
                .hash();

        return hc.toString().toUpperCase();
    }

    /**
     * Force a close
     */
    public static synchronized void closeClients() {
        Iterator<String> it = CLIENT_CONNECTIONS.keySet().iterator();

        while (it.hasNext()) {
            String key = it.next();
            TransportClient c = CLIENT_CONNECTIONS.get(key);
            if (c != null) {
                c.close();
                CLIENT_CONNECTIONS.remove(key);
            }
        }
    }

    /**
     * getClient synchronized and calls PreBuiltTransportClient
     *
     * @return {code null} if error
     */
    public static synchronized TransportClient getClient(Settings nodeSettings, List<ElasticsearchNode> nodes) {
        if (nodeSettings == null || nodes.size() == 0) {
            LOGGER.warn("Elasticsearch openConnection missing nodeSettings/nodes");
            nodeSettings = Settings.builder()
                    .put("client.transport.sniff", false)
                    .put("transport.tcp.compress", false)
                    .put("transport.tcp_no_delay", true)
                    .build();
        }
        TransportClient c = CLIENT_CONNECTIONS.get(getHash(nodeSettings, nodes));
        if (c == null || !isAlive(c)) {
            try {
                c = new PreBuiltTransportClient(nodeSettings);
                for (ElasticsearchNode n : nodes) {
                    c.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(n.getHostname()), n.getPort()));
                }
                CLIENT_CONNECTIONS.put(getHash(nodeSettings, nodes), c);
                LOGGER.warn("Creating connection {}", getHashString(nodeSettings, nodes));
                return c;
            } catch (Exception error) {
                LOGGER.warn(
                        String.format("Elasticsearch getClient Cannot open ES Exception [%s: %s]",
                                error.getClass().getName(),
                                error.getMessage()),
                        error);
            }
            return null;
        } else {
            return c;
        }
    }
}
