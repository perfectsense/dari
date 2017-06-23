package com.psddev.dari.elasticsearch;

import com.google.common.base.MoreObjects;

import java.util.List;

class CreateIndexKey {
    private String indexName;
    private String indexId;
    private int shardsMax;
    private List<ElasticsearchNode> clusterNodes;
    private org.elasticsearch.common.settings.Settings nodeSettings;

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public int getShardsMax() {
        return shardsMax;
    }

    public void setShardsMax(int shardsMax) {
        this.shardsMax = shardsMax;
    }

    public String getIndexId() {
        return indexId;
    }

    public void setIndexId(String indexId) {
        this.indexId = indexId;
    }

    public List<ElasticsearchNode> getClusterNodes() {
        return clusterNodes;
    }

    public void setClusterNodes(List<ElasticsearchNode> clusterNodes) {
        this.clusterNodes = clusterNodes;
    }

    public org.elasticsearch.common.settings.Settings getNodeSettings() {
        return nodeSettings;
    }

    public void setNodeSettings(org.elasticsearch.common.settings.Settings nodeSettings) {
        this.nodeSettings = nodeSettings;
    }

    @Override
    public String toString() {

        return MoreObjects.toStringHelper(this)
                .add("indexName", indexName)
                .toString();

    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CreateIndexKey indexKey = (CreateIndexKey) o;

        return indexName.equals(indexKey.indexName);
    }

    @Override
    public int hashCode() {
        return indexName.hashCode();
    }
}
