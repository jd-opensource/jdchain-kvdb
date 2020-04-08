package com.jd.blockchain.kvdb.protocol;

/**
 * Cluster information
 */
public class KVDBClusterInfo implements ClusterInfo {

    // cluster name
    private String name;
    // Cluster URLs
    private String[] urls;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String[] getURLs() {
        return urls;
    }

    public void setURLs(String[] urls) {
        this.urls = urls;
    }
}
