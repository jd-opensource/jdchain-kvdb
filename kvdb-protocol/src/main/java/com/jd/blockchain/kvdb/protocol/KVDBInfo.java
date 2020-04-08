package com.jd.blockchain.kvdb.protocol;

/**
 * Server information
 */
public class KVDBInfo implements Info {

    // Whether is cluster mode
    private boolean clusterMode;
    // Cluster info
    private ClusterInfo[] cluster;

    @Override
    public boolean isClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }

    @Override
    public ClusterInfo[] getCluster() {
        return cluster;
    }

    public void setCluster(ClusterInfo[] cluster) {
        this.cluster = cluster;
    }

}
