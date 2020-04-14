package com.jd.blockchain.kvdb.protocol;

/**
 * Server information
 */
public class KVDBDatabaseInfo implements DatabaseInfo {

    // Whether is cluster mode
    private boolean clusterMode;
    // Cluster info
    private ClusterItem clusterItem;

    @Override
    public boolean isClusterMode() {
        return clusterMode;
    }

    public void setClusterMode(boolean clusterMode) {
        this.clusterMode = clusterMode;
    }

    @Override
    public ClusterItem getClusterItem() {
        return clusterItem;
    }

    public void setClusterItem(ClusterItem clusterItem) {
        this.clusterItem = clusterItem;
    }

}
