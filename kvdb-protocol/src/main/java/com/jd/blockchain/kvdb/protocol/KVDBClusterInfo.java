package com.jd.blockchain.kvdb.protocol;

public class KVDBClusterInfo implements ClusterInfo {

    private ClusterItem[] clusterItems;

    public KVDBClusterInfo(ClusterItem[] clusterItems) {
        this.clusterItems = clusterItems;
    }

    @Override
    public int size() {
        return clusterItems.length;
    }

    @Override
    public ClusterItem[] getClusterItems() {
        return clusterItems;
    }

    public void setClusterItems(ClusterItem[] clusterItems) {
        this.clusterItems = clusterItems;
    }
}
