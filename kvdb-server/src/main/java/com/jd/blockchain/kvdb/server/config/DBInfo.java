package com.jd.blockchain.kvdb.server.config;

public class DBInfo {
    private String name;
    private boolean enable;
    private String dbRootdir;
    private int partitions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isEnable() {
        return enable;
    }

    public void setEnable(boolean enable) {
        this.enable = enable;
    }

    public String getDbRootdir() {
        return dbRootdir;
    }

    public void setDbRootdir(String dbRootdir) {
        this.dbRootdir = dbRootdir;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
}
