package com.jd.blockchain.kvdb.protocol.parameter;

public class KVDBCreateDatabaseParam implements CreateDatabaseParam {

    private String name;
    private String rootDir;
    private Integer partitions;

    public KVDBCreateDatabaseParam() {
    }

    public KVDBCreateDatabaseParam(String name, String rootDir, Integer partitions) {
        this.name = name;
        this.rootDir = rootDir;
        this.partitions = partitions;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getRootDir() {
        return rootDir;
    }

    @Override
    public void setRootDir(String rootDir) {
        this.rootDir = rootDir;
    }

    @Override
    public Integer getPartitions() {
        return partitions;
    }

    @Override
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }
}
