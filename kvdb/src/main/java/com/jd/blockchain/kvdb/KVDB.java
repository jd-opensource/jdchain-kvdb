package com.jd.blockchain.kvdb;

import com.jd.blockchain.kvdb.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.rocksdb.RocksDBProxy;
import com.jd.blockchain.utils.io.FileUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class KVDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDB.class);

    static {
        RocksDB.loadLibrary();
    }

    public static KVDBInstance[] getRocksDBs(String dbPath, int dbSize, int partitions) {
        try {
            String rootPath = FileUtils.getFullPath(dbPath);
            KVDBInstance[] dbs = new KVDBInstance[dbSize];
            for (int i = 0; i < dbSize; i++) {
                String partitionPath = rootPath + File.separator + i;
                FileUtils.makeDirectory(dbPath);
                if (partitions > 1) {
                    dbs[i] = RocksDBCluster.open(partitionPath, partitions);
                } else {
                    dbs[i] = RocksDBProxy.open(partitionPath);
                }
            }
            return dbs;
        } catch (RocksDBException e) {
            LOGGER.error("rocksdb init error", e);
        }
        return null;
    }

}
