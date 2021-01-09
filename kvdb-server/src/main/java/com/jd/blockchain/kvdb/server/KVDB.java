package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.engine.Config;
import com.jd.blockchain.kvdb.engine.KVDBInstance;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBProxy;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.server.config.DBInfo;
import com.jd.blockchain.kvdb.server.config.KVDBConfig;
import com.jd.blockchain.kvdb.server.config.ServerConfig;

import utils.io.FileUtils;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KVDB {

    private static final String WAL_FILE = "kvdb.wal";

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Load default databases from dblist config.
     *
     * @param config
     * @return
     * @throws RocksDBException
     */
    public static Map<String, KVDBInstance> initDBs(ServerConfig config) throws KVDBException {
        Map<String, KVDBInstance> dbs = new HashMap<>();

        for (DBInfo dbInfo : config.getDbList().getEnabledDatabases()) {
            if (dbInfo.isEnable()) {
                String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
                FileUtils.makeDirectory(dbPath);
                Config logConfig = new Config(dbPath + File.separator + WAL_FILE, config.getKvdbConfig().isWalDisable(), config.getKvdbConfig().getWalFlush());
                try {
                    if (dbInfo.getPartitions() > 1) {
                        dbs.put(dbInfo.getName(), RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig));
                    } else {
                        dbs.put(dbInfo.getName(), RocksDBProxy.open(dbPath, logConfig));
                    }
                } catch (RocksDBException e) {
                    throw new KVDBException(e);
                }
            }
        }

        return dbs;
    }

    /**
     * Load default databases from dbInfo.
     *
     * @param dbInfo
     * @return
     * @throws RocksDBException
     */
    public static KVDBInstance initDB(DBInfo dbInfo, KVDBConfig config) throws KVDBException {
        String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
        FileUtils.makeDirectory(dbPath);
        Config logConfig = new Config(dbPath + File.separator + WAL_FILE, config.isWalDisable(), config.getWalFlush());
        try {
            if (dbInfo.getPartitions() > 1) {
                return RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig);
            } else {
                return RocksDBProxy.open(dbPath, logConfig);
            }
        } catch (RocksDBException e) {
            throw new KVDBException(e);
        }

    }

    /**
     * Create database
     *
     * @param dbInfo
     * @param config
     * @return
     * @throws RocksDBException
     */
    public static KVDBInstance createDB(DBInfo dbInfo, KVDBConfig config) throws KVDBException {
        KVDBInstance db;
        String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
        FileUtils.makeDirectory(dbPath);
        Config logConfig = new Config(dbPath + File.separator + WAL_FILE, config.isWalDisable(), config.getWalFlush());
        try {
            if (dbInfo.getPartitions() > 1) {
                db = RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig);
            } else {
                db = RocksDBProxy.open(dbPath, logConfig);
            }
        } catch (RocksDBException e) {
            throw new KVDBException(e);
        }

        return db;
    }
}
