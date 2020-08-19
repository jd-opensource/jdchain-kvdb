package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.engine.KVDBInstance;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBProxy;
import com.jd.blockchain.kvdb.engine.wal.RedoLogConfig;
import com.jd.blockchain.kvdb.server.config.DBInfo;
import com.jd.blockchain.kvdb.server.config.KVDBConfig;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.utils.io.FileUtils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class KVDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDB.class);

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
    public static Map<String, KVDBInstance> initDBs(ServerConfig config) throws RocksDBException {
        Map<String, KVDBInstance> dbs = new HashMap<>();

        for (DBInfo dbInfo : config.getDbList().getEnabledDatabases()) {
            if (dbInfo.isEnable()) {
                String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
                FileUtils.makeDirectory(dbPath);
                RedoLogConfig logConfig = new RedoLogConfig(dbPath, config.getKvdbConfig().isWalDisable(), config.getKvdbConfig().getWalFlush());
                if (dbInfo.getPartitions() > 1) {
                    dbs.put(dbInfo.getName(), RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig));
                } else {
                    dbs.put(dbInfo.getName(), RocksDBProxy.open(dbPath, logConfig));
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
    public static KVDBInstance initDB(DBInfo dbInfo, KVDBConfig config) throws RocksDBException {
        Map<String, KVDBInstance> dbs = new HashMap<>();
        String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
        FileUtils.makeDirectory(dbPath);
        RedoLogConfig logConfig = new RedoLogConfig(dbPath, config.isWalDisable(), config.getWalFlush());
        if (dbInfo.getPartitions() > 1) {
            return RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig);
        } else {
            return dbs.put(dbInfo.getName(), RocksDBProxy.open(dbPath, logConfig));
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
    public static KVDBInstance createDB(DBInfo dbInfo, KVDBConfig config) throws RocksDBException {
        KVDBInstance db;
        String dbPath = dbInfo.getDbRootdir() + File.separator + dbInfo.getName();
        FileUtils.makeDirectory(dbPath);
        RedoLogConfig logConfig = new RedoLogConfig(dbPath, config.isWalDisable(), config.getWalFlush());
        if (dbInfo.getPartitions() > 1) {
            db = RocksDBCluster.open(dbPath, dbInfo.getPartitions(), logConfig);
        } else {
            db = RocksDBProxy.open(dbPath, logConfig);
        }

        return db;
    }
}
