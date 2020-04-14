package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.DatabaseInfo;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.kvdb.server.executor.Executor;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;

public interface ServerContext {

    ServerConfig getConfig();

    Session getSession(String sourceKey);

    Executor getExecutor(String command);

    Map<String, KVDBInstance> getDatabases();

    KVDBInstance getDatabase(String name);

    KVDBInstance createDatabase(String name) throws KVDBException, RocksDBException, IOException;

    DatabaseInfo getDatabaseInfo(String database);

    ClusterInfo getClusterInfo();
}
