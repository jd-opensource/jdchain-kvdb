package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.kvdb.server.executor.Executor;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.Map;

public interface ServerContext {

    ServerConfig getConfig();

    int getClients();

    Session getSession(String sourceKey);

    Executor getExecutor(String command);

    Map<String, KVDBInstance> getDBs();

    KVDBInstance getDB(String name);

    KVDBInstance createDB(String name) throws KVDBException, RocksDBException, IOException;
}
