package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.handler.Executor;
import com.jd.blockchain.kvdb.KVDBInstance;

public interface ServerContext {

    String getHost();

    int getPort();

    ServerConfig getConfig();

    int getClients();

    Session getSession(String sourceKey);

    Executor getExecutor(String command);

    int dbSize();

    KVDBInstance getDB(int index);
}
