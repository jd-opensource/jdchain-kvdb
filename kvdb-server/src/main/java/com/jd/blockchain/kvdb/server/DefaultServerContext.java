package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.Command;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.handler.Executor;
import com.jd.blockchain.kvdb.server.handler.UnknowExecutor;
import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DefaultServerContext implements ServerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServerContext.class);

    private final ConcurrentHashMap<String, Session> clients = new ConcurrentHashMap<>();

    private final Map<String, Executor> executors = new HashMap<>();

    private final ServerConfig config;

    private final KVDBInstance[] rocksdbs;

    public DefaultServerContext(String configFile) {
        config = new ServerConfig(configFile);
        rocksdbs = KVDB.getRocksDBs(config.getDbPath(), config.getDbSize(), config.getDbPartition());
    }

    public ServerConfig getConfig() {
        return config;
    }

    public String getHost() {
        return config.getHost();
    }

    public int getPort() {
        return config.getPort();
    }

    public int getClients() {
        return clients.size();
    }

    public Executor getExecutor(String command) {
        return executors.get(command);
    }

    @Override
    public int dbSize() {
        return rocksdbs.length;
    }

    @Override
    public KVDBInstance getDB(int index) {
        if (index < 0 || index >= rocksdbs.length) {
            index = 0;
            LOGGER.warn("index out of bounds, reset to zero, you can input values in [{}, {}]", 0, rocksdbs.length - 1);
        }
        return rocksdbs[index];
    }

    public void stop() {
        clients.clear();
        for (KVDBInstance db : rocksdbs) {
            db.close();
        }
    }

    Session getSession(String sourceKey, Function<String, Session> factory) {
        return clients.computeIfAbsent(sourceKey, key -> factory.apply(key));
    }

    public Session getSession(String key) {
        return clients.get(key);
    }

    protected void removeSession(String sourceKey) {
        Session session = getSession(sourceKey);
        if (null != session) {
            session.close();
        }
        clients.remove(sourceKey);
    }

    protected void addExecutor(String name, Executor executor) {
        executors.put(name.toLowerCase(), executor);
    }

    public void processCommand(String sourceKey, Message message) {
        Command command = (Command) message.getContent();
        Executor executor = executors.get(command.getName());
        if (null == executor) {
            executor = new UnknowExecutor();
        }
        Session session = getSession(sourceKey);
        session.publish(executor.execute(new DefaultRequest(this, session, message)));
    }
}
