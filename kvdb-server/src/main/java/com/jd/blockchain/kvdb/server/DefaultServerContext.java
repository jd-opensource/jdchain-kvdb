package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.kvdb.server.executor.Executor;
import com.jd.blockchain.utils.StringUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DefaultServerContext implements ServerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultServerContext.class);

    private final ConcurrentHashMap<String, Session> clients = new ConcurrentHashMap<>();

    private final Map<String, Executor> executors = new HashMap<>();

    private final ServerConfig config;

    // Hold all the databases
    private final Map<String, KVDBInstance> rocksdbs;
    private Map<String, ClusterItem> clusterInfoMapping;
    private Map<String, String> dbClusterMapping;


    public DefaultServerContext(ServerConfig config) throws RocksDBException, IOException {
        this.config = config;
        rocksdbs = KVDB.initDBs(config.getDbList());
        clusterInfoMapping = config.getClusterMapping();
        dbClusterMapping = new HashMap<>();
        for (Map.Entry<String, ClusterItem> entry : clusterInfoMapping.entrySet()) {
            for (String url : entry.getValue().getURLs()) {
                KVDBURI uri = new KVDBURI(url);
                dbClusterMapping.put(uri.getDatabase(), entry.getKey());
            }
        }
    }

    public ServerConfig getConfig() {
        return config;
    }

    public int getClients() {
        return clients.size();
    }

    public Executor getExecutor(String command) {
        Executor executor = executors.get(command.toLowerCase());

        return null != executor ? executor : executors.get(Command.CommandType.UNKNOWN.getCommand().toLowerCase());
    }

    @Override
    public Map<String, KVDBInstance> getDatabases() {
        return rocksdbs;
    }

    @Override
    public KVDBInstance getDatabase(String name) {
        return rocksdbs.get(name);
    }

    public synchronized KVDBInstance createDatabase(String dbName) throws KVDBException, RocksDBException, IOException {
        if (rocksdbs.containsKey(dbName)) {
            throw new KVDBException("database exists");
        }

        KVDBInstance kvdbInstance = KVDB.createDB(config.getKvdbConfig(), dbName);
        config.getKvdbConfig().createDatabase(dbName);
        rocksdbs.put(dbName, kvdbInstance);

        return kvdbInstance;
    }

    @Override
    public DatabaseInfo getDatabaseInfo(String database) {
        KVDBDatabaseInfo info = new KVDBDatabaseInfo();
        String cluster = dbClusterMapping.get(database);
        if (StringUtils.isEmpty(cluster)) {
            info.setClusterMode(false);
        } else {
            info.setClusterMode(true);
            info.setClusterItem(clusterInfoMapping.get(cluster));
        }
        return info;
    }

    @Override
    public ClusterInfo getClusterInfo() {
        return new KVDBClusterInfo(clusterInfoMapping.values().toArray(new ClusterItem[clusterInfoMapping.size()]));
    }

    public void stop() {
        clients.clear();
        for (KVDBInstance db : rocksdbs.values()) {
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
        Session session = getSession(sourceKey);
        session.publish(executors.get(command.getName()).execute(new DefaultRequest(this, session, message)));
    }
}
