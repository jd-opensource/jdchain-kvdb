package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVWriteBatch;
import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.ClusterItem;
import com.jd.blockchain.kvdb.protocol.proto.Command;
import com.jd.blockchain.kvdb.protocol.proto.DatabaseClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBDatabaseClusterInfo;
import com.jd.blockchain.kvdb.server.config.DBInfo;
import com.jd.blockchain.kvdb.server.config.DBList;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.kvdb.server.executor.Executor;
import com.jd.blockchain.kvdb.server.wal.Entity;
import com.jd.blockchain.kvdb.server.wal.KV;
import com.jd.blockchain.kvdb.server.wal.Meta;
import com.jd.blockchain.kvdb.server.wal.RedoLog;
import com.jd.blockchain.kvdb.server.wal.Wal;
import com.jd.blockchain.kvdb.server.wal.WalCommand;
import com.jd.blockchain.kvdb.server.wal.WalEntity;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 服务端上下文信息
 */
public class KVDBServerContext implements ServerContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBServerContext.class);

    // 当前服务器所有客户端连接
    private final ConcurrentHashMap<String, Session> clients = new ConcurrentHashMap<>();

    // 所有命令操作集合，命令名-操作对象
    private final Map<String, Executor> executors = new HashMap<>();

    // 服务器配置信息
    private final ServerConfig config;

    // 数据库实例集，数据库名-实例
    private final Map<String, KVDBInstance> rocksdbs;
    // 集群配置集，集群名称-配置
    private Map<String, ClusterItem> clusterInfoMapping;
    // 数据库实例-集群配置对照关系，数据库名-集群名
    private Map<String, String> dbClusterMapping;

    private Wal<Entity, Meta> wal;


    public KVDBServerContext(ServerConfig config) throws RocksDBException, IOException {
        this.config = config;
        // 创建或加载 dblist 中配置的数据库实例
        rocksdbs = KVDB.initDBs(config.getDbList());
        // 保存集群配置
        clusterInfoMapping = config.getClusterMapping();
        // 保存数据库实例-集群对照关系
        dbClusterMapping = new HashMap<>();
        for (Map.Entry<String, ClusterItem> entry : clusterInfoMapping.entrySet()) {
            for (String url : entry.getValue().getURLs()) {
                KVDBURI uri = new KVDBURI(url);
                dbClusterMapping.put(uri.getDatabase(), entry.getKey());
            }
        }
        wal = new RedoLog(config.getKvdbConfig());
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

    @Override
    public synchronized DatabaseClusterInfo setDB(Session session, String dbName) {
        KVDBInstance kvdbInstance = rocksdbs.get(dbName);
        if (null != kvdbInstance) {
            session.setDB(dbName, kvdbInstance);

            return getDatabaseInfo(dbName);
        }

        throw new KVDBException("database not exists");
    }

    public synchronized KVDBInstance createDatabase(DBInfo dbInfo) throws KVDBException, RocksDBException, IOException {
        if (rocksdbs.containsKey(dbInfo.getName())) {
            throw new KVDBException("database already exists");
        }
        long lsn = wal.append(WalEntity.newCreateDatabaseEntity(dbInfo));
        KVDBInstance kvdbInstance = KVDB.createDB(config.getKvdbConfig(), dbInfo);
        config.getDbList().createDatabase(dbInfo);
        rocksdbs.put(dbInfo.getName(), kvdbInstance);
        wal.updateMeta(lsn);
        return kvdbInstance;
    }

    @Override
    public DatabaseClusterInfo getDatabaseInfo(String database) {
        KVDBDatabaseClusterInfo info = new KVDBDatabaseClusterInfo();
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
        return config.getClusterInfo();
    }

    @Override
    public synchronized void enableDatabase(String database) throws KVDBException {
        try {
            // 数据库处于可用状态
            if (rocksdbs.containsKey(database)) {
                return;
            }
            DBInfo dbInfo = config.getDbList().getDatabase(database);
            // 数据库不存在
            if (null == dbInfo) {
                throw new KVDBException("database not exists");
            }
            long lsn = wal.append(WalEntity.newEnableDatabaseEntity(database));
            config.getDbList().enableDatabase(database);
            rocksdbs.put(database, KVDB.initDB(dbInfo));
            wal.updateMeta(lsn);
        } catch (Exception e) {
            throw new KVDBException(e.toString());
        }
    }

    @Override
    public synchronized void disableDatabase(String database) throws KVDBException {
        try {
            // 数据库不处于可用状态
            KVDBInstance instance = rocksdbs.get(database);
            if (null == instance) {
                throw new KVDBException("database not exists or disabled");
            }
            // 参与到集群中的数据库实例不可修改
            if (dbClusterMapping.containsKey(database)) {
                throw new KVDBException("database in cluster can not modified");
            }
            long lsn = wal.append(WalEntity.newDisableDatabaseEntity(database));
            rocksdbs.remove(database);
            // 关闭数据库
            instance.close();
            // 更新数据库状态
            config.getDbList().disableDatabase(database);
            wal.updateMeta(lsn);
        } catch (Exception e) {
            throw new KVDBException(e.toString());
        }
    }

    @Override
    public synchronized void dropDatabase(String database) throws KVDBException {
        try {
            // 数据库实例不存在
            DBInfo dbInfo = config.getDbList().getDatabase(database);
            if (null == dbInfo) {
                throw new KVDBException("database not exists");
            }
            // 参与到集群中的数据库实例不可修改
            if (dbClusterMapping.containsKey(database)) {
                throw new KVDBException("database in cluster can not modified");
            }
            KVDBInstance instance = rocksdbs.get(database);
            long lsn = wal.append(WalEntity.newDropDatabaseEntity(database));
            // 关闭数据库实例
            if (null != instance) {
                instance.close();
            }
            rocksdbs.remove(database);
            // 更新配置并删除数据库目录
            config.getDbList().dropDatabase(dbInfo);
            wal.updateMeta(lsn);
        } catch (Exception e) {
            throw new KVDBException(e.toString());
        }
    }

    @Override
    public Wal<Entity, Meta> getWal() {
        return wal;
    }

    @Override
    public void redo() throws IOException, RocksDBException {
        LOGGER.debug("redo wal...");

        if (!wal.metaUpdated()) {
            // disable wal for redo
            wal.disable();

            Meta meta = wal.readMeta();
            long position = wal.position(meta.getLsn());
            long lsn = meta.getLsn();
            while (position >= 0) {
                position = wal.next(position);
                if (position < 0) {
                    break;
                }
                Entity e = wal.get(position);
                if (e == null) {
                    break;
                }
                LOGGER.debug("redo {} {} {}", e.getLsn(), e.getCommand(), e.getDB());
                if (e.getCommand().equals(WalCommand.PUT)) { // redo put
                    KVDBInstance instance = getDatabase(e.getDB());
                    KVWriteBatch batch = instance.beginBatch();
                    for (KV kv : e.getKVs()) {
                        batch.set(kv.getKey(), kv.getValue());
                    }
                    batch.commit();
                } else if (e.getCommand().equals(WalCommand.CREATE_DATABASE)) { // redo create database
                    DBInfo info = new DBInfo();
                    info.setName(e.getDB());
                    for (KV kv : e.getKVs()) {
                        String key = new String(kv.getKey());
                        if (key.equals(DBList.PROPERTITY_ENABLE)) {
                            info.setEnable(BytesUtils.toBoolean(kv.getValue()[0]));
                        } else if (key.equals(DBList.PROPERTITY_ROOTDIR)) {
                            info.setDbRootdir(BytesUtils.toString(kv.getValue()));
                        } else if (key.equals(DBList.PROPERTITY_PARTITIONS)) {
                            info.setPartitions(BytesUtils.toInt(kv.getValue()));
                        }
                    }
                    if (!config.getDbList().getDatabaseNameSet().contains(info.getName())) {
                        createDatabase(info);
                    }
                } else if (e.getCommand().equals(WalCommand.ENABLE_DATABASE)) { // redo enable database
                    enableDatabase(e.getDB());
                } else if (e.getCommand().equals(WalCommand.DISABLE_DATABASE)) { // redo disable database
                    if (rocksdbs.containsKey(e.getDB())) {
                        disableDatabase(e.getDB());
                    }
                } else if (e.getCommand().equals(WalCommand.DROP_DATABASE)) { // redo drop database
                    if (config.getDbList().getDatabaseNameSet().contains(e.getDB())) {
                        dropDatabase(e.getDB());
                    }
                }
                lsn = e.getLsn();
            }

            // update meta
            wal.updateMeta(lsn);

            // reset wal
            if (!config.getKvdbConfig().isWalDisable()) {
                wal.enable();
            }

        }
        LOGGER.debug("redo wal complete");
    }

    public void stop() throws IOException {
        clients.clear();
        for (KVDBInstance db : rocksdbs.values()) {
            db.close();
        }
        wal.close();
    }

    public Session getSession(String sourceKey, Function<String, Session> factory) {
        return clients.computeIfAbsent(sourceKey, key -> factory.apply(key));
    }

    public Session getSession(String key) {
        return clients.get(key);
    }

    /**
     * 移除会话
     *
     * @param sourceKey
     */
    protected void removeSession(String sourceKey) {
        Session session = getSession(sourceKey);
        if (null != session) {
            session.close();
        }
        clients.remove(sourceKey);
    }

    /**
     * 添加命令处理对象
     *
     * @param name
     * @param executor
     */
    protected void addExecutor(String name, Executor executor) {
        executors.put(name.toLowerCase(), executor);
    }

    /**
     * 执行命令
     *
     * @param sourceKey
     * @param message
     */
    public void processCommand(String sourceKey, Message message) {
        Command command = (Command) message.getContent();
        Session session = getSession(sourceKey);
        session.publish(executors.get(command.getName()).execute(new KVDBRequest(this, session, message)));
    }
}
