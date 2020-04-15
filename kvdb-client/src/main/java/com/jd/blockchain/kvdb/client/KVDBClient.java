package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.exception.KVDBTimeoutException;
import com.jd.blockchain.kvdb.protocol.parameter.CreateDatabaseParam;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * KVDB-SDK
 */
public class KVDBClient implements KVDBOperator {

    private ClientConfig config;
    /**
     * 保存当前所有连接，服务器地址加端口作为键值
     */
    private Map<String, NettyClient> clients = new HashMap<>();
    /**
     * 当前数据库实际操作者，当前选择数据库为单实例时使用{@link KVDBSingle}，否则{@link KVDBCluster}
     */
    private KVDBOperator operator;

    public KVDBClient(KVDBURI uri) throws KVDBException {
        this(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
    }

    public KVDBClient(ClientConfig config) throws KVDBException {
        this.config = config;
        start();
    }

    /**
     * 创建客户端等待就绪状态，当配置数据库不为空时执行切换数据库操作
     */
    private void start() {
        clients.put(config.getHost() + config.getPort(), newNettyClient(config));
        if (!StringUtils.isEmpty(config.getDatabase())) {
            use(config.getDatabase());
        }
    }

    /**
     * 创建服务端连接，提供连接就绪回调接口。
     * 针对连接初始创建回调接口执行唤醒等待操作；
     * 针对服务掉线重连回调接口执行客户端重启操作。
     *
     * @param config
     * @return
     */
    private NettyClient newNettyClient(ClientConfig config) {
        CountDownLatch cdl = new CountDownLatch(1);
        NettyClient client = new NettyClient(config, () -> {
            if (cdl.getCount() > 0) {
                cdl.countDown();
            } else {
                if (!StringUtils.isEmpty(config.getDatabase())) {
                    clients.get(config.getHost() + config.getPort()).send(KVDBMessage.use(config.getDatabase()));
                }
            }
        });
        try {
            cdl.await(config.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new KVDBTimeoutException("new netty client timeout");
        }
        return client;
    }

    /**
     * 关闭客户端
     */
    public void close() {
        for (Map.Entry<String, NettyClient> entry : clients.entrySet()) {
            entry.getValue().stop();
        }
        clients.clear();
    }

    /**
     * 切换数据库，获取数据库配置信息，自动切换数据库单实例和集群操作模式
     *
     * @param db
     * @return
     * @throws KVDBException
     */
    public synchronized DatabaseInfo use(String db) throws KVDBException {
        if (StringUtils.isEmpty(db)) {
            throw new KVDBException("database is empty");
        }
        // 执行`use`命令，获取数据库配置信息
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.use(db));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }
        try {
            DatabaseInfo info = BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), DatabaseInfo.class);
            config.setDatabase(db);
            if (info.isClusterMode()) {
                // 集群模式下，创建当前数据库所有服务节点连接，并执行切换数据库操作
                NettyClient[] selectedClients = new NettyClient[info.getClusterItem().getURLs().length];
                for (int i = 0; i < info.getClusterItem().getURLs().length; i++) {
                    KVDBURI uri = new KVDBURI(info.getClusterItem().getURLs()[i]);
                    if (uri.isLocalhost() && uri.getPort() == config.getPort()) {
                        selectedClients[i] = clients.get(uri.getHost() + uri.getPort());
                        continue;
                    }
                    NettyClient nettyClient;
                    if (!clients.containsKey(uri.getHost() + uri.getPort())) {
                        nettyClient = newNettyClient(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
                        clients.put(uri.getHost() + uri.getPort(), nettyClient);
                    } else {
                        nettyClient = clients.get(uri.getHost() + uri.getPort());
                    }
                    nettyClient.send(KVDBMessage.use(uri.getDatabase()));
                    selectedClients[i] = nettyClient;
                }
                operator = new KVDBCluster(selectedClients);
            } else {
                // 单实例模式下，无需再执行数据库切换操作，仅切换操作对象
                operator = new KVDBSingle(clients.get(config.getHost() + config.getPort()));
            }

            return info;
        } catch (Exception e) {
            String msg = e.getMessage();
            throw new KVDBException(!StringUtils.isEmpty(msg) ? msg : e.toString());
        }
    }

    /**
     * 创建数据库
     *
     * @param parameter
     * @return
     * @throws KVDBException
     */
    public boolean createDatabase(CreateDatabaseParam parameter) throws KVDBException {
        Response response = clients.get(config.getHost() + config.getPort())
                .send(KVDBMessage.createDatabase(new Bytes(BinaryProtocol.encode(parameter, CreateDatabaseParam.class))));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    /**
     * 服务器集群配置
     *
     * @return
     * @throws KVDBException
     */
    public ClusterItem[] clusterInfo() throws KVDBException {
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.clusterInfo());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }
        ClusterInfo clusterInfo = BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), ClusterInfo.class);
        return clusterInfo.getClusterItems();
    }

    /**
     * 当前服务器所有可提供服务数据库名称列表
     *
     * @return
     * @throws KVDBException
     */
    public String[] showDatabases() throws KVDBException {
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.showDatabases());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        Bytes[] dbs = response.getResult();
        String[] names = new String[dbs.length];
        for (int i = 0; i < dbs.length; i++) {
            names[i] = BytesUtils.toString(dbs[i].toBytes());
        }

        return names;
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.exists(key);
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.exists(keys);
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.get(key);
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.get(keys);
    }

    @Override
    public boolean put(Bytes... kvs) throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.put(kvs);
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.batchBegin();
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.batchAbort();
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        if (StringUtils.isEmpty(config.getDatabase())) {
            throw new KVDBException("no database selected");
        }
        return operator.batchCommit();
    }
}
