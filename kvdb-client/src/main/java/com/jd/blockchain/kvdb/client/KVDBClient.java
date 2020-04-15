package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.exception.KVDBTimeoutException;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KVDBClient implements KVDBOperator {

    private ClientConfig config;
    private Map<String, NettyClient> clients = new HashMap<>();
    private KVDBOperator operator;

    public KVDBClient(KVDBURI uri) throws KVDBException {
        this(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
    }

    public KVDBClient(ClientConfig config) throws KVDBException {
        this.config = config;
        start();
    }

    private void start() {
        clients.put(config.getHost() + config.getPort(), newNettyClient(config));
        if (!StringUtils.isEmpty(config.getDatabase())) {
            use(config.getDatabase());
        }
    }

    private void restart() {
        close();
        start();
    }

    private NettyClient newNettyClient(ClientConfig config) {
        CountDownLatch cdl = new CountDownLatch(1);
        NettyClient client = new NettyClient(config, () -> {
            if (cdl.getCount() > 0) {
                cdl.countDown();
            } else {
                restart();
            }
        });
        try {
            cdl.await(config.getTimeout(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new KVDBTimeoutException("new netty client timeout");
        }
        return client;
    }

    public void close() {
        for (Map.Entry<String, NettyClient> entry : clients.entrySet()) {
            entry.getValue().stop();
        }
        clients.clear();
    }

    public synchronized DatabaseInfo use(String db) throws KVDBException {
        if (StringUtils.isEmpty(db)) {
            throw new KVDBException("database is empty");
        }
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
                operator = new KVDBSingle(clients.get(config.getHost() + config.getPort()));
            }

            return info;
        } catch (Exception e) {
            String msg = e.getMessage();
            throw new KVDBException(!StringUtils.isEmpty(msg) ? msg : e.toString());
        }
    }

    public boolean createDatabase(String db) throws KVDBException {
        if (StringUtils.isEmpty(db)) {
            throw new KVDBException("database is empty");
        }
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.createDatabase(Bytes.fromString(db)));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

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
