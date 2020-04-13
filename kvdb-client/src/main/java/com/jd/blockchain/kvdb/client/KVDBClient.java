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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class KVDBClient implements KVDBOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBClient.class);

    private ClientConfig config;
    private Map<String, NettyClient> clients = new HashMap<>();
    private KVDBOperator operator;

    public KVDBClient(KVDBURI uri) throws KVDBException {
        this(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
    }

    public KVDBClient(ClientConfig config) throws KVDBException {
        this.config = config;
        clients.put(config.getHost() + config.getPort(), new NettyClient(config));
        use(config.getDb());
    }

    public void close() {
        for (Map.Entry<String, NettyClient> entry : clients.entrySet()) {
            entry.getValue().stop();
        }
    }

    public synchronized DBInfo use(String db) throws KVDBException {
        if (StringUtils.isEmpty(db)) {
            return null;
        }
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.use(db));
        DBInfo info = BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), DBInfo.class);
        if (info.isClusterMode()) {
            NettyClient[] selectedClients = new NettyClient[info.getCluster().getURLs().length];
            for (int i = 0; i < info.getCluster().getURLs().length; i++) {
                KVDBURI uri = new KVDBURI(info.getCluster().getURLs()[i]);
                if ((uri.getHost().equals(config.getHost()) || (KVDBURI.isLocalhost(uri.getHost()) && KVDBURI.isLocalhost(config.getHost())))
                        && uri.getPort() == config.getPort()) {
                    selectedClients[i] = clients.get(uri.getHost() + uri.getPort());
                    continue;
                }
                NettyClient client;
                if (!clients.containsKey(uri.getHost() + uri.getPort())) {
                    client = new NettyClient(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
                    clients.put(uri.getHost() + uri.getPort(), client);
                } else {
                    client = clients.get(uri.getHost() + uri.getPort());
                }
                client.send(KVDBMessage.use(uri.getDatabase()));
                selectedClients[i] = client;
            }
            operator = new KVDBCluster(selectedClients);
        } else {
            operator = new KVDBSingle(clients.get(config.getHost() + config.getPort()));
        }

        return info;
    }

    public boolean createDatabase(String db) throws KVDBException {
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.createDatabase(Bytes.fromString(db)));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    public ClusterInfo[] clusterInfo() throws KVDBException {
        Response response = clients.get(config.getHost() + config.getPort()).send(KVDBMessage.clusterInfo());
        Bytes[] clusterInfos = response.getResult();
        ClusterInfo[] infos = new ClusterInfo[clusterInfos.length];
        for (int i = 0; i < clusterInfos.length; i++) {
            infos[i] = BinaryProtocol.decodeAs(clusterInfos[i].toBytes(), ClusterInfo.class);
        }
        return infos;
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
        return operator.exists(key);
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        return operator.exists(keys);
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return operator.get(key);
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        return operator.get(keys);
    }

    @Override
    public boolean put(Bytes... kvs) throws KVDBException {
        return operator.put(kvs);
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        return operator.batchBegin();
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        return operator.batchAbort();
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        return operator.batchCommit();
    }
}
