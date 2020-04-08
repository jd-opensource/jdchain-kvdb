package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.Info;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.Response;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVDBClient implements KVDBOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBClient.class);

    private KVDBOperator operator;

    public KVDBClient(KVDBURI uri) throws KVDBException {
        this(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()));
    }

    public KVDBClient(ClientConfig config) throws KVDBException {
        NettyClient nettyClient = new NettyClient(config, false);
        Response response = nettyClient.send(KVDBMessage.info());
        Info info = BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), Info.class);
        if (info.isClusterMode()) {
            operator = new KVDBCluster(config.getDb(), info.getCluster());
        } else {
            operator = new KVDBSingle(new NettyClient(config));
        }
        nettyClient.stop();
    }

    @Override
    public void close() {
        operator.close();
    }

    @Override
    public boolean use(String db) throws KVDBException {
        return operator.use(db);
    }

    @Override
    public boolean createDatabase(String db) throws KVDBException {
        return operator.createDatabase(db);
    }

    @Override
    public Info info() throws KVDBException {
        return operator.info();
    }

    @Override
    public String[] showDatabases() throws KVDBException {
        return operator.showDatabases();
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
