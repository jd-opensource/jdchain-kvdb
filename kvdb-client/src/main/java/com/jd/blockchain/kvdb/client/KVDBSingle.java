package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.exception.KVDBTimeoutException;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KVDBSingle implements KVDBOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBSingle.class);

    private final NettyClient client;

    public KVDBSingle(NettyClient client) {
        this.client = client;
    }

    public Response send(Message message) throws KVDBException {
        return client.send(message);
    }

    @Override
    public void close() {
        client.stop();
    }

    @Override
    public boolean use(String db) throws KVDBException {
        Response response = send(KVDBMessage.use(Bytes.fromString(db)));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean createDatabase(String db) throws KVDBException {
        Response response = send(KVDBMessage.createDB(Bytes.fromString(db)));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public Info info() throws KVDBException {
        Response response = send(KVDBMessage.info());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return BinaryProtocol.decodeAs(response.getResult()[0].toBytes(), Info.class);
    }

    @Override
    public String[] showDatabases() throws KVDBException {
        Response response = send(KVDBMessage.showDBs());
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
        Response response = send(KVDBMessage.exists(key));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return BytesUtils.toInt(response.getResult()[0].toBytes()) == 1;
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        Response response = send(KVDBMessage.exists(keys));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        boolean[] results = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            results[i] = BytesUtils.toInt(response.getResult()[i].toBytes()) == 1;
        }

        return results;
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        Response response = send(KVDBMessage.get(key));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return response.getResult()[0];
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        Response response = send(KVDBMessage.get(keys));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return response.getResult();
    }

    @Override
    public boolean put(Bytes... kvs) throws KVDBException {
        if (kvs.length % 2 != 0) {
            throw new KVDBException("keys and values must in pairs");
        }
        Response response = send(KVDBMessage.put(kvs));
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        Response response = send(KVDBMessage.batchBegin());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        Response response = send(KVDBMessage.batchAbort());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        Response response = send(KVDBMessage.batchCommit());
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }
}
