package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.engine.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.Message;

import io.netty.channel.ChannelHandlerContext;
import utils.Bytes;

import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.Map;

/**
 * 连接会话
 */
public class KVDBSession implements Session {
    // 最大batch数量
    private static final int MAX_BATCH_SIZE = 10000000;
    // 会话ID
    private final String id;
    // Channel上下文
    private final ChannelHandlerContext ctx;
    // 当前数据库实例名称
    private String dbName;
    // 当前数据库实例
    private KVDBInstance instance;
    // 批处理模式
    private volatile boolean batchMode;
    // 待提交批处理数据集
    private HashMap<Bytes, byte[]> batch;

    public KVDBSession(String id, ChannelHandlerContext ctx) {
        this.id = id;
        this.ctx = ctx;
        this.batch = new HashMap<>();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setDB(String dbName, KVDBInstance instance) {
        batchAbort();
        this.dbName = dbName;
        this.instance = instance;
    }

    @Override
    public KVDBInstance getDBInstance() {
        return instance;
    }

    @Override
    public String getDBName() {
        return dbName;
    }

    @Override
    public void publish(Message msg) {
        if (null != msg) {
            ctx.writeAndFlush(msg);
        }
    }

    @Override
    public void close() {
        if (null != batch) {
            batch.clear();
        }
        batch = null;
        if (null != ctx) {
            ctx.close();
        }
    }

    @Override
    public boolean batchMode() {
        return batchMode;
    }

    /**
     * 开启批处理操作，幂等
     */
    @Override
    public void batchBegin() {
        if (!batchMode) {
            batchMode = true;
        }
        if (null != batch) {
            batch.clear();
        } else {
            batch = new HashMap<>();
        }
    }

    /**
     * 取消批处理，幂等
     */
    @Override
    public void batchAbort() {
        batchMode = false;
        if (null != batch) {
            batch.clear();
        }
    }

    /**
     * 提交批处理，执行rocksdb批处理操作
     *
     * @throws KVDBException
     */
    @Override
    public void batchCommit() throws KVDBException {
        batchCommit(batch.size());
    }

    @Override
    public void batchCommit(long size) throws KVDBException {
        if (!batchMode) {
            throw new KVDBException("not in batch mode");
        }
        batchMode = false;
        if (batch.size() != size) {
            throw new KVDBException("batch size not match, expect:" + size + ", actually:" + batch.size());
        }
        try {
            try {
                instance.batchSet(batch);
            } catch (RocksDBException e) {
                throw new KVDBException(e);
            }
        } finally {
            batch.clear();
        }
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        boolean[] values = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            Bytes key = keys[i];
            byte[] value = null;
            if (batchMode) {
                value = batch.get(key);
            }
            if (null == value) {
                try {
                    value = instance.get(key.toBytes());
                } catch (RocksDBException e) {
                    throw new KVDBException(e);
                }
            }
            values[i] = null != value ? true : false;
        }

        return values;
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        Bytes[] values = new Bytes[keys.length];
        for (int i = 0; i < keys.length; i++) {
            final Bytes key = keys[i];
            byte[] value = null;
            if (batchMode) {
                value = batch.get(key);
            }
            if (null == value) {
                try {
                    value = instance.get(key.toBytes());
                } catch (RocksDBException e) {
                    throw new KVDBException(e);
                }
            }
            values[i] = null != value ? new Bytes(value) : null;
        }

        return values;
    }

    @Override
    public void put(Map<Bytes, byte[]> kvs) throws KVDBException {
        if (kvs.size() > MAX_BATCH_SIZE) {
            throw new KVDBException("too large executions");
        }
        if (batchMode) {
            if (batch.size() + kvs.size() > MAX_BATCH_SIZE) {
                throw new KVDBException("too large executions in batch");
            }
            batch.putAll(kvs);
        } else {
            try {
                instance.batchSet(kvs);
            } catch (RocksDBException e) {
                throw new KVDBException(e);
            }
        }
    }

    @Override
    public void put(Bytes key, byte[] value) throws KVDBException {
        if (batchMode) {
            if (batch.size() + 1 > MAX_BATCH_SIZE) {
                throw new KVDBException("too large executions in batch");
            }
            batch.put(key, value);
        } else {
            try {
                instance.set(key.toBytes(), value);
            } catch (RocksDBException e) {
                throw new KVDBException(e);
            }
        }
    }

}
