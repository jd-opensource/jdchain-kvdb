package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.Message;
import io.netty.channel.ChannelHandlerContext;
import org.rocksdb.RocksDBException;

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
    // 批处理
    private KVDBBatch batch;

    public KVDBSession(String id, ChannelHandlerContext ctx) {
        this.id = id;
        this.ctx = ctx;
        this.batch = new KVDBBatch();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public synchronized void setDB(String dbName, KVDBInstance instance) throws RocksDBException {
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
        batch.close();
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
     *
     * @throws RocksDBException
     */
    @Override
    public synchronized void batchBegin() throws RocksDBException {
        if (!batchMode) {
            batchMode = true;
        }
        batch.begin();
    }

    /**
     * 取消批处理，幂等
     *
     * @throws RocksDBException
     */
    @Override
    public synchronized void batchAbort() throws RocksDBException {
        batchMode = false;
        batch.abort();
    }

    /**
     * 提交批处理，执行rocksdb批处理操作
     *
     * @throws RocksDBException
     */
    @Override
    public synchronized void batchCommit() throws RocksDBException {
        batchMode = false;
        batch.commit(instance);
    }

    @Override
    public synchronized void batchCommit(long size) throws RocksDBException {
        batchMode = false;
        if (batch.size() != size) {
            throw new KVDBException("batch size not match, expect:" + size + ", actually:" + batch.size());
        }
        batch.commit(instance);
    }

    /**
     * 批处理时读钩子，具体操作逻辑由各自executor定义
     *
     * @param hook
     * @return
     * @throws RocksDBException
     */
    @Override
    public byte[] readInBatch(BatchHook hook) throws RocksDBException {
        return hook.exec(batch);
    }

    /**
     * 批处理时读钩子，具体操作逻辑由各自executor定义
     *
     * @param hook
     * @return
     * @throws RocksDBException
     */
    @Override
    public byte[] writeInBatch(BatchHook hook) throws RocksDBException {
        if (batch.size() < MAX_BATCH_SIZE) {
            synchronized (batch) {
                if (batch.size() < MAX_BATCH_SIZE) {
                    return hook.exec(batch);
                }
            }
        }
        throw new KVDBException("executions in this batch is too huge, no more execution allowed");
    }
}
