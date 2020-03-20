package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVWriteBatch;
import com.jd.blockchain.utils.Bytes;
import io.netty.channel.ChannelHandlerContext;
import org.rocksdb.RocksDBException;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultSession implements Session {

    private final String id;
    private final ChannelHandlerContext ctx;
    private KVDBInstance db;
    private boolean batchMode;
    private ConcurrentHashMap<Bytes, byte[]> batch;

    public DefaultSession(String id, ChannelHandlerContext ctx, KVDBInstance db) {
        this.id = id;
        this.ctx = ctx;
        this.db = db;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setDB(KVDBInstance db) throws RocksDBException {
        batchAbort();
        this.db = db;
    }

    @Override
    public KVDBInstance getDB() {
        return db;
    }

    @Override
    public void publish(Message msg) {
        ctx.writeAndFlush(msg);
    }

    @Override
    public void close() {
        if (null != batch) {
            batch.clear();
        }
        ctx.close();
    }

    @Override
    public boolean batchMode() {
        return batchMode;
    }

    @Override
    public synchronized void batchBegin() throws RocksDBException {
        if (batchMode) {
            return;
        }
        batchMode = true;
        if (null != batch) {
            batch.clear();
        } else {
            batch = new ConcurrentHashMap<>();
        }
    }

    @Override
    public synchronized void batchAbort() throws RocksDBException {
        batchMode = false;
        if (null != batch) {
            batch.clear();
        }
    }

    @Override
    public synchronized void batchCommit() throws RocksDBException {
        batchMode = false;
        if (null != batch) {
            KVWriteBatch writeBatch = db.beginBatch();
            Iterator<Map.Entry<Bytes, byte[]>> iterator = batch.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Bytes, byte[]> entry = iterator.next();
                writeBatch.set(entry.getKey().toBytes(), entry.getValue());
            }
            writeBatch.commit();
            batch.clear();
        }
    }

    @Override
    public byte[] doInBatch(BatchHook hook) throws RocksDBException {
        return hook.exec(batch);
    }
}
