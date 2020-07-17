package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVWriteBatch;
import com.jd.blockchain.utils.Bytes;
import org.rocksdb.RocksDBException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class KVDBBatch implements Batch {

    // 待提交批处理数据集
    private HashMap<Bytes, byte[]> batch;

    @Override
    public void begin() {
        if (null != batch) {
            batch.clear();
        } else {
            batch = new HashMap<>();
        }
    }

    @Override
    public void commit(KVDBInstance instance) throws RocksDBException {
        try {
            KVWriteBatch writeBatch = instance.beginBatch();
            Iterator<Map.Entry<Bytes, byte[]>> iterator = batch.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<Bytes, byte[]> entry = iterator.next();
                writeBatch.set(entry.getKey().toBytes(), entry.getValue());
            }
            writeBatch.commit();
        } finally {
            batch.clear();
        }
    }

    @Override
    public void abort() {
        if (null != batch) {
            batch.clear();
        }
    }

    @Override
    public void close() {
        if (null != batch) {
            batch.clear();
        }
        batch = null;
    }

    @Override
    public long size() {
        return null != batch ? batch.size() : 0;
    }

    @Override
    public byte[] get(Bytes key) {
        return null != batch ? batch.get(key) : null;
    }

    @Override
    public void put(Bytes key, byte[] value) {
        if (null != batch) {
            batch.put(key, value);
        }
    }
}
