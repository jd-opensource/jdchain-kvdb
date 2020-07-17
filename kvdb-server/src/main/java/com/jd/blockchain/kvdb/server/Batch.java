package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.utils.Bytes;
import org.rocksdb.RocksDBException;

/**
 * 批处理
 */
public interface Batch {

    /**
     * 开启批处理
     */
    void begin();

    /**
     * 提交批处理
     */
    void commit(KVDBInstance instance) throws RocksDBException;

    /**
     * 取消批处理
     */
    void abort();

    /**
     * 关闭
     */
    void close();

    /**
     * 操作数
     */
    long size();

    /**
     * 获取批处理中键值
     */
    byte[] get(Bytes key);

    /**
     * 设置键值
     */
    void put(Bytes key, byte[] value);
}
