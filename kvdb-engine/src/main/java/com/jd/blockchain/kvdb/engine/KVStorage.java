package com.jd.blockchain.kvdb.engine;

import org.rocksdb.RocksDBException;

import utils.Bytes;

import java.util.Map;

public interface KVStorage extends KVWrite, KVRead {

    /**
     * 提交一次批处理
     *
     * @param kvs
     */
    void batchSet(Map<Bytes, byte[]> kvs) throws RocksDBException;

}