package com.jd.blockchain.kvdb.engine;

import org.rocksdb.RocksDBException;

public interface KVWrite {

    void set(byte[] key, byte[] value) throws RocksDBException;

}