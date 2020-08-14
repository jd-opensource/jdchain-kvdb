package com.jd.blockchain.kvdb;

import org.rocksdb.RocksDBException;

public interface KVWrite {

    void set(byte[] key, byte[] value) throws RocksDBException;

}