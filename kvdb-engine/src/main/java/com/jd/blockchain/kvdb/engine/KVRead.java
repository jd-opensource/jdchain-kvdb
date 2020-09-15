package com.jd.blockchain.kvdb.engine;

import org.rocksdb.RocksDBException;

public interface KVRead {

    byte[] get(byte[] key) throws RocksDBException;

}