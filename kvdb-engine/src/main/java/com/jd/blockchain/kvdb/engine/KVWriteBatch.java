package com.jd.blockchain.kvdb.engine;

import org.rocksdb.RocksDBException;

public interface KVWriteBatch extends KVWrite {

    void commit() throws RocksDBException;

}
