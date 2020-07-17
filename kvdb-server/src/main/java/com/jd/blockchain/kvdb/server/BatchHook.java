package com.jd.blockchain.kvdb.server;

import org.rocksdb.RocksDBException;

/**
 * 批处理钩子
 */
public interface BatchHook {

    byte[] exec(Batch batch) throws RocksDBException;

}
