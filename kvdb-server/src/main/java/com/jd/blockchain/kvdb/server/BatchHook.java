package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.utils.Bytes;
import org.rocksdb.RocksDBException;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 批处理钩子
 */
public interface BatchHook {

    byte[] exec(ConcurrentHashMap<Bytes, byte[]> wb) throws RocksDBException;

}
