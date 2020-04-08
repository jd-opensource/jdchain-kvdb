package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.Message;
import org.rocksdb.RocksDBException;

public interface Session {

    String getId();

    void setDB(String dbName, KVDBInstance instance) throws RocksDBException;

    KVDBInstance getDBInstance();

    String getDBName();

    void publish(Message msg);

    void close();

    boolean batchMode();

    void batchBegin() throws RocksDBException;

    void batchAbort() throws RocksDBException;

    void batchCommit() throws RocksDBException;

    byte[] doInBatch(BatchHook hook) throws RocksDBException;
}
