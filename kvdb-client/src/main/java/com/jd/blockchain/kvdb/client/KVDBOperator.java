package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.Info;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;

/**
 * KVDB SDK 所有支持操作
 */
public interface KVDBOperator {

    void close();

    boolean use(String db) throws KVDBException;

    boolean createDatabase(String db) throws KVDBException;

    Info info() throws KVDBException;

    String[] showDatabases() throws KVDBException;

    boolean exists(Bytes key) throws KVDBException;

    boolean[] exists(Bytes... keys) throws KVDBException;

    Bytes get(Bytes key) throws KVDBException;

    Bytes[] get(Bytes... keys) throws KVDBException;

    boolean put(Bytes... kvs) throws KVDBException;

    boolean batchBegin() throws KVDBException;

    boolean batchAbort() throws KVDBException;

    boolean batchCommit() throws KVDBException;
}
