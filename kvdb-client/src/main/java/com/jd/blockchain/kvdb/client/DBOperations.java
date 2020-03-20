package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;

public interface DBOperations {

    boolean select(long timeout, int db) throws KVDBException;

    boolean select(int db) throws KVDBException;

    boolean exists(long timeout, Bytes key) throws KVDBException;

    boolean exists(Bytes key) throws KVDBException;

    boolean[] exists(long timeout, Bytes... keys) throws KVDBException;

    boolean[] exists(Bytes... keys) throws KVDBException;

    Bytes get(long timeout, Bytes key) throws KVDBException;

    Bytes get(Bytes key) throws KVDBException;

    Bytes[] get(long timeout, Bytes... keys) throws KVDBException;

    Bytes[] get(Bytes... keys) throws KVDBException;

    boolean put(long timeout, Bytes... keys) throws KVDBException;

    boolean put(Bytes... kvs) throws KVDBException;

    boolean batchBegin(long timeout) throws KVDBException;

    boolean batchBegin() throws KVDBException;

    boolean batchAbort(long timeout) throws KVDBException;

    boolean batchAbort() throws KVDBException;

    boolean batchCommit(long timeout) throws KVDBException;

    boolean batchCommit() throws KVDBException;
}
