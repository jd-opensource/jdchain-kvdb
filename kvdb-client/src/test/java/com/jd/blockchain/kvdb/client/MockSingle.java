package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

import utils.Bytes;

/**
 * KVDBSingle测试类
 */
public class MockSingle implements KVDBOperator {

    public MockSingle() {
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        return true;
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        return new boolean[keys.length];
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return key;
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        return new Bytes[keys.length];
    }

    @Override
    public boolean put(Bytes key, Bytes value) throws KVDBException {
        return true;
    }

    @Override
    public boolean put(Bytes key, Bytes value, boolean aSync) throws KVDBException {
        return true;
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        return true;
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        return true;
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        return true;
    }

    @Override
    public boolean batchCommit(long size) throws KVDBException {
        return true;
    }

    @Override
    public void close() {

    }
}
