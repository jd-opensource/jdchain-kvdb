package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;

/**
 * KVDB SDK 所有支持操作
 */
public interface KVDBOperator {

    // 是否存在某个键值
    boolean exists(Bytes key) throws KVDBException;

    // 查询多个键存在性
    boolean[] exists(Bytes... keys) throws KVDBException;

    // 获取键值
    Bytes get(Bytes key) throws KVDBException;

    // 获取多个键值
    Bytes[] get(Bytes... keys) throws KVDBException;

    // 设置键值对，支持一次多个键值对操作以`key value key value ...`即`key`，`value`交替出现的方式提交
    boolean put(Bytes... kvs) throws KVDBException;

    // 开启`batch`
    boolean batchBegin() throws KVDBException;

    // 取消`batch`
    boolean batchAbort() throws KVDBException;

    // 提交`batch`，未提交的`batch`对其他客户端连接不可见。
    boolean batchCommit() throws KVDBException;
}
