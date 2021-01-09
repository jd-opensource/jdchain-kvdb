package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.engine.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.Message;

import utils.Bytes;

import java.util.Map;

/**
 * 连接会话
 */
public interface Session {

    /**
     * 会话ID
     *
     * @return
     */
    String getId();

    /**
     * 设置当前会话使用的数据库实例名称
     *
     * @param dbName
     * @param instance
     */
    void setDB(String dbName, KVDBInstance instance);

    /**
     * @return 数据库实例
     */
    KVDBInstance getDBInstance();

    /**
     * @return 数据库名称
     */
    String getDBName();

    /**
     * 返送消息
     *
     * @param msg
     */
    void publish(Message msg);

    /**
     * 关闭
     */
    void close();

    /**
     * @return 是否处于批处理模式
     */
    boolean batchMode();

    /**
     * 开启批处理
     *
     * @throws KVDBException
     */
    void batchBegin() throws KVDBException;

    /**
     * 取消批处理
     *
     * @throws KVDBException
     */
    void batchAbort() throws KVDBException;

    /**
     * 提交批处理
     *
     * @throws KVDBException
     */
    void batchCommit() throws KVDBException;

    /**
     * 提交批处理
     *
     * @param size 批处理操作校验
     * @throws KVDBException
     */
    void batchCommit(long size) throws KVDBException;

    /**
     * 键值存在性查询
     *
     * @param keys 支持多个键
     * @return
     * @throws KVDBException
     */
    boolean[] exists(Bytes... keys) throws KVDBException;

    /**
     * 查询键值
     *
     * @param keys 支持多个键
     * @return
     * @throws KVDBException
     */
    Bytes[] get(Bytes... keys) throws KVDBException;

    /**
     * 设置键值
     *
     * @param kvs 支持批量写入
     * @throws KVDBException
     */
    void put(Map<Bytes, byte[]> kvs) throws KVDBException;

    /**
     * 设置键值
     *
     * @param key
     * @param value
     * @throws KVDBException
     */
    void put(Bytes key, byte[] value) throws KVDBException;
}
