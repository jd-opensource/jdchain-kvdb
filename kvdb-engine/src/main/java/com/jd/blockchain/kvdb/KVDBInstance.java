package com.jd.blockchain.kvdb;

public abstract class KVDBInstance implements KVStorage {

    /**
     * 关闭数据库；<p>
     * <p>
     * 注：关闭过程中可能引发的异常将被处理而不会被抛出；
     */
    public abstract void close();

    /**
     * 移除数据库；<p>
     * <p>
     * 注：移除过程中可能引发的异常将被处理而不会被抛出；
     */
    public abstract void drop();
}