package com.jd.blockchain.kvdb.server.wal;

import java.io.IOException;

/**
 * write-ahead-logging
 */
public interface Wal<E, M> extends Append<E>, Overwrite<M> {

    /**
     * 当前LSN
     *
     * @return
     * @throws IOException
     */
    long latestLsn() throws IOException;

    /**
     * 下一日志起始位置
     *
     * @param position
     * @return
     * @throws IOException
     */
    long next(long position) throws IOException;

    /**
     * 根据起始位置解析日志
     *
     * @param position
     * @return
     * @throws IOException
     */
    E get(long position) throws IOException;

    /**
     * 根据原子操作ID查找日志
     *
     * @param lsn
     * @return
     * @throws IOException
     */
    E get(Long lsn) throws IOException;

    /**
     * 起始位置
     *
     * @param lsn
     * @return
     * @throws IOException
     */
    long position(Long lsn) throws IOException;

    /**
     * 根据原子操作ID检查日志存在性
     *
     * @param lsn
     * @return
     * @throws IOException
     */
    boolean exists(Long lsn) throws IOException;

    /**
     * disable wal
     */
    void disable();

    /**
     * enable wal
     */
    void enable();

    /**
     * 更新meta信息
     *
     * @param lsn
     * @return
     * @throws IOException
     */
    void updateMeta(long lsn) throws IOException;

    /**
     * 是否是最最新数据
     *
     * @return
     * @throws IOException
     */
    boolean metaUpdated() throws IOException;

}
