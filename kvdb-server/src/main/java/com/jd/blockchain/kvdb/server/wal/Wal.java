package com.jd.blockchain.kvdb.server.wal;

import java.io.IOException;

/**
 * write-ahead-logging
 */
public interface Wal {

    /**
     * 追加日志
     *
     * @param entity
     * @throws IOException
     */
    void append(Entity entity) throws IOException;

    /**
     * 落盘
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * 下一日志起始位置
     *
     * @param position
     * @return
     * @throws IOException
     */
    long next(long position) throws IOException;

    /**
     * 根据起始位置和长度解析日志
     *
     * @param position
     * @param length
     * @return
     * @throws IOException
     */
    Entity get(long position, int length) throws IOException;

    /**
     * 根据起始位置解析日志
     *
     * @param position
     * @return
     * @throws IOException
     */
    Entity get(long position) throws IOException;

    /**
     * 根据原子操作ID查找日志
     *
     * @param id
     * @return
     * @throws IOException
     */
    Entity get(String id) throws IOException;

    /**
     * 根据原子操作ID检查日志存在性
     *
     * @param id
     * @return
     * @throws IOException
     */
    boolean exists(String id) throws IOException;

}
