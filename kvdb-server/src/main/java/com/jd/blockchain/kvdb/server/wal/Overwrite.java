package com.jd.blockchain.kvdb.server.wal;

import java.io.IOException;

/**
 * file overwrite
 */
public interface Overwrite<T> {

    /**
     * 查询出所有数据
     *
     * @return
     * @throws IOException
     */
    T readMeta() throws IOException;

    /**
     * 覆盖写入新数据
     *
     * @param data
     * @throws IOException
     */
    void writeMeta(T data) throws IOException;

}
