package com.jd.blockchain.kvdb.engine.wal;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * file overwrite
 */
public interface Overwrite<T extends Serializable> extends Closeable {

    /**
     * 查询出所有数据
     *
     * @return
     * @throws IOException
     */
    T read() throws IOException;

    /**
     * 覆盖写入新数据
     *
     * @param data
     * @throws IOException
     */
    void write(T data) throws IOException;

}
