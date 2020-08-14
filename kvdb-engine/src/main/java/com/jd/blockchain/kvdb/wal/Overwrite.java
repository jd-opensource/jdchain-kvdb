package com.jd.blockchain.kvdb.wal;

import java.io.IOException;
import java.io.Serializable;

/**
 * file overwrite
 */
public interface Overwrite<T extends Serializable> {

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
