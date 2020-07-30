package com.jd.blockchain.kvdb.server.wal;

import java.io.Closeable;
import java.io.IOException;

public interface Append<T> extends Closeable {

    /**
     * 追加内容
     *
     * @param data
     * @return
     * @throws IOException
     */
    long append(T data) throws IOException;

    /**
     * 刷盘
     *
     * @param meta
     * @throws IOException
     */
    void flush(boolean meta) throws IOException;

    /**
     * 返回当前内容大小
     *
     * @return
     */
    long size() throws IOException;

    /**
     * 获取指定位置内容
     *
     * @param position
     * @param size
     * @return
     */
    T get(long position, int size) throws IOException;

}
