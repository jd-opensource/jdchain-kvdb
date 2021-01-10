package com.jd.blockchain.wal;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

public interface Wal<E extends Serializable> extends Closeable {

    /**
     * 追加日志
     *
     * @param data
     * @return 写入字节长度
     * @throws IOException
     */
    int append(E data) throws IOException;

    /**
     * 设置最新日志快照，旧日志将不再可读，并将在未来某一时刻被清除
     *
     * @throws IOException
     */
    void checkpoint() throws IOException;

    /**
     * 刷盘
     *
     * @throws IOException
     */
    void flush() throws IOException;

    /**
     * 前向迭代器
     *
     * @return
     * @throws IOException
     */
    WalIterator<E> forwardIterator() throws IOException;

    /**
     * 反向迭代器
     *
     * @return
     * @throws IOException
     */
    WalIterator<E> backwardIterator() throws IOException;

    /**
     * 清空日志内容
     */
    void clear() throws IOException;

}
