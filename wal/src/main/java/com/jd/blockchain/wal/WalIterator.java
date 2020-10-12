package com.jd.blockchain.wal;

import java.io.IOException;
import java.io.Serializable;

/**
 * 日志迭代器
 */
public interface WalIterator<E extends Serializable> {

    boolean hasNext() throws IOException;

    E next() throws IOException;

}
