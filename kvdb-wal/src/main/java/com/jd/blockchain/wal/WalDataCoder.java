package com.jd.blockchain.wal;

import java.io.Serializable;

public interface WalDataCoder<E extends Serializable> {

    /**
     * 编码
     *
     * @param data
     * @return
     */
    byte[] encode(E data);

    /**
     * 解码
     *
     * @param data
     * @return
     */
    E decode(byte[] data);

}
