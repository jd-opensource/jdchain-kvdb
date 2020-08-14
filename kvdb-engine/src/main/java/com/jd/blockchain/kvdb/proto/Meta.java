package com.jd.blockchain.kvdb.proto;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

/**
 * wal meta information
 */
@DataContract(code = 0x102)
public interface Meta {

    /**
     * 最新LSN
     *
     * @return
     */
    @DataField(order = 1, primitiveType = PrimitiveType.INT64)
    Long getLsn();

}
