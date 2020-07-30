package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

/**
 * wal meta information
 */
@DataContract(code = Constants.WAL_META)
public interface Meta {

    /**
     * 最新LSN
     *
     * @return
     */
    @DataField(order = 1, primitiveType = PrimitiveType.INT64)
    Long getLsn();

}
