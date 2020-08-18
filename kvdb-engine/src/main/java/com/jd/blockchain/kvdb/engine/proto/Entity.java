package com.jd.blockchain.kvdb.engine.proto;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

@DataContract(code = 0x101)
public interface Entity {

    @DataField(order = 1, primitiveType = PrimitiveType.INT64)
    Long getLsn();

    @DataField(order = 2, list = true, refContract = true)
    KV[] getKVs();

    void setLsn(Long lsn);
}
