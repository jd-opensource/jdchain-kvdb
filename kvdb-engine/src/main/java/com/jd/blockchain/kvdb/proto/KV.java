package com.jd.blockchain.kvdb.proto;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

@DataContract(code = 0x100)
public interface KV {

    @DataField(order = 1, primitiveType = PrimitiveType.BYTES)
    byte[] getKey();

    @DataField(order = 2, primitiveType = PrimitiveType.BYTES)
    byte[] getValue();

}
