package com.jd.blockchain.kvdb.protocol.proto.wal;

import static com.jd.blockchain.kvdb.protocol.Constants.WAL_KV;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;

@DataContract(code = WAL_KV)
public interface KV {

    @DataField(order = 1, primitiveType = PrimitiveType.BYTES)
    byte[] getKey();

    @DataField(order = 2, primitiveType = PrimitiveType.BYTES)
    byte[] getValue();

}
