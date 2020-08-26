package com.jd.blockchain.kvdb.protocol.proto.wal;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

import static com.jd.blockchain.kvdb.protocol.Constants.WAL_KV;

@DataContract(code = WAL_KV)
public interface KV {

    @DataField(order = 1, primitiveType = PrimitiveType.BYTES)
    byte[] getKey();

    @DataField(order = 2, primitiveType = PrimitiveType.BYTES)
    byte[] getValue();

}
