package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

@DataContract(code = Constants.WAL_KV)
public interface KV {

    @DataField(order = 1, primitiveType = PrimitiveType.BYTES)
    byte[] getKey();

    @DataField(order = 2, primitiveType = PrimitiveType.BYTES)
    byte[] getValue();

}
