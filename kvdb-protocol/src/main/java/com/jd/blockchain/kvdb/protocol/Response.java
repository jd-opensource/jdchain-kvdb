package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.utils.Bytes;

/**
 * Response from server
 */
@DataContract(code = Constants.RESPONSE)
public interface Response extends MessageContent {

    @DataField(order = 0, primitiveType = PrimitiveType.INT32)
    int getCode();

    @DataField(order = 1, list = true, primitiveType = PrimitiveType.BYTES)
    Bytes[] getResult();

}
