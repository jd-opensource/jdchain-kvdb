package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

/**
 * Base message
 */
@DataContract(code = Constants.MESSAGE)
public interface Message {

    @DataField(order = 0, primitiveType = PrimitiveType.TEXT)
    String getId();

    @DataField(order = 2, refContract = true, genericContract = true)
    MessageContent getContent();

}
