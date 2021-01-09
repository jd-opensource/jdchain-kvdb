package com.jd.blockchain.kvdb.protocol.proto;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

/**
 * KVDB 消息封装
 */
@DataContract(code = Constants.MESSAGE)
public interface Message {

    /**
     * 消息ID
     *
     * @return
     */
    @DataField(order = 0, primitiveType = PrimitiveType.INT64)
    Long getId();

    /**
     * 消息体
     *
     * @return
     */
    @DataField(order = 2, refContract = true, genericContract = true)
    MessageContent getContent();

}
