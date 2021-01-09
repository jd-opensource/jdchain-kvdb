package com.jd.blockchain.kvdb.protocol.proto;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

import utils.Bytes;

/**
 * 返回消息
 */
@DataContract(code = Constants.RESPONSE)
public interface Response extends MessageContent {

    /**
     * @return 状态, {@link Constants#SUCCESS}, {@link Constants#ERROR}
     */
    @DataField(order = 0, primitiveType = PrimitiveType.INT32)
    int getCode();

    /**
     * @return 结果
     */
    @DataField(order = 1, list = true, primitiveType = PrimitiveType.BYTES)
    Bytes[] getResult();

}
