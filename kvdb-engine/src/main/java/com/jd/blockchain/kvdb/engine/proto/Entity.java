package com.jd.blockchain.kvdb.engine.proto;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;

import java.io.Serializable;

@DataContract(code = 0x101)
public interface Entity extends Serializable {

    @DataField(order = 1, list = true, refContract = true)
    KV[] getKVs();
}
