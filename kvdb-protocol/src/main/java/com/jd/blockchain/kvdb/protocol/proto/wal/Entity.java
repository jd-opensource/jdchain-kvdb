package com.jd.blockchain.kvdb.protocol.proto.wal;

import java.io.Serializable;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;

import static com.jd.blockchain.kvdb.protocol.Constants.WAL_ENTITY;

@DataContract(code = WAL_ENTITY)
public interface Entity extends Serializable {

    @DataField(order = 1, list = true, refContract = true)
    KV[] getKVs();
}
