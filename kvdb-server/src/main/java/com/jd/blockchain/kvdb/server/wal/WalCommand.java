package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.binaryproto.EnumContract;
import com.jd.blockchain.binaryproto.EnumField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

/**
 * commands in wal
 */
@EnumContract(code = Constants.WAL_COMMAND)
public enum WalCommand {

    CREATE_DATABASE((byte) 0),
    DISABLE_DATABASE((byte) 1),
    ENABLE_DATABASE((byte) 2),
    DROP_DATABASE((byte) 3),
    PUT((byte) 4);

    @EnumField(type = PrimitiveType.INT8)
    public final byte CODE;

    WalCommand(byte code) {
        this.CODE = code;
    }

}
