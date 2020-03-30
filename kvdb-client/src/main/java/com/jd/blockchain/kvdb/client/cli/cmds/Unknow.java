package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBSingle;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;

public class Unknow implements Cmd {

    @Override
    public void execute(KVDBSingle client, ReadWriter rw, String... parameters) throws KVDBException {
        rw.writeLine("un support cmd");
    }

}
