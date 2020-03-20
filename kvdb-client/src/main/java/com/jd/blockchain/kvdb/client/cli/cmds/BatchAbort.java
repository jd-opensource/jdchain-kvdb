package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;

public class BatchAbort implements Cmd {

    @Override
    public void execute(KVDBClient client, ReadWriter rw, String... parameters) throws KVDBException {

        rw.writeLine(client.batchAbort());
    }

}
