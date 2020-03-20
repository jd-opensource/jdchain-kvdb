package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

public class Quit implements Cmd {

    @Override
    public void execute(KVDBClient client, ReadWriter rw, String... parameters) throws KVDBException {
        client.stop();
        System.exit(0);
    }

}
