package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.client.cli.exception.ErrorParametersException;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;

public class Select implements Cmd {

    @Override
    public void execute(KVDBClient client, ReadWriter rw, String... parameters) throws KVDBException {
        if (null == parameters || parameters.length != 1) {
            throw new ErrorParametersException("please input one and only one digit");
        }

        int db = 0;
        try {
            db = Integer.valueOf(parameters[0]);
        } catch (Exception e) {
            rw.writeLine("digit parameter needed");
            return;
        }

        rw.writeLine(String.valueOf(client.select(db)));
    }
}
