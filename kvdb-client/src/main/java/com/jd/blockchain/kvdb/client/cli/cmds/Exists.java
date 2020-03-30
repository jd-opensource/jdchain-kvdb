package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBSingle;
import com.jd.blockchain.kvdb.client.cli.exception.ErrorParametersException;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;

public class Exists implements Cmd {

    @Override
    public void execute(KVDBSingle client, ReadWriter rw, String... parameters) throws KVDBException {
        if (null == parameters || parameters.length == 0) {
            throw new ErrorParametersException("please input any keys");
        }
        Bytes[] keys = new Bytes[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            keys[i] = Bytes.fromString(parameters[i]);
        }
        boolean[] values = client.exists(keys);
        for (int i = 0; i < parameters.length; i++) {
            rw.writeLine(String.format("\"%s\"=%s", parameters[i], values[i]));
        }
    }

}
