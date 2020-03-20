package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.StringUtils;

public class Help implements Cmd {

    @Override
    public void execute(KVDBClient client, ReadWriter rw, String... parameters) throws KVDBException {
        rw.writeLine("support commands:");
        rw.writeLine("");
        for (Cmds cmd : Cmds.values()) {
            if (!StringUtils.isEmpty(cmd.name)) {
                rw.writeLine(" " + cmd.name + " : " + cmd.help);
            }
        }
        rw.writeLine("");
    }

}
