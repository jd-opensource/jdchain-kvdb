package com.jd.blockchain.kvdb.client.cli;

import com.jd.blockchain.kvdb.client.ClientConfig;
import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.client.cli.cmds.*;
import com.jd.blockchain.kvdb.client.cli.utils.ConsoleUtils;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;
import com.jd.blockchain.utils.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KVDBCli {

    private static final Map<String, Cmd> CMDS = new HashMap<>();

    static {
        CMDS.put(Cmd.Cmds.SELECT.getName(), new Select());
        CMDS.put(Cmd.Cmds.EXISTS.getName(), new Exists());
        CMDS.put(Cmd.Cmds.GET.getName(), new Get());
        CMDS.put(Cmd.Cmds.PUT.getName(), new Put());
        CMDS.put(Cmd.Cmds.BATCH_BEGIN.getName(), new BatchBegin());
        CMDS.put(Cmd.Cmds.BATCH_ABORT.getName(), new BatchAbort());
        CMDS.put(Cmd.Cmds.BATCH_COMMIT.getName(), new BatchCommit());
        CMDS.put(Cmd.Cmds.UNKNOW.getName(), new Unknow());
        CMDS.put(Cmd.Cmds.QUIT.getName(), new Quit());
        CMDS.put(Cmd.Cmds.HELP.getName(), new Help());
    }

    public static void main(String[] args) {
        ClientConfig config = new ClientConfig(args);
        KVDBClient client = new KVDBClient(config);
        client.start();
        ReadWriter rw = ConsoleUtils.getReadWriter();
        while (true) {
            try {
                rw.writePrefix(config.getHost(), config.getPort());
                String line = rw.readLine();
                if (!StringUtils.isEmpty(line)) {
                    String[] inputs = line.split("\\s+");
                    Cmd cmd = CMDS.get(inputs[0]);
                    if (null == cmd) {
                        cmd = CMDS.get(Cmd.Cmds.UNKNOW.getName());
                    }
                    cmd.execute(client, rw, Arrays.copyOfRange(inputs, 1, inputs.length));
                }
            } catch (Exception e) {
                rw.writeLine(e.getMessage());
            }
        }

    }

}
