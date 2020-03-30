package com.jd.blockchain.kvdb.client.cli.cmds;

import com.jd.blockchain.kvdb.client.KVDBSingle;
import com.jd.blockchain.kvdb.client.cli.utils.ReadWriter;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

public interface Cmd {

    enum Cmds {
        SELECT("select", "change database"),
        PUT("put", "set key-value pairs"),
        GET("get", "get key-value pairs"),
        EXISTS("exists", "check existence of keys"),
        BATCH_BEGIN("batch-begin", "begin batch"),
        BATCH_ABORT("batch-abort", "abort batch"),
        BATCH_COMMIT("batch-commit", "commit batch"),
        UNKNOW("", "unknow command"),
        HELP("help", "i am the man"),
        QUIT("quit", "bye~");

        String name;

        String help;

        Cmds(String name, String help) {
            this.name = name;
            this.help = help;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getHelp() {
            return help;
        }

        public void setHelp(String help) {
            this.help = help;
        }
    }

    void execute(KVDBSingle client, ReadWriter rw, String... parameters) throws KVDBException;

}
