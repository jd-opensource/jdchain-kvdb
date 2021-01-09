package com.jd.blockchain.kvdb.protocol.proto;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

import utils.Bytes;

/**
 * Command from client
 */
@DataContract(code = Constants.COMMAND)
public interface Command extends MessageContent {

    String COMMAND_CREATE_DATABASE = "create database";
    String COMMAND_DISABLE_DATABASE = "disable database";
    String COMMAND_ENABLE_DATABASE = "enable database";
    String COMMAND_DROP_DATABASE = "drop database";
    String COMMAND_CLUSTER_INFO = "cluster";
    String COMMAND_SHOW_DATABASES = "show databases";
    String COMMAND_USE = "use";
    String COMMAND_PUT = "put";
    String COMMAND_GET = "get";
    String COMMAND_EXISTS = "exists";
    String COMMAND_BATCH_BEGIN = "batch begin";
    String COMMAND_BATCH_ABORT = "batch abort";
    String COMMAND_BATCH_COMMIT = "batch commit";
    String COMMAND_UNKNOWN = "unknown";

    enum CommandType {
        CREATE_DATABASE(COMMAND_CREATE_DATABASE, false),
        DISABLE_DATABASE(COMMAND_DISABLE_DATABASE, false),
        ENABLE_DATABASE(COMMAND_ENABLE_DATABASE, false),
        DROP_DATABASE(COMMAND_DROP_DATABASE, false),
        CLUSTER_INFO(COMMAND_CLUSTER_INFO, true),
        SHOW_DATABASES(COMMAND_SHOW_DATABASES, false),
        USE(COMMAND_USE, true),
        PUT(COMMAND_PUT, true),
        GET(COMMAND_GET, true),
        EXISTS(COMMAND_EXISTS, true),
        BATCH_BEGIN(COMMAND_BATCH_BEGIN, true),
        BATCH_ABORT(COMMAND_BATCH_ABORT, true),
        BATCH_COMMIT(COMMAND_BATCH_COMMIT, true),
        UNKNOWN(COMMAND_UNKNOWN, true);

        // 操作名称
        String command;
        // 是否所有端口开放，false表示仅对本地管理工具开放
        boolean open;

        CommandType(String command, boolean open) {
            this.command = command;
            this.open = open;
        }

        public String getCommand() {
            return command;
        }

        public void setCommand(String command) {
            this.command = command;
        }

        public boolean isOpen() {
            return open;
        }

        public static CommandType getCommand(String command) {
            for (CommandType ct : CommandType.values()) {
                if (ct.command.equals(command)) {
                    return ct;
                }
            }

            return UNKNOWN;
        }
    }

    /**
     * @return 命令名称
     */
    @DataField(order = 0, primitiveType = PrimitiveType.TEXT)
    String getName();

    /**
     * @return 参数列表
     */
    @DataField(order = 1, list = true, primitiveType = PrimitiveType.BYTES)
    Bytes[] getParameters();

}
