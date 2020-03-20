package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.utils.Bytes;

/**
 * Command from client
 */
@DataContract(code = Constants.COMMAND)
public interface Command extends MessageContent {

    enum CommandType {
        SELECT("select"),
        PUT("put"),
        GET("get"),
        EXISTS("exists"),
        BATCH_BEGIN("batch-begin"),
        BATCH_ABORT("batch-abort"),
        BATCH_COMMIT("batch-commit");

        String command;

        CommandType(String command) {
            this.command = command;
        }

        public String getCommand() {
            return command;
        }

        public void setCommand(String command) {
            this.command = command;
        }
    }

    @DataField(order = 0, primitiveType = PrimitiveType.TEXT)
    String getName();

    @DataField(order = 1, list = true, primitiveType = PrimitiveType.BYTES)
    Bytes[] getParameters();

}
