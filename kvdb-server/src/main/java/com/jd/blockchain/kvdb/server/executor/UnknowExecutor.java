package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_UNKNOWN;

/**
 * 处理未被识别的操作
 */
@KVDBExecutor(command = COMMAND_UNKNOWN)
public class UnknowExecutor implements Executor {

    @Override
    public Message execute(Request request) {

        return KVDBMessage.error(request.getId(), "un support command: " + request.getCommand().getName());
    }
}
