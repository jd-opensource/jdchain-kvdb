package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_UNKNOWN;

/**
 * 处理未被识别的操作
 */
@KVDBExecutor(command = COMMAND_UNKNOWN)
public class UnknowExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShowDatabasesExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("execute un support command:{}", request.getCommand().getName());

        return KVDBMessage.error(request.getId(), "un support command: " + request.getCommand().getName());
    }
}
