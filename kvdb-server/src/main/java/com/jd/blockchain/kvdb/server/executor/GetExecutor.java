package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_GET;

@KVDBExecutor(command = COMMAND_GET)
public class GetExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute get: {}", request.getSession().getId(), request.getId(), request.getCommand().getParameters());
        try {
            if (null == request.getSession().getDBInstance()) {
                return KVDBMessage.error(request.getId(), "no database selected");
            }

            return KVDBMessage.success(request.getId(), request.getSession().get(request.getCommand().getParameters()));
        } catch (Exception e) {
            LOGGER.error("{}-{} execute get error", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
