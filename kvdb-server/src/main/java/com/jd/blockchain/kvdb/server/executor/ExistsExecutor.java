package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;

import utils.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_EXISTS;

/**
 * 存在性查询
 */
@KVDBExecutor(command = COMMAND_EXISTS)
public class ExistsExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExistsExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute exists: {}", request.getSession().getId(), request.getId(), request.getCommand().getParameters());
        try {
            if (null == request.getSession().getDBInstance()) {
                return KVDBMessage.error(request.getId(), "no database selected");
            }

            boolean[] es = request.getSession().exists(request.getCommand().getParameters());
            Bytes[] values = new Bytes[es.length];
            for (int i = 0; i < es.length; i++) {
                values[i] = es[i] ? Bytes.fromInt(1) : Bytes.fromInt(0);
            }

            return KVDBMessage.success(request.getId(), values);
        } catch (Exception e) {
            LOGGER.error("{}-{} execute exists error", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
