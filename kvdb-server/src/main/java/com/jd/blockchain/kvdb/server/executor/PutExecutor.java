package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_PUT;

/**
 * 设置键值对
 */
@KVDBExecutor(command = COMMAND_PUT)
public class PutExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute put: {}", request.getSession().getId(), request.getId(), request.getCommand().getParameters());
        try {
            if (null == request.getSession().getDBInstance()) {
                return KVDBMessage.error(request.getId(), "no database selected");
            }

            Bytes[] params = request.getCommand().getParameters();
            if (params.length % 2 != 0) {
                return KVDBMessage.error(request.getId(), "keys and values must in pairs");
            }

            Map<Bytes, byte[]> kvs = new HashMap<>();
            for (int i = 0; i < params.length / 2; i = i + 2) {
                kvs.put(params[i], params[i + 1].toBytes());
            }

            request.getSession().put(kvs);

            return KVDBMessage.success(request.getId());
        } catch (Exception e) {
            LOGGER.error("{}-{} execute put error", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
