package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;

import utils.Bytes;
import utils.io.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_BATCH_COMMIT;

@KVDBExecutor(command = COMMAND_BATCH_COMMIT)
public class BatchCommitExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCommitExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute batch commit", request.getSession().getId(), request.getId());
        try {
            Bytes[] params = request.getCommand().getParameters();
            if (null == params || params.length == 0) {
                request.getSession().batchCommit();
            } else {
                request.getSession().batchCommit(BytesUtils.toLong(params[0].toBytes()));
            }

            return KVDBMessage.success(request.getId());
        } catch (Exception e) {
            LOGGER.error("{}-{} execute batch commit error", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
