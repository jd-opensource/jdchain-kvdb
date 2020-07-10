package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.kvdb.server.Session;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchCommitExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchCommitExecutor.class);

    @Override
    public Message execute(Request request) {

        Session session = request.getSession();
        if (session.batchMode()) {
            LOGGER.debug("execute batch commit");
            try {
                Bytes[] params = request.getCommand().getParameters();
                if (null == params || params.length == 0) {
                    session.batchCommit();
                } else {
                    session.batchCommit(BytesUtils.toLong(params[0].toBytes()));
                }
                return KVDBMessage.success(request.getId());
            } catch (Exception e) {
                LOGGER.error("execute batch commit error", e);
                return KVDBMessage.error(request.getId(), e.toString());
            }
        } else {
            return KVDBMessage.error(request.getId(), "not in batch mode");
        }
    }
}
