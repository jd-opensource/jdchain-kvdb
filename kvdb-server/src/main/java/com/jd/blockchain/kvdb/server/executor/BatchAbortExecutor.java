package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 取消批处理
 */
public class BatchAbortExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchAbortExecutor.class);

    @Override
    public Message execute(Request request) {

        LOGGER.debug("execute begin batch");

        try {
            request.getSession().batchAbort();
            return KVDBMessage.success(request.getId());
        } catch (Exception e) {
            LOGGER.debug("execute batch abort error", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }

    }
}
