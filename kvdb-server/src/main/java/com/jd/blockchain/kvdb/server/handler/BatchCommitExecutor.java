package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.kvdb.server.Session;
import org.rocksdb.RocksDBException;
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
                session.batchCommit();
                return KVDBMessage.success(request.getId());
            } catch (RocksDBException e) {
                LOGGER.debug("execute batch commit error", e);
                return KVDBMessage.error(request.getId(), e.getMessage());
            }
        } else {
            return KVDBMessage.error(request.getId(), "not in batch mode");
        }
    }
}
