package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchBeginExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchBeginExecutor.class);

    @Override
    public Message execute(Request request) {

        LOGGER.debug("execute begin batch");

        try {
            request.getSession().batchBegin();
            return KVDBMessage.success(request.getId());
        } catch (RocksDBException e) {
            LOGGER.debug("execute batch begin error", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
