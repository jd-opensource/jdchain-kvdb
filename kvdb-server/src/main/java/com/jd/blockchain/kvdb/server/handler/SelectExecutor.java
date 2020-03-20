package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            int db = BytesUtils.toInt(request.getCommand().getParameters()[0].toBytes());
            LOGGER.debug("execute select, db:{}", db);
            if (db < 0 || db > request.getServerContext().dbSize() - 1) {
                return KVDBMessage.error(request.getId(), "index out of bounds, you can input values in [0, " + (request.getServerContext().dbSize() - 1) + "]");
            } else {
                request.getSession().setDB(request.getServerContext().getDB(db));
                return KVDBMessage.success(request.getId());
            }
        } catch (Exception e) {
            LOGGER.error("execute select error", e);
            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
