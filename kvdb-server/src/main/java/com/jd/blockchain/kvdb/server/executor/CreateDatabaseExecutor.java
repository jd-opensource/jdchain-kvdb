package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatabaseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatabaseExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            String db = BytesUtils.toString(request.getCommand().getParameters()[0].toBytes());
            LOGGER.debug("execute create databases, db:{}", db);
            if (StringUtils.isEmpty(db)) {
                return KVDBMessage.error(request.getId(), "db name empty");
            } else {
                request.getServerContext().createDB(db);
                return KVDBMessage.success(request.getId());
            }
        } catch (Exception e) {
            LOGGER.error("execute create databases", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
