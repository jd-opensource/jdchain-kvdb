package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UseExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            String db = BytesUtils.toString(request.getCommand().getParameters()[0].toBytes());
            LOGGER.debug("execute use, db:{}", db);
            if (StringUtils.isEmpty(db)) {
                return KVDBMessage.error(request.getId(), "db name empty");
            } else {
                KVDBInstance kvdbInstance = request.getServerContext().getDB(db);
                if (null != kvdbInstance) {
                    request.getSession().setDB(db, kvdbInstance);
                    return KVDBMessage.success(request.getId());
                } else {
                    return KVDBMessage.error(request.getId(), "db not exists");
                }
            }
        } catch (Exception e) {
            LOGGER.error("execute use error", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
