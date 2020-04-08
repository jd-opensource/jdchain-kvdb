package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ShowDatabasesExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShowDatabasesExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            LOGGER.debug("execute show databases");
            Map<String, KVDBInstance> dbs = request.getServerContext().getDBs();
            Bytes[] names = new Bytes[dbs.size()];
            int i = 0;
            for (String name : dbs.keySet()) {
                names[i] = Bytes.fromString(name);
                i++;
            }
            return KVDBMessage.success(request.getId(), names);
        } catch (Exception e) {
            LOGGER.error("execute show databases", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
