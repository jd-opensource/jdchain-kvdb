package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.StringUtils;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDatabaseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatabaseExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            Bytes[] params = request.getCommand().getParameters();
            String db = BytesUtils.toString(params[0].toBytes());
            int partitions = request.getServerContext().getConfig().getKvdbConfig().getDbsPartitions();
            if (params.length > 1) {
                partitions = BytesUtils.toInt(params[1].toBytes());
            }
            // TODO 根目录
            LOGGER.debug("execute create databases, db:{}", db);
            if (StringUtils.isEmpty(db)) {
                return KVDBMessage.error(request.getId(), "db name empty");
            } else {
                request.getServerContext().createDatabase(db);
                return KVDBMessage.success(request.getId());
            }
        } catch (Exception e) {
            LOGGER.error("execute create databases", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
