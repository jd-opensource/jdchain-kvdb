package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.kvdb.server.wal.WalEntity;
import com.jd.blockchain.utils.io.BytesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_DROP_DATABASE;

@KVDBExecutor(command = COMMAND_DROP_DATABASE)
public class DropDatabaseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DropDatabaseExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            String database = BytesUtils.toString(request.getCommand().getParameters()[0].toBytes());
            request.getServerContext().getWal().append(WalEntity.newDropDatabaseEntity(request.getId()));
            request.getServerContext().dropDatabase(database);

            return KVDBMessage.success(request.getId());
        } catch (Exception e) {
            LOGGER.error("execute create databases", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
