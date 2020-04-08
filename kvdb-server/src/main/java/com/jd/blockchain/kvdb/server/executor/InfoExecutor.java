package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.Info;
import com.jd.blockchain.kvdb.protocol.KVDBInfo;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.kvdb.server.ServerContext;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfoExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InfoExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            LOGGER.debug("execute info");
            KVDBInfo info = new KVDBInfo();
            ServerContext context = request.getServerContext();
            info.setClusterMode(context.getConfig().isClusterMode());
            if (info.isClusterMode()) {
                info.setCluster(context.getConfig().getClusterInfoList());
            }
            return KVDBMessage.success(request.getId(), new Bytes(BinaryProtocol.encode(info, Info.class)));
        } catch (Exception e) {
            LOGGER.error("execute info", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
