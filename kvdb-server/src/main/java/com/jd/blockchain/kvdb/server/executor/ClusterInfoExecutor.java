package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterInfoExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            LOGGER.debug("execute cluster sync");
            ClusterInfo[] infos = request.getServerContext().getClusterInfo();
            Bytes[] clusters = new Bytes[infos.length];
            for (int i = 0; i < infos.length; i++) {
                clusters[i] = new Bytes(BinaryProtocol.encode(infos[i], ClusterInfo.class));
            }
            return KVDBMessage.success(request.getId(), clusters);
        } catch (Exception e) {
            LOGGER.error("execute cluster sync", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
