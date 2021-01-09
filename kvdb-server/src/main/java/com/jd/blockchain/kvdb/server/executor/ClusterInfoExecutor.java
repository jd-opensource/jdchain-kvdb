package com.jd.blockchain.kvdb.server.executor;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.proto.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;

import utils.Bytes;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_CLUSTER_INFO;

@KVDBExecutor(command = COMMAND_CLUSTER_INFO)
public class ClusterInfoExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterInfoExecutor.class);

    @Override
    public Message execute(Request request) {
        try {
            LOGGER.debug("{}-{} execute cluster sync", request.getSession().getId(), request.getId());
            ClusterInfo info = request.getServerContext().getClusterInfo();

            return KVDBMessage.success(request.getId(), new Bytes(BinaryProtocol.encode(info, ClusterInfo.class)));
        } catch (Exception e) {
            LOGGER.error("{}-{}, execute cluster sync", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
