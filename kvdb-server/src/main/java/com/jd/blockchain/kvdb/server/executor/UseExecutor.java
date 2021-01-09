package com.jd.blockchain.kvdb.server.executor;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.proto.DatabaseClusterInfo;
import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;

import utils.Bytes;
import utils.StringUtils;
import utils.io.BytesUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_USE;

/**
 * 切换数据库实例
 */
@KVDBExecutor(command = COMMAND_USE)
public class UseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(UseExecutor.class);

    /**
     * @param request
     * @return 返回实例单实例或集群信息
     */
    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute use database: {}", request.getSession().getId(), request.getId(), request.getCommand().getParameters());
        try {
            String db = BytesUtils.toString(request.getCommand().getParameters()[0].toBytes());
            if (StringUtils.isEmpty(db)) {

                return KVDBMessage.error(request.getId(), "db name empty");
            } else {

                return KVDBMessage.success(request.getId(),
                        new Bytes(BinaryProtocol.encode(request.getServerContext().setDB(request.getSession(), db), DatabaseClusterInfo.class)));
            }
        } catch (Exception e) {
            LOGGER.error("{}-{} execute use database error", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
