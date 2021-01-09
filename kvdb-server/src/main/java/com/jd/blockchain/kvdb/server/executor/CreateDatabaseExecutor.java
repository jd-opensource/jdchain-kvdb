package com.jd.blockchain.kvdb.server.executor;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.proto.DatabaseBaseInfo;
import com.jd.blockchain.kvdb.protocol.proto.Message;
import com.jd.blockchain.kvdb.protocol.proto.impl.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.kvdb.server.config.DBInfo;

import utils.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.jd.blockchain.kvdb.protocol.proto.Command.COMMAND_CREATE_DATABASE;

@KVDBExecutor(command = COMMAND_CREATE_DATABASE)
public class CreateDatabaseExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateDatabaseExecutor.class);

    @Override
    public Message execute(Request request) {
        LOGGER.debug("{}-{} execute create database: {}", request.getSession().getId(), request.getId(), request.getCommand().getParameters());
        try {
            // name 必填，rootDir 和 partitions 可选
            DatabaseBaseInfo param = BinaryProtocol.decode(request.getCommand().getParameters()[0].toBytes(), DatabaseBaseInfo.class);
            DBInfo dbInfo = new DBInfo();
            dbInfo.setEnable(true);
            if (StringUtils.isEmpty(param.getName().trim())) {

                return KVDBMessage.error(request.getId(), "database name empty");
            }
            dbInfo.setName(param.getName().trim());
            if (StringUtils.isEmpty(param.getRootDir().trim())) {
                dbInfo.setDbRootdir(request.getServerContext().getConfig().getKvdbConfig().getDbsRootdir());
            } else {
                dbInfo.setDbRootdir(param.getRootDir().trim());
            }
            if (param.getPartitions() < 0) {

                return KVDBMessage.error(request.getId(), "partitions can not be negative");
            } else if (param.getPartitions() > 0) {
                dbInfo.setPartitions(param.getPartitions());
            } else {
                dbInfo.setPartitions(request.getServerContext().getConfig().getKvdbConfig().getDbsPartitions());
            }
            request.getServerContext().createDatabase(dbInfo);

            return KVDBMessage.success(request.getId());
        } catch (Exception e) {
            LOGGER.error("{}-{} execute create databases", request.getSession().getId(), request.getId(), e);

            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
