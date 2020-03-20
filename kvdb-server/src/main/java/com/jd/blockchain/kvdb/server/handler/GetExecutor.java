package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetExecutor.class);

    @Override
    public Message execute(Request request) {

        try {
            boolean batch = request.getSession().batchMode();
            Bytes[] keys = request.getCommand().getParameters();
            Bytes[] values = new Bytes[keys.length];
            for (int i = 0; i < keys.length; i++) {
                final Bytes key = keys[i];
                byte[] value;
                if (!batch) {
                    value = request.getSession().getDB().get(key.toBytes());
                } else {
                    value = request.getSession().doInBatch((wb) -> wb.get(key));
                    if (null == value) {
                        value = request.getSession().getDB().get(key.toBytes());
                    }
                }
                LOGGER.debug("execute get, key:{}, value:{}", BytesUtils.toString(key.toBytes()), value);
                if (null != value) {
                    values[i] = new Bytes(value);
                } else {
                    values[i] = null;
                }
            }
            return KVDBMessage.success(request.getId(), values);
        } catch (RocksDBException e) {
            LOGGER.error("execute get error", e);
            return KVDBMessage.error(request.getId(), e.getMessage());
        }
    }
}
