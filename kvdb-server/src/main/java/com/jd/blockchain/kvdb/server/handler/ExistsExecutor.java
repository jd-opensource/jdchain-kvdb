package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExistsExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExistsExecutor.class);

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
                LOGGER.debug("execute exists, key:{}, value:{}", BytesUtils.toString(keys[i].toBytes()), value);
                values[i] = null != value ? Bytes.fromInt(1) : Bytes.fromInt(0);
            }
            return KVDBMessage.success(request.getId(), values);
        } catch (RocksDBException e) {
            LOGGER.error("execute exists error", e);
            return KVDBMessage.error(request.getId(), e.toString());
        }
    }
}
