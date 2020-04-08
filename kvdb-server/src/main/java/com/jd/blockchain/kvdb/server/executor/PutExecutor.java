package com.jd.blockchain.kvdb.server.executor;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVWriteBatch;
import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.protocol.KVDBMessage;
import com.jd.blockchain.kvdb.server.Request;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PutExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutExecutor.class);

    @Override
    public Message execute(Request request) {

        boolean batch = request.getSession().batchMode();
        Bytes[] kvs = request.getCommand().getParameters();
        if (kvs.length % 2 != 0) {
            return KVDBMessage.error(request.getId(), "keys and values must in pairs");
        }

        if (batch) {
            try {
                for (int i = 0; i < kvs.length; ) {
                    LOGGER.debug("execute put in batch, key:{}, value:{}", BytesUtils.toString(kvs[i].toBytes()), kvs[i + 1].toBytes());
                    final Bytes key = kvs[i];
                    final Bytes value = kvs[i + 1];
                    request.getSession().doInBatch((wb) -> {
                        wb.put(key, value.toBytes());
                        return null;
                    });
                    i = i + 2;
                }
            } catch (RocksDBException e) {
                LOGGER.debug("execute put error", e);
                return KVDBMessage.error(request.getId(), e.toString());
            }
        } else {
            KVDBInstance db = request.getSession().getDBInstance();
            if (kvs.length == 2) {
                try {
                    LOGGER.debug("execute put, key:{}, value:{}", BytesUtils.toString(kvs[0].toBytes()), kvs[1].toBytes());
                    db.set(kvs[0].toBytes(), kvs[1].toBytes());
                } catch (Exception e) {
                    LOGGER.debug("execute put error", e);
                    return KVDBMessage.error(request.getId(), e.toString());
                }
            } else {
                KVWriteBatch wb = request.getSession().getDBInstance().beginBatch();
                try {
                    for (int i = 0; i < kvs.length; ) {
                        LOGGER.debug("execute put, key:{}, value:{}", BytesUtils.toString(kvs[i].toBytes()), kvs[i + 1].toBytes());
                        wb.set(kvs[i].toBytes(), kvs[i + 1].toBytes());
                        i = i + 2;
                    }
                    wb.commit();
                } catch (Exception e) {
                    LOGGER.debug("execute put error", e);
                    return KVDBMessage.error(request.getId(), e.toString());
                }
            }
        }

        return KVDBMessage.success(request.getId());
    }
}
