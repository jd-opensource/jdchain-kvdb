package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.client.partition.Partitioner;
import com.jd.blockchain.kvdb.client.partition.SimpleMurmur3HashPartitioner;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 集群数据库操作
 */
public class KVDBCluster implements KVDBOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBCluster.class);
    // 线程池，用于异步多数据库连接的数据操作
    private ExecutorService executor;
    // 服务器分片器
    private Partitioner partition;
    // 集群节点数据库实例实际操作对象
    private KVDBSingle[] operators;

    public KVDBCluster(NettyClient[] clients) throws KVDBException {
        if (null != clients && clients.length > 0) {
            operators = new KVDBSingle[clients.length];
            int i = 0;
            for (NettyClient client : clients) {
                operators[i] = new KVDBSingle(client);
                i++;
            }
            partition = new SimpleMurmur3HashPartitioner(operators.length);
            executor = Executors.newFixedThreadPool(operators.length);
        } else {
            throw new KVDBException("no cluster config present");
        }
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        return operators[partition.partition(key)].exists(key);
    }

    /**
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        try {
            CompletionService<ExecuteResultInBatch<Boolean>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < keys.length; i++) {
                final int index = i;
                completionService.submit(() -> {
                    try {
                        return new ExecuteResultInBatch(index, operators[partition.partition(keys[index])].exists(keys[index]));
                    } catch (KVDBException e) {
                        return new ExecuteResultInBatch(index, e);
                    }
                });
            }
            boolean[] results = new boolean[keys.length];
            for (int i = 0; i < keys.length; i++) {
                ExecuteResultInBatch<Boolean> result = completionService.take().get();
                if (!result.success()) {
                    throw result.exception;
                }
                results[result.index] = result.data;
            }
            return results;
        } catch (Exception e) {
            LOGGER.error("Exists command error! --" + e.getMessage(), e);
            throw new KVDBException("Exists command error");
        }
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return operators[partition.partition(key)].get(key);
    }

    /**
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        try {
            CompletionService<ExecuteResultInBatch<Bytes>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < keys.length; i++) {
                final int index = i;
                completionService.submit(() -> {
                    try {
                        return new ExecuteResultInBatch(index, operators[partition.partition(keys[index])].get(keys[index]));
                    } catch (KVDBException e) {
                        return new ExecuteResultInBatch(index, e);
                    }
                });
            }
            Bytes[] results = new Bytes[keys.length];
            for (int i = 0; i < keys.length; i++) {
                ExecuteResultInBatch<Bytes> result = completionService.take().get();
                if (!result.success()) {
                    throw result.exception;
                }
                results[result.index] = result.data;
            }
            return results;
        } catch (Exception e) {
            LOGGER.error("Get command error! --" + e.getMessage(), e);
            throw new KVDBException("Get command error");
        }
    }

    /**
     * @param key
     * @param value
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean put(Bytes key, Bytes value) throws KVDBException {
        return put(key, value, false);
    }

    /**
     * @param key
     * @param value
     * @param inBatch
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean put(Bytes key, Bytes value, boolean inBatch) throws KVDBException {
        return operators[partition.partition(key)].put(key, value, inBatch);
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        try {
            CompletionService<ExecuteResultInBatch<Boolean>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < operators.length; i++) {
                final int index = i;
                completionService.submit(() -> {
                    try {
                        return new ExecuteResultInBatch(index, operators[index].batchBegin());
                    } catch (KVDBException e) {
                        return new ExecuteResultInBatch(index, e);
                    }
                });
            }
            for (int i = 0; i < operators.length; i++) {
                ExecuteResultInBatch<Boolean> result = completionService.take().get();
                if (!result.success()) {
                    throw result.exception;
                }
                if (!result.data) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("BatchBegin command error! --" + e.getMessage(), e);
            throw new KVDBException("BatchBegin command error");
        }
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        try {
            CompletionService<ExecuteResultInBatch<Boolean>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < operators.length; i++) {
                final int index = i;
                completionService.submit(() -> {
                    try {
                        return new ExecuteResultInBatch(index, operators[index].batchAbort());
                    } catch (KVDBException e) {
                        return new ExecuteResultInBatch(index, e);
                    }
                });
            }
            for (int i = 0; i < operators.length; i++) {
                ExecuteResultInBatch<Boolean> result = completionService.take().get();
                if (!result.success()) {
                    throw result.exception;
                }
                if (!result.data) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("BatchAbort command error! --" + e.getMessage(), e);
            throw new KVDBException("BatchAbort command error");
        }
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        try {
            CompletionService<ExecuteResultInBatch<Boolean>> completionService = new ExecutorCompletionService<>(executor);
            for (int i = 0; i < operators.length; i++) {
                final int index = i;
                completionService.submit(() -> {
                    try {
                        return new ExecuteResultInBatch(index, operators[index].batchCommit());
                    } catch (KVDBException e) {
                        return new ExecuteResultInBatch(index, e);
                    }
                });
            }
            for (int i = 0; i < operators.length; i++) {
                ExecuteResultInBatch<Boolean> result = completionService.take().get();
                if (!result.success()) {
                    throw result.exception;
                }
                if (!result.data) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOGGER.error("BatchCommit command error! --" + e.getMessage(), e);
            throw new KVDBException("BatchCommit command error");
        }
    }

    @Override
    public boolean batchCommit(long size) throws KVDBException {
        // TODO 带有size参数的批量操作对于多实例无效
        return batchBegin();
    }

    @Override
    public void close() {
        for (KVDBSingle operator : operators) {
            operator.close();
        }
        if (null != executor && !executor.isShutdown()) {
            executor.shutdown();
        }
    }

    class ExecuteResultInBatch<E> {
        private int index;
        private E data;
        private Exception exception;

        private ExecuteResultInBatch() {
        }

        private ExecuteResultInBatch(int index, E data) {
            this.index = index;
            this.data = data;
        }

        private ExecuteResultInBatch(int index, Exception e) {
            this.index = index;
            this.exception = e;
        }

        public boolean success() {
            return null != exception;
        }
    }
}
