package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.client.partition.Partitioner;
import com.jd.blockchain.kvdb.client.partition.SimpleMurmur3HashPartitioner;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.wal.Entity;
import com.jd.blockchain.kvdb.protocol.proto.wal.KV;
import com.jd.blockchain.kvdb.protocol.proto.wal.KVItem;
import com.jd.blockchain.kvdb.protocol.proto.wal.WalEntity;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.wal.FileLogger;
import com.jd.blockchain.wal.Wal;
import com.jd.blockchain.wal.WalConfig;
import com.jd.blockchain.wal.WalDataCoder;
import com.jd.blockchain.wal.WalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    // 数据库名称
    private String dbName;
    // 集群节点数据库实例实际操作对象
    private KVDBSingle[] operators;
    // 是否处于批处理模式
    private Boolean batchMode = false;
    // 批处理数据
    private Map<Bytes, Bytes> batch = new HashMap<>();
    // wal
    private Wal<Entity> wal;
    // 是否需要执行wal redo操作
    private volatile boolean needRedo = false;

    public KVDBCluster(String db, NettyClient[] clients) throws KVDBException, IOException {
        if (null != clients && clients.length > 0) {
            this.dbName = db;
            operators = new KVDBSingle[clients.length];
            int i = 0;
            for (NettyClient client : clients) {
                operators[i] = new KVDBSingle(client);
                i++;
            }
            partition = new SimpleMurmur3HashPartitioner(operators.length);
            executor = Executors.newFixedThreadPool(operators.length);
            wal = new FileLogger(new WalConfig(db, 1, true), new WalDataCoder<Entity>() {

                @Override
                public byte[] encode(Entity entity) {
                    return BinaryProtocol.encode(entity, Entity.class);
                }

                @Override
                public Entity decode(byte[] bytes) {
                    return BinaryProtocol.decode(bytes, Entity.class);
                }
            });
            checkAndRedoWal();
        } else {
            throw new KVDBException("no cluster config present");
        }
    }

    private void checkAndRedoWal() {
        if (!needRedo) {
            return;
        } else {
            synchronized (wal) {
                try {
                    WalIterator<Entity> iterator = wal.forwardIterator();
                    while (iterator.hasNext()) {
                        Entity entity = iterator.next();
                        for (KV kv : entity.getKVs()) {
                            operators[partition.partition(kv.getKey())].put(new Bytes(kv.getKey()), new Bytes(kv.getValue()), false);
                        }
                    }
                    wal.checkpoint();
                    needRedo = false;
                } catch (Exception e) {
                    LOGGER.error("redo error in " + dbName, e);
                    throw new KVDBException("redo error in " + dbName);
                }
            }
        }
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        checkAndRedoWal();
        return operators[partition.partition(key)].exists(key);
    }

    /**
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        checkAndRedoWal();
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
            LOGGER.error("exists error", e);
            throw new KVDBException(e.getMessage());
        }
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        checkAndRedoWal();
        return operators[partition.partition(key)].get(key);
    }

    /**
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        checkAndRedoWal();
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
            LOGGER.error("get error", e);
            throw new KVDBException(e.getMessage());
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
     * @param aSync
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean put(Bytes key, Bytes value, boolean aSync) throws KVDBException {
        checkAndRedoWal();
        synchronized (batchMode) {
            if (batchMode) {
                batch.put(key, value);
            }
            return operators[partition.partition(key)].put(key, value, aSync);
        }
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        checkAndRedoWal();
        try {
            synchronized (batchMode) {
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
                batchMode = true;
                batch.clear();

                return true;

            }
        } catch (Exception e) {
            LOGGER.error("batch begin error!", e);
            throw new KVDBException(e.getMessage());
        }
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        checkAndRedoWal();
        try {
            synchronized (batchMode) {
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
                batchMode = false;
                batch.clear();

                return true;
            }
        } catch (Exception e) {
            LOGGER.error("batch abort error!", e);
            throw new KVDBException(e.getMessage());
        }
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        checkAndRedoWal();
        synchronized (batchMode) {
            synchronized (wal) {
                KVItem[] walkvs = new KVItem[batch.size()];
                int j = 0;
                for (Map.Entry<Bytes, Bytes> entry : batch.entrySet()) {
                    walkvs[j] = new KVItem(entry.getKey().toBytes(), entry.getValue().toBytes());
                    j++;
                }
                try {
                    wal.append(WalEntity.newPutEntity(walkvs));
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
                            throw new KVDBException("batch commit error");
                        }
                    }
                    wal.checkpoint();

                    return true;
                } catch (IOException e) {
                    needRedo = true;
                    LOGGER.error("wal error", e);
                    throw new KVDBException(e.getMessage());
                } catch (KVDBException e) {
                    needRedo = true;
                    LOGGER.error("batch commit error!", e);
                    throw e;
                } catch (Exception e) {
                    needRedo = true;
                    LOGGER.error("batch commit error!", e);
                    throw new KVDBException(e.getMessage());
                } finally {
                    batchMode = false;
                    batch.clear();
                }
            }
        }
    }

    @Override
    public boolean batchCommit(long size) throws KVDBException {
        checkAndRedoWal();
        synchronized (batchMode) {
            synchronized (wal) {
                try {
                    if (size == batch.size()) {
                        throw new KVDBException("batch commit size wrong, expected: " + size + ", actually: " + batch.size());
                    }
                    KVItem[] walkvs = new KVItem[batch.size()];
                    int[] ps = new int[partition.getPartitionCount()];
                    int j = 0;
                    for (Map.Entry<Bytes, Bytes> entry : batch.entrySet()) {
                        ps[partition.partition(entry.getKey())]++;
                        walkvs[j] = new KVItem(entry.getKey().toBytes(), entry.getValue().toBytes());
                        j++;
                    }

                    wal.append(WalEntity.newPutEntity(walkvs));
                    CompletionService<ExecuteResultInBatch<Boolean>> completionService = new ExecutorCompletionService<>(executor);
                    for (int i = 0; i < operators.length; i++) {
                        final int index = i;
                        completionService.submit(() -> {
                            try {
                                return new ExecuteResultInBatch(index, operators[index].batchCommit(ps[index]));
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
                            throw new KVDBException("batch commit error");
                        }
                    }
                    wal.checkpoint();

                    return true;
                } catch (IOException e) {
                    needRedo = true;
                    LOGGER.error("wal error", e);
                    throw new KVDBException(e.getMessage());
                } catch (KVDBException e) {
                    needRedo = true;
                    LOGGER.error("batch commit error!", e);
                    throw e;
                } catch (Exception e) {
                    needRedo = true;
                    LOGGER.error("batch commit error!", e);
                    throw new KVDBException(e.getMessage());
                } finally {
                    batchMode = false;
                    batch.clear();
                }
            }
        }
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
        private KVDBException exception;

        private ExecuteResultInBatch() {
        }

        private ExecuteResultInBatch(int index, E data) {
            this.index = index;
            this.data = data;
        }

        private ExecuteResultInBatch(int index, KVDBException e) {
            this.index = index;
            this.exception = e;
        }

        public boolean success() {
            return null != exception;
        }
    }
}
