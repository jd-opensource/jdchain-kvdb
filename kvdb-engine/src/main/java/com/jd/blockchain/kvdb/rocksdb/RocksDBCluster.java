package com.jd.blockchain.kvdb.rocksdb;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.proto.Entity;
import com.jd.blockchain.kvdb.proto.KV;
import com.jd.blockchain.kvdb.proto.KVItem;
import com.jd.blockchain.kvdb.proto.WalEntity;
import com.jd.blockchain.kvdb.wal.Iterator;
import com.jd.blockchain.kvdb.wal.RedoLog;
import com.jd.blockchain.kvdb.wal.RedoLogConfig;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.FileUtils;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RocksDBCluster extends KVDBInstance {

    public static final Logger LOGGER = LoggerFactory.getLogger(RocksDBCluster.class);

    private Partitioner partitioner;

    private String rootPath;

    private RedoLog wal;

    private RocksDBProxy[] dbPartitions;

    private ExecutorService executor;

    private RocksDBCluster(String rootPath, RocksDBProxy[] dbPartitions, ExecutorService executor) {
        this(rootPath, dbPartitions, executor, null);
    }

    private RocksDBCluster(String rootPath, RocksDBProxy[] dbPartitions, ExecutorService executor, RedoLog wal) {
        this.rootPath = rootPath;
        this.dbPartitions = dbPartitions;
        this.executor = executor;
        this.partitioner = new SimpleMurmur3HashPartitioner(dbPartitions.length);
        this.wal = wal;
    }

    public static RocksDBCluster open(String path, int partitions, RedoLogConfig config) throws RocksDBException {
        String rootPath = FileUtils.getFullPath(path);
        RocksDBProxy[] dbPartitions = new RocksDBProxy[partitions];
        for (int i = 0; i < partitions; i++) {
            String partitionPath = rootPath + File.separator + "parti-" + i;
            FileUtils.makeDirectory(partitionPath);
            dbPartitions[i] = RocksDBProxy.open(partitionPath);
        }

        ExecutorService executor = Executors.newFixedThreadPool(dbPartitions.length);

        try {
            RocksDBCluster instance;
            if (null == config || config.isWalDisable()) {
                instance = new RocksDBCluster(rootPath, dbPartitions, executor);
            } else {
                instance = new RocksDBCluster(rootPath, dbPartitions, executor, new RedoLog(rootPath, config.getWalFlush()));
            }

            instance.redo();

            return instance;
        } catch (IOException e) {
            throw new RocksDBException(e.toString());
        }
    }

    // 检查wal日志进行数据一致性检查
    private void redo() throws IOException, RocksDBException {
        if (wal != null) {
            LOGGER.debug("redo wal...");

            if (!wal.metaUpdated()) {
                long lsn = wal.getMeta().getLsn();
                long position = wal.position(lsn);
                Iterator iterator = wal.entityIterator(position < 0 ? 0 : position);
                while (iterator.hasNext()) {
                    Entity e = iterator.next();
                    if (e.getLsn() > lsn) {
                        LOGGER.debug("redo {} {}", rootPath, e.getLsn());
                        WriteBatch[] batchs = new WriteBatch[dbPartitions.length];
                        for (KV kv : e.getKVs()) {
                            int index = partitioner.partition(kv.getKey());
                            if (null == batchs[index]) {
                                batchs[index] = new WriteBatch();
                            }
                            batchs[index].put(kv.getKey(), kv.getValue());
                        }
                        for (int i = 0; i < dbPartitions.length; i++) {
                            if (batchs[i] != null && batchs[i].getDataSize() > 0) {
                                dbPartitions[i].write(batchs[i]);
                            }
                        }
                        lsn = e.getLsn();
                    }
                }

                // update meta
                wal.updateMeta(lsn);
            }
            LOGGER.info("redo wal complete");
        }
    }

    @Override
    public synchronized void set(byte[] key, byte[] value) throws RocksDBException {
        long lsn = -1;
        if (null != wal) {
            try {
                lsn = wal.append(WalEntity.newPutEntity());
            } catch (IOException e) {
                throw new RocksDBException(e.toString());
            }
        }
        int pid = partitioner.partition(key);
        dbPartitions[pid].set(key, value);
        if (lsn > 0) {
            try {
                wal.updateMeta(lsn);
            } catch (IOException e) {
                throw new RocksDBException(e.toString());
            }
        }
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        int pid = partitioner.partition(key);
        return dbPartitions[pid].get(key);
    }

    @Override
    public synchronized void close() {
        if (null != wal) {
            try {
                wal.close();
            } catch (IOException e) {
                LOGGER.error("Error occurred while closing wal[" + rootPath + "]", e);
            }
        }
        if (dbPartitions != null) {
            try {
                executor.shutdown();
            } catch (Exception e) {
                LOGGER.error("Error occurred while closing rocksdb cluster[" + rootPath + "]", e);
            }

            for (int i = 0; i < dbPartitions.length; i++) {
                dbPartitions[i].close();
            }

            dbPartitions = null;
        }
    }

    @Override
    public synchronized void drop() {
        if (null != wal) {
            try {
                wal.close();
            } catch (IOException e) {
                LOGGER.error("Error occurred while closing wal[" + rootPath + "]", e);
            }
        }
        if (dbPartitions != null) {
            RocksDBProxy[] dbs = dbPartitions;

            close();

            for (int i = 0; i < dbs.length; i++) {
                dbs[i].drop();
            }

            try {
                FileUtils.deleteFile(rootPath);
            } catch (Exception e) {
                LOGGER.error("Error occurred while dropping rocksdb cluster[" + rootPath + "]", e);
            }
        }

    }

    @Override
    public synchronized void batchSet(Map<Bytes, byte[]> kvs) throws RocksDBException {
        if (null == kvs || kvs.size() == 0) {
            return;
        }
        Map<Bytes, byte[]>[] batches = new HashMap[dbPartitions.length];
        KVItem[] walkvs = new KVItem[kvs.size()];
        int j = 0;
        for (Map.Entry<Bytes, byte[]> entry : kvs.entrySet()) {
            int index = partitioner.partition(entry.getKey().toBytes());
            if (null == batches[index]) {
                batches[index] = new HashMap<>();
            }
            batches[index].put(entry.getKey(), entry.getValue());
            walkvs[j] = new KVItem(entry.getKey().toBytes(), entry.getValue());
            j++;
        }

        try {
            long lsn = -1;
            if (null != wal) {
                lsn = wal.append(WalEntity.newPutEntity(walkvs));
            }
            // TODO 多线程提交不同分片存在数据不一致问题
            CompletionService<Boolean> completionService = new ExecutorCompletionService<>(executor);
            int batchThreads = 0;
            for (int i = 0; i < batches.length; i++) {
                final int index = i;
                if (null != batches[i] && batches[i].size() > 0) {
                    completionService.submit(() -> {
                        try {
                            dbPartitions[index].batchSet(batches[index]);
                            return true;
                        } catch (Exception e) {
                            LOGGER.error("KVWrite batch task error! --" + e.getMessage(), e);
                            return false;
                        }
                    });

                    batchThreads++;
                }
            }
            for (int i = 0; i < batchThreads; i++) {
                if (!completionService.take().get()) {
                    LOGGER.error("Sub thread batch commit error");
                    throw new RocksDBException("Sub thread batch commit error");
                }
            }
            if (lsn > 0) {
                wal.updateMeta(lsn);
            }
        } catch (Exception e) {
            LOGGER.error("KVWrite batch commit error! --" + e.getMessage(), e);
            throw new RocksDBException(e.toString());
        }
    }
}
