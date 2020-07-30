package com.jd.blockchain.kvdb.rocksdb;

import com.jd.blockchain.kvdb.KVDBInstance;
import com.jd.blockchain.kvdb.KVWriteBatch;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocksDBProxy extends KVDBInstance {

    private static Logger LOGGER = LoggerFactory.getLogger(RocksDBProxy.class);

    private WriteOptions writeOptions;

    private RocksDB db;

    protected String path;

    public String getPath() {
        return path;
    }

    private RocksDBProxy(RocksDB db, String path) {
        this.db = db;
        this.path = path;
        this.writeOptions = initWriteOptions();

    }

    private static WriteOptions initWriteOptions() {
        WriteOptions options = new WriteOptions();
        // TODO 使用KVDB自实现WAL?
        options.setDisableWAL(true);
        options.setNoSlowdown(false);
        return options;
    }

    private static Options initDBOptions() {
        Cache cache = new LRUCache(512 * SizeUnit.MB, 64, false);

        final BlockBasedTableConfig tableOptions = new BlockBasedTableConfig()
                .setBlockCache(cache)
                .setMetadataBlockSize(4096)
                .setCacheIndexAndFilterBlocks(true) // 设置索引和布隆过滤器使用Block Cache内存
                .setCacheIndexAndFilterBlocksWithHighPriority(true)
                .setIndexType(IndexType.kTwoLevelIndexSearch) // 设置两级索引，控制索引占用内存
                .setPinTopLevelIndexAndFilter(false)
                .setBlockSize(4096)
                .setFilterPolicy(null) // 不设置布隆过滤器
                ;

        Options options = new Options()
                // 最多占用256 * 6 + 512 = 2G内存
                .setWriteBufferSize(256 * SizeUnit.MB)
                .setMaxWriteBufferNumber(6)
                .setMinWriteBufferNumberToMerge(2)
                .setMaxOpenFiles(100) // 控制最大打开文件数量，防止内存持续增加
                .setAllowConcurrentMemtableWrite(true) //允许并行Memtable写入
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableOptions)
                .setMaxBackgroundCompactions(5)
                .setMaxBackgroundFlushes(4);
        return options;
    }

    private static MutableColumnFamilyOptions initColumnFamilyOptions() {
        return MutableColumnFamilyOptions.builder()
                .setWriteBufferSize(32 * 1024 * 1024)
                .setMaxWriteBufferNumber(4)
                .build();
    }

    private static void initDB(RocksDB db) throws RocksDBException {
        ColumnFamilyHandle defaultColumnFamily = db.getDefaultColumnFamily();
        db.setOptions(defaultColumnFamily, initColumnFamilyOptions());
    }

    public static RocksDBProxy open(String path) throws RocksDBException {
        RocksDB db = RocksDB.open(initDBOptions(), path);

        initDB(db);

        return new RocksDBProxy(db, path);
    }

    @Override
    public byte[] get(byte[] key) throws RocksDBException {
        return db.get(key);
    }

    @Override
    public KVWriteBatch beginBatch() {
        return new RocksDBWriteBatch(db);
    }

    @Override
    public void set(byte[] key, byte[] value) throws RocksDBException {
        db.put(writeOptions, key, value);
    }

    private static class RocksDBWriteBatch implements KVWriteBatch {

        private RocksDB db;
        private WriteBatch batch;

        public RocksDBWriteBatch(RocksDB db) {
            this.db = db;
            this.batch = new WriteBatch();
        }

        @Override
        public void set(String key, String value) throws RocksDBException {
            set(BytesUtils.toBytes(key), BytesUtils.toBytes(value));
        }

        @Override
        public void set(byte[] key, byte[] value) throws RocksDBException {
            batch.put(key, value);
        }

        @Override
        public void commit() throws RocksDBException {
            db.write(new WriteOptions(), batch);
        }

    }

    @Override
    public void close() {
        if (db != null) {
            try {
                db.close();
            } catch (Exception e) {
                LOGGER.error("Error occurred while closing rocksdb[" + path + "]", e);
            } finally {
                db = null;
            }
        }
    }

    @Override
    public synchronized void drop() {
        if (db != null) {
            try {
                close();
                FileUtils.deleteFile(path);
            } catch (Exception e) {
                LOGGER.error("Error occurred while dropping rocksdb[" + path + "]", e);
            }
        }
    }

}
