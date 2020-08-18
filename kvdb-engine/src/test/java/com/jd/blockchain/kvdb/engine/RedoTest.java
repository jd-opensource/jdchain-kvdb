package com.jd.blockchain.kvdb.engine;

import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBProxy;
import com.jd.blockchain.kvdb.engine.wal.CheckpointWriter;
import com.jd.blockchain.kvdb.engine.wal.RedoLog;
import com.jd.blockchain.kvdb.engine.wal.RedoLogConfig;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Paths;

public class RedoTest {

    static final String dbPath = "dbs/test";
    static final String walPath = "dbs";

    static {
        RocksDB.loadLibrary();
    }

    @Before
    public void startUp() {
        FileUtils.makeDirectory(dbPath);
    }

    @After
    public void tearDown() {
        FileUtils.deleteFile(walPath, true);
    }

    @Test
    public void testSingle() throws RocksDBException, IOException {
        // without wal
        KVDBInstance instance = RocksDBProxy.open(dbPath);
        long lsn = setTestKVs(instance);
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBProxy.open(dbPath);
        Assert.assertNull(instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal disabled
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, true, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, true, -1));
        Assert.assertNull(instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal enabled
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        // delete meta file
        FileUtils.deleteFile(Paths.get(walPath, RedoLog.META_FILE).toString(), true);
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, false, -1));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal enabled, reset meta file
        FileUtils.deleteFile(walPath, true);
        FileUtils.makeDirectory(dbPath);
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        CheckpointWriter writer = new CheckpointWriter(Paths.get(walPath, RedoLog.META_FILE));
        Assert.assertEquals(lsn, writer.read().longValue());
        writer.write(lsn - 2);
        instance = RocksDBProxy.open(dbPath, new RedoLogConfig(walPath, false, -1));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn - 1), instance.get(BytesUtils.toBytes(lsn - 1)));
        for (int i = 1; i <= lsn - 2; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes((long) i)));
        }
        instance.close();
    }

    private long setTestKVs(KVDBInstance instance) throws RocksDBException, IOException {
        for (int i = 1; i <= 100; i++) {
            instance.set(BytesUtils.toBytes((long) i), BytesUtils.toBytes((long) i));
        }

        return 100;
    }

    @Test
    public void testCluster() throws RocksDBException, IOException {
// without wal
        KVDBInstance instance = RocksDBCluster.open(dbPath, 4);
        long lsn = setTestKVs(instance);
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBCluster.open(dbPath, 4);
        Assert.assertNull(instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal disabled
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, true, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, true, -1));
        Assert.assertNull(instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal enabled
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        // delete meta file
        FileUtils.deleteFile(Paths.get(walPath, RedoLog.META_FILE).toString(), true);
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, false, -1));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        instance.close();

        // with wal enabled, reset meta file
        FileUtils.deleteFile(walPath, true);
        FileUtils.makeDirectory(dbPath);
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        CheckpointWriter writer = new CheckpointWriter(Paths.get(walPath, RedoLog.META_FILE));
        Assert.assertEquals(lsn, writer.read().longValue());
        writer.write(lsn - 2);
        instance = RocksDBCluster.open(dbPath, 4, new RedoLogConfig(walPath, false, -1));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn), instance.get(BytesUtils.toBytes(lsn)));
        Assert.assertArrayEquals(BytesUtils.toBytes(lsn - 1), instance.get(BytesUtils.toBytes(lsn - 1)));
        for (int i = 1; i <= lsn - 2; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes((long) i)));
        }
        instance.close();
    }

}
