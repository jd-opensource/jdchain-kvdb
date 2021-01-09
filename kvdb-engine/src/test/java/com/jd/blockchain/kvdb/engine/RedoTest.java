package com.jd.blockchain.kvdb.engine;

import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.engine.rocksdb.RocksDBProxy;

import utils.io.BytesUtils;
import utils.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class RedoTest {

    static final String dbPath = "dbs/test";
    static final String walPath = "dbs/kvdb.wal";

    static {
        RocksDB.loadLibrary();
    }

    @Before
    public void startUp() {
        FileUtils.makeDirectory(dbPath);
    }

    @After
    public void tearDown() {
        FileUtils.deleteFile("dbs", true);
    }

    private int setTestKVs(KVDBInstance instance) throws RocksDBException {
        for (int i = 0; i < 100; i++) {
            instance.set(BytesUtils.toBytes(i), BytesUtils.toBytes(i));
        }

        return 100;
    }

    private void resetCheckpoint() throws IOException {
        FileChannel fileChannel = FileChannel.open(Paths.get(walPath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 8);
        mb.put(BytesUtils.toBytes(8l));
        fileChannel.close();
    }

    @Test
    public void testSingle() throws RocksDBException, IOException {
        // without wal
        KVDBInstance instance = RocksDBProxy.open(dbPath);
        int lsn = setTestKVs(instance);
        for (int i = 0; i < lsn; i++) {
            Assert.assertArrayEquals(BytesUtils.toBytes(i), instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBProxy.open(dbPath);
        for (int i = 0; i < lsn; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();

        // with wal disabled
        instance = RocksDBProxy.open(dbPath, new Config(walPath, true, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBProxy.open(dbPath, new Config(walPath, true, -1));
        for (int i = 0; i < lsn; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();

        // with wal enabled
        instance = RocksDBProxy.open(dbPath, new Config(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        // reset wal checkpoint
        resetCheckpoint();
        instance = RocksDBProxy.open(dbPath, new Config(walPath, false, -1));
        for (int i = 0; i < lsn; i++) {
            Assert.assertArrayEquals(BytesUtils.toBytes(i), instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();
    }

    @Test
    public void testCluster() throws IOException, RocksDBException {
// without wal
        KVDBInstance instance = RocksDBCluster.open(dbPath, 4);
        int lsn = setTestKVs(instance);
        for (int i = 0; i < lsn; i++) {
            Assert.assertArrayEquals(BytesUtils.toBytes(i), instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBCluster.open(dbPath, 4);
        for (int i = 0; i < lsn; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();

        // with wal disabled
        instance = RocksDBCluster.open(dbPath, 4, new Config(walPath, true, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        instance = RocksDBCluster.open(dbPath, 4, new Config(walPath, true, -1));
        for (int i = 0; i < lsn; i++) {
            Assert.assertNull(instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();

        // with wal enabled
        instance = RocksDBCluster.open(dbPath, 4, new Config(walPath, false, -1));
        lsn = setTestKVs(instance);
        instance.close();
        FileUtils.deleteFile(dbPath, true);
        // reset wal checkpoint
        resetCheckpoint();
        instance = RocksDBCluster.open(dbPath, 4, new Config(walPath, false, -1));
        for (int i = 0; i < lsn; i++) {
            Assert.assertArrayEquals(BytesUtils.toBytes(i), instance.get(BytesUtils.toBytes(i)));
        }
        instance.close();
    }

}
