package com.jd.blockchain.kvdb.client;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.proto.wal.Entity;
import com.jd.blockchain.wal.FileLogger;
import com.jd.blockchain.wal.Wal;
import com.jd.blockchain.wal.WalConfig;
import com.jd.blockchain.wal.WalDataCoder;
import com.jd.blockchain.wal.WalIterator;

import utils.Bytes;
import utils.io.FileUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class KVDBClusterTest {

    private String dbName = "cluster";
    private KVDBCluster cluster;
    private MockSingle[] mockSingles = new MockSingle[]{
            spy(new MockSingle()),
            spy(new MockSingle()),
            spy(new MockSingle()),
            spy(new MockSingle())
    };
    private Wal<Entity> wal;
    private WalDataCoder<Entity> dataCoder = new WalDataCoder<Entity>() {

        @Override
        public byte[] encode(Entity entity) {
            return BinaryProtocol.encode(entity, Entity.class);
        }

        @Override
        public Entity decode(byte[] bytes) {
            return BinaryProtocol.decode(bytes);
        }
    };

    @Before
    public void startUp() throws IOException, KVDBException {
        cluster = new KVDBCluster(dbName, mockSingles);
        wal = new FileLogger(new WalConfig(dbName), dataCoder);
    }

    @After
    public void tearDown() {
        FileUtils.deleteFile(dbName);
    }

    @Test
    public void testWal() throws IOException, KVDBException, InterruptedException {
        // set kv no in batch
        CountDownLatch cdl1 = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        cluster.put(Bytes.fromInt(j), Bytes.fromInt(j));
                    } catch (KVDBException e) {
                        e.printStackTrace();
                    }
                }
                cdl1.countDown();
            }).start();
        }
        cdl1.await();
        // read wal
        WalIterator<Entity> iterator = wal.forwardIterator();
        Assert.assertFalse(iterator.hasNext());

        // set kv in batch
        AtomicInteger count = new AtomicInteger(0);
        cluster.batchBegin();
        CountDownLatch cdl2 = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        int k = count.incrementAndGet();
                        cluster.put(Bytes.fromInt(k), Bytes.fromInt(k));
                    } catch (KVDBException e) {
                        e.printStackTrace();
                    }
                }
                cdl2.countDown();
            }).start();
        }
        cdl2.await();
        cluster.batchCommit();

        Assert.assertTrue(iterator.hasNext());
        Entity entity = iterator.next();
        Assert.assertEquals(count.get(), entity.getKVs().length);
        Assert.assertFalse(iterator.hasNext());

        wal.close();
        wal = new FileLogger(new WalConfig(dbName), dataCoder);
        Assert.assertFalse(wal.forwardIterator().hasNext());
    }

    @Test
    public void testPartialFail() throws KVDBException, InterruptedException, IOException {
        AtomicInteger count = new AtomicInteger(0);
        cluster.batchBegin();
        CountDownLatch cdl = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        int k = count.incrementAndGet();
                        cluster.put(Bytes.fromInt(k), Bytes.fromInt(k));
                    } catch (KVDBException e) {
                        e.printStackTrace();
                    }
                }
                cdl.countDown();
            }).start();
        }
        cdl.await();
        // some operator failed
        when(mockSingles[0].batchCommit()).thenReturn(false);
        when(mockSingles[0].put(any(), any(), anyBoolean())).thenReturn(false);
        boolean exception = false;
        try { cluster.batchCommit(); } catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        WalIterator<Entity> iterator = wal.forwardIterator();
        Assert.assertTrue(iterator.hasNext());
        Entity entity = iterator.next();
        Assert.assertEquals(count.get(), entity.getKVs().length);
        Assert.assertFalse(iterator.hasNext());
        Bytes lastKey = Bytes.fromInt(count.get());
        try { cluster.exists(lastKey); } catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.get(lastKey);} catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.put(lastKey, lastKey);} catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.batchBegin();} catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.batchAbort();} catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.batchCommit();} catch (Exception e) { exception = true; } Assert.assertTrue(exception); exception = false;
        try { cluster.batchCommit(0);} catch (Exception e) { exception = true; } Assert.assertTrue(exception);
        wal.close();
        wal = new FileLogger(new WalConfig(dbName), dataCoder);
        Assert.assertTrue(wal.forwardIterator().hasNext());

        // recover
        when(mockSingles[0].put(any(), any(), anyBoolean())).thenReturn(true);
        Assert.assertTrue(cluster.exists(lastKey));
        wal.close();
        wal = new FileLogger(new WalConfig(dbName), dataCoder);
        Assert.assertFalse(wal.forwardIterator().hasNext());
    }

    @Test
    public void testCommitWithSize() throws KVDBException, InterruptedException, IOException {
        AtomicInteger count = new AtomicInteger(0);
        cluster.batchBegin();
        CountDownLatch cdl = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                for (int j = 0; j < 1000; j++) {
                    try {
                        int k = count.incrementAndGet();
                        cluster.put(Bytes.fromInt(k), Bytes.fromInt(k));
                    } catch (KVDBException e) {
                        e.printStackTrace();
                    }
                }
                cdl.countDown();
            }).start();
        }
        cdl.await();
        Assert.assertTrue(cluster.batchCommit(count.get()));
        wal = new FileLogger(new WalConfig(dbName), dataCoder);
        Assert.assertFalse(wal.forwardIterator().hasNext());
    }

}