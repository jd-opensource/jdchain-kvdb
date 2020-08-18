package com.jd.blockchain.kvdb.engine.wal;

import com.jd.blockchain.kvdb.engine.proto.Entity;
import com.jd.blockchain.kvdb.engine.proto.KVItem;
import com.jd.blockchain.kvdb.engine.proto.WalEntity;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class RedoLogTest {

    RedoLog wal;

    @Before
    public void startUp() throws IOException {
        FileUtils.makeDirectory("kvdb");
        wal = new RedoLog("kvdb", 1);
    }

    @After
    public void tearDown() throws IOException {
        wal.close();
        FileUtils.deleteFile("kvdb", true);
    }

    @Test
    public void latestLsn() throws IOException {
        Assert.assertEquals(-1l, wal.latestLsn());

        Entity entity = WalEntity.newPutEntity();
        long lsn = wal.append(entity);
        Assert.assertEquals(1, lsn);
        Assert.assertEquals(1, wal.latestLsn());
    }

    @Test
    public void append() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] k = BytesUtils.toBytes(i);
            Entity entity = WalEntity.newPutEntity(new KVItem(k, k));
            long lsn = wal.append(entity);
            Assert.assertEquals(i + 1, lsn);
        }
    }

    @Test
    public void clear() throws IOException {
        Assert.assertEquals(0, wal.size());

        append();
        Assert.assertNotEquals(0, wal.size());

        wal.clear();
        Assert.assertEquals(0, wal.size());
    }

    @Test
    public void get() throws IOException {
        Assert.assertNull(wal.get(0));
        append();
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, BytesUtils.toInt(wal.get(i * wal.size() / 100).getKVs()[0].getValue()));
        }
    }

    @Test
    public void query() throws IOException {
        for (int i = 0; i < 100; i++) {
            Assert.assertNull(wal.query(i));
        }
        append();
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i, BytesUtils.toInt(wal.query(i + 1).getKVs()[0].getValue()));
        }
    }

    @Test
    public void position() throws IOException {
        append();
        for (int i = 0; i < 100; i++) {
            Assert.assertEquals(i * wal.size() / 100, wal.position(i + 1));
        }
    }

    @Test
    public void updated() throws IOException {
        Assert.assertTrue(wal.updated());

        wal.setCheckpoint(1);
        Assert.assertFalse(wal.updated());
    }

    @Test
    public void getCheckpoint() throws IOException {
        Assert.assertEquals(-1l, wal.getCheckpoint());

        append();

        // 从1开始严格递增
        wal.setCheckpoint(100);
        Assert.assertEquals(-1l, wal.getCheckpoint());

        wal.setCheckpoint(1);
        Assert.assertEquals(1l, wal.getCheckpoint());

        wal.setCheckpoint(2);
        Assert.assertEquals(2l, wal.getCheckpoint());
    }

    @Test
    public void entityIterator() throws IOException {
        Iterator iterator = wal.entityIterator(0);
        Assert.assertFalse(iterator.hasNext());

        append();
        iterator = wal.entityIterator(0);
        Assert.assertTrue(iterator.hasNext());
        int i = 0;
        while (iterator.hasNext()) {
            Assert.assertEquals(i++, BytesUtils.toInt(iterator.next().getKVs()[0].getValue()));
        }

        Assert.assertEquals(100, i);
    }
}
