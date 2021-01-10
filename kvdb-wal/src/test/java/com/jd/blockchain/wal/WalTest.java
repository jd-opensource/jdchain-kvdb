package com.jd.blockchain.wal;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import utils.io.BytesUtils;

import java.io.IOException;
import java.nio.file.Files;

public class WalTest {

    private WalConfig config;
    private FileLogger<byte[]> wal;

    @Before
    public void startUp() throws IOException {
        config = new WalConfig(Files.createTempFile("kvdb", ".wal").toFile().getAbsolutePath(), 1, true);
        wal = new FileLogger(config, new WalDataCoder<byte[]>() {
            @Override
            public byte[] encode(byte[] data) {
                return data;
            }

            @Override
            public byte[] decode(byte[] data) {
                return data;
            }
        });
    }

    @After
    public void tearDown() throws IOException {
        wal.close();
    }

    @Test
    public void testTruncateExisting() throws IOException {
        WalDataCoder dataCoder = new WalDataCoder<Integer>() {
            @Override
            public byte[] encode(Integer data) {
                return BytesUtils.toBytes(data);
            }

            @Override
            public Integer decode(byte[] data) {
                return BytesUtils.toInt(data);
            }
        };

        wal.append(BytesUtils.toBytes(1));
        Assert.assertTrue(wal.forwardIterator().hasNext());
        Assert.assertTrue(wal.backwardIterator().hasNext());
        wal.close();

        config.setTruncateExisting(true);
        wal = new FileLogger(config, dataCoder);
        Assert.assertFalse(wal.forwardIterator().hasNext());
        Assert.assertFalse(wal.backwardIterator().hasNext());
    }


    @Test
    public void append() throws IOException {
        byte[][] datas = new byte[100][];
        for (int i = 0; i < 100; i++) {
            datas[i] = BytesUtils.toBytes(i);
        }
        long size = wal.size();
        for (byte[] data : datas) {
            size += wal.append(data);
        }
        Assert.assertEquals(size, wal.size());
    }

    @Test
    public void checkpoint() throws IOException {
        append();
        Assert.assertTrue(wal.backwardIterator().hasNext());
        wal.checkpoint();
        Assert.assertFalse(wal.backwardIterator().hasNext());
    }

    @Test
    public void forwardIterator() throws IOException {
        byte[][] datas = new byte[100][];
        for (int i = 0; i < 100; i++) {
            datas[i] = BytesUtils.toBytes(i);
        }
        for (byte[] data : datas) {
            wal.append(data);
        }
        WalIterator<byte[]> iterator = wal.forwardIterator();
        Assert.assertTrue(iterator.hasNext());
        int i = 0;
        while (iterator.hasNext()) {
            byte[] data = iterator.next();
            Assert.assertArrayEquals(datas[i], data);
            i++;
        }
        Assert.assertEquals(100, i);
    }

    @Test
    public void backwardIterator() throws IOException {
        byte[][] datas = new byte[100][];
        for (int i = 0; i < 100; i++) {
            datas[i] = BytesUtils.toBytes(i);
        }
        for (byte[] data : datas) {
            wal.append(data);
        }
        WalIterator<byte[]> iterator = wal.backwardIterator();
        Assert.assertTrue(iterator.hasNext());
        int i = 0;
        while (iterator.hasNext()) {
            i++;
            byte[] data = iterator.next();
            Assert.assertArrayEquals(datas[100 - i], data);
        }
        Assert.assertEquals(100, i);
    }

    @Test
    public void clear() throws IOException {
        append();
        Assert.assertTrue(wal.backwardIterator().hasNext());
        wal.clear();
        Assert.assertFalse(wal.backwardIterator().hasNext());
        Assert.assertEquals(wal.HEADER_SIZE, wal.size());
    }
}
