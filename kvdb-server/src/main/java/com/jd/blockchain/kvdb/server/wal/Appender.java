package com.jd.blockchain.kvdb.server.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * file appender, using java nio
 */
public class Appender implements Append<byte[]> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Appender.class);

    private FileChannel fileChannel;
    private int flush;
    // 是否有未刷新数据
    private volatile boolean needToFlush;
    // 异步刷盘线程
    private ExecutorService asyncFlushExecutorService;
    // 定时刷盘线程
    private ScheduledExecutorService scheduledFlushExecutorService;

    public Appender(String file, int flush) throws IOException {
        this.flush = flush;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
        this.asyncFlushExecutorService = Executors.newFixedThreadPool(1);
        if (flush > 0) {
            scheduledFlushExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledFlushExecutorService.scheduleAtFixedRate(() -> {
                try {
                    flush(true);
                } catch (Exception e) {
                    LOGGER.error("appender flush error", e);
                }
            }, 0, flush, TimeUnit.SECONDS);
        }
    }

    public long append(byte[] data) throws IOException {
        MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_WRITE, size(), data.length);
        mb.put(data);
        needToFlush = true;
        if (flush == 0) {
            // 异步实时刷盘
            asyncFlushExecutorService.submit(() -> {
                try {
                    flush(true);
                } catch (IOException e) {
                    LOGGER.error("appender flush error", e);
                }
            });
        }
        return data.length;
    }

    public void flush(boolean meta) throws IOException {
        if (needToFlush) {
            needToFlush = false;
            LOGGER.debug("appender flushing...");
            fileChannel.force(meta);
            LOGGER.debug("appender flush complete");
        }
    }

    @Override
    public long size() throws IOException {
        return fileChannel.size();
    }

    @Override
    public byte[] get(long position, int size) throws IOException {
        if (position > size() || position + size > size()) {
            return null;
        }
        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, size);
        try {
            byte[] bs = new byte[size];
            byteBuffer.get(bs);
            return bs;
        } finally {
            byteBuffer.clear();
        }
    }

    @Override
    public void close() throws IOException {
        if (null != asyncFlushExecutorService) {
            asyncFlushExecutorService.shutdown();
        }
        if (null != asyncFlushExecutorService) {
            asyncFlushExecutorService.shutdown();
        }
        if (null != fileChannel) {
            fileChannel.close();
        }
    }
}
