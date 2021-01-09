package com.jd.blockchain.wal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.io.BytesUtils;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class FileLogger<E extends Serializable> implements Wal<E> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileLogger.class);

    // 日志头信息长度，目前仅用于存储最新快照位置
    public static final int HEADER_SIZE = 8;
    // 日志数据头信息长度
    public static final int DATA_HEADER_SIZE = 4;

    // 最新快照位置
    private long checkpoint = 0;
    // 日志文件路径
    private Path walPath;
    // 日志文件通道
    private FileChannel fileChannel;
    // 日志配置项
    private WalConfig config;
    private WalDataCoder<E> dataCoder;

    // 定时刷盘线程
    private ScheduledExecutorService scheduledFlushExecutorService;

    public FileLogger(WalConfig config, WalDataCoder<E> dataCoder) throws IOException {
        this.config = config;
        this.dataCoder = dataCoder;
        this.walPath = Paths.get(config.getLogFile());
        if (config.isTruncateExisting()) {
            this.fileChannel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } else {
            this.fileChannel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
        // 初始化快照信息
        initCheckpoint();
        fileChannel.position(size());
        if (config.getFlushInterval() > 0) {
            scheduledFlushExecutorService = Executors.newSingleThreadScheduledExecutor();
            scheduledFlushExecutorService.scheduleAtFixedRate(() -> {
                try {
                    flush();
                } catch (Exception e) {
                    LOGGER.error("log flush error", e);
                }
            }, 0, config.getFlushInterval(), TimeUnit.SECONDS);
        }
    }

    private void initCheckpoint() throws IOException {
        long size = size();
        if (size == 0) {
            checkpoint();
        } else {
            long point = getCheckpoint();
            if (point > size) {
                checkpoint = size;
            } else {
                checkpoint = point;
            }
        }
    }

    public synchronized int append(E e) throws IOException {
        byte[] data = dataCoder.encode(e);
        byte[] dataHeader = BytesUtils.toBytes(data.length);
        byte[] bytes = new byte[data.length + dataHeader.length * 2];
        System.arraycopy(dataHeader, 0, bytes, 0, dataHeader.length);
        System.arraycopy(data, 0, bytes, dataHeader.length, data.length);
        System.arraycopy(dataHeader, 0, bytes, dataHeader.length + data.length, dataHeader.length);
        int length = fileChannel.write(ByteBuffer.wrap(bytes));
        if (config.getFlushInterval() == 0) {
            // 实时刷盘
            flush();
        }
        return length;
    }

    @Override
    public synchronized void checkpoint() throws IOException {
        long size = size();
        if (size == 0) {
            size = HEADER_SIZE;
        }
        if (size >= config.getMaxCheckpointSize()) {
            // 清理快照
            clear();
        } else {
            // 生成新快照
            fileChannel.position(0);
            fileChannel.write(ByteBuffer.wrap(BytesUtils.toBytes(size)));
            checkpoint = size;
            fileChannel.position(checkpoint);
        }
        if (config.getFlushInterval() == 0) {
            // 实时刷盘
            flush();
        }
    }

    protected long getCheckpoint() throws IOException {
        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, HEADER_SIZE);
        try {
            byte[] bs = new byte[HEADER_SIZE];
            byteBuffer.get(bs);
            return BytesUtils.toLong(bs);
        } finally {
            byteBuffer.clear();
        }
    }

    public void flush() throws IOException {
        fileChannel.force(config.isFlushMetaData());
    }

    protected long size() throws IOException {
        return fileChannel.size();
    }

    @Override
    public WalIterator<E> forwardIterator() {
        return new ForwardIterator();
    }

    @Override
    public WalIterator<E> backwardIterator() throws IOException {
        return new BackwardIterator();
    }

    @Override
    public synchronized void clear() throws IOException {
        fileChannel = FileChannel.open(walPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.READ, StandardOpenOption.WRITE);
        initCheckpoint();
    }

    public void close() throws IOException {
        if (null != scheduledFlushExecutorService) {
            scheduledFlushExecutorService.shutdown();
        }
        if (null != fileChannel) {
            fileChannel.close();
        }
    }

    private byte[] get(long position, int size) throws IOException {
        if (position < checkpoint || size <= 0 || position > size() || position + size > size()) {
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

    class ForwardIterator implements WalIterator<E> {

        private long position;

        public ForwardIterator() {
            this.position = checkpoint;
        }

        @Override
        public boolean hasNext() throws IOException {
            return position < size();
        }

        @Override
        public E next() throws IOException {
            byte[] data = null;
            byte[] header = get(position, DATA_HEADER_SIZE);
            if (null != header) {
                int size = BytesUtils.toInt(header);
                data = get(position + DATA_HEADER_SIZE, size);
                position = position + DATA_HEADER_SIZE * 2 + size;
            }

            return dataCoder.decode(data);
        }
    }

    class BackwardIterator implements WalIterator<E> {

        private long position;

        public BackwardIterator() throws IOException {
            this.position = size();
        }

        @Override
        public boolean hasNext() throws IOException {
            return position > checkpoint;
        }

        @Override
        public E next() throws IOException {
            byte[] data = null;
            byte[] header = get(position - DATA_HEADER_SIZE, DATA_HEADER_SIZE);
            if (null != header) {
                int size = BytesUtils.toInt(header);
                data = get(position - DATA_HEADER_SIZE - size, size);
                position = position - DATA_HEADER_SIZE * 2 - size;
            }

            return dataCoder.decode(data);
        }
    }
}
