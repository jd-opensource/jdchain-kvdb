package com.jd.blockchain.kvdb.engine.wal;

import com.jd.blockchain.utils.io.BytesUtils;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * 记录已刷盘最大LSN
 */
public class CheckpointWriter implements Overwrite<Long> {

    private FileChannel fileChannel;

    public CheckpointWriter(Path path) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }

    @Override
    public Long read() throws IOException {
        if (fileChannel.size() == 0) {
            return -1l;
        }
        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size());
        try {
            byte[] bs = new byte[64];
            byteBuffer.get(bs);
            return BytesUtils.toLong(bs);
        } finally {
            byteBuffer.clear();
        }
    }

    @Override
    public void write(Long data) throws IOException {
        MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, 64);
        mb.put(BytesUtils.toBytes(data));
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
