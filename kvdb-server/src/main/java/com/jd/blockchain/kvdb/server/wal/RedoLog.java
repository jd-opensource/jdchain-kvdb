package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.binaryproto.DataContractRegistry;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class RedoLog implements Wal {

    static {
        DataContractRegistry.register(Entity.class);
    }

    public static final int HEADER_SIZE = 4;
    private FileChannel fileChannel;

    public RedoLog(String logDir) throws IOException {
        if (!Files.exists(Paths.get(logDir))) {
            FileUtils.makeDirectory(logDir);
        }
        Path path = Paths.get(logDir, "kvdb.wal");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        fileChannel = new RandomAccessFile(path.toString(), "rw").getChannel();
    }

    /**
     * length_header + operations + length_header
     *
     * @param entity
     * @throws IOException
     */
    @Override
    public synchronized void append(Entity entity) throws IOException {
        byte[] oprBytes = BinaryProtocol.encode(entity, Entity.class);
        byte[] header = BytesUtils.toBytes(oprBytes.length);
        MappedByteBuffer mb = fileChannel.map(FileChannel.MapMode.READ_WRITE, fileChannel.size(), header.length * 2 + oprBytes.length);
        mb.put(header);
        mb.put(oprBytes);
        mb.put(header);
        mb.force();
    }

    @Override
    public synchronized void flush() throws IOException {
        fileChannel.force(true);
    }

    @Override
    public long next(long position) throws IOException {
        if (position >= fileChannel.size() - 1) {
            return -1;
        }
        int length = readHeader(position);
        return position + length + HEADER_SIZE * 2;
    }

    @Override
    public Entity get(long position, int length) throws IOException {
        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position + HEADER_SIZE, length);
        try {
            byte[] bs = new byte[length];
            byteBuffer.get(bs);
            Entity entity = BinaryProtocol.decode(bs);
            return entity;
        } finally {
            byteBuffer.clear();
        }
    }

    @Override
    public Entity get(long position) throws IOException {
        return get(position, readHeader(position));
    }

    @Override
    public Entity get(String id) throws IOException {
        long position = fileChannel.size();
        while (position > 0) {
            int length = readHeader(position - HEADER_SIZE);
            Entity entity = get(position - length - HEADER_SIZE * 2, length);
            if (entity.id().equals(id)) {
                return entity;
            }
            position = position - length - 2 * HEADER_SIZE;
        }
        return null;
    }

    @Override
    public boolean exists(String id) throws IOException {
        return null != get(id);
    }

    private int readHeader(long position) throws IOException {
        MappedByteBuffer byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, position, HEADER_SIZE);
        try {
            byte[] bs = new byte[HEADER_SIZE];
            byteBuffer.get(bs);
            return BytesUtils.toInt(bs);
        } finally {
            byteBuffer.clear();
        }
    }
}
