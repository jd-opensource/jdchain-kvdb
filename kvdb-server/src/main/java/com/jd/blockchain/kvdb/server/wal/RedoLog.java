package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.binaryproto.DataContractRegistry;
import com.jd.blockchain.kvdb.server.config.KVDBConfig;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * RedoLog:
 * HEADER_SIZE + ENTITY + HEADER_SIZE
 * 支持逆向查询
 */
public class RedoLog implements Wal<Entity, Meta> {

    private static final String WAL_FILE = "kvdb.wal";
    private static final String META_FILE = "wal.meta";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedoLog.class);

    static {
        DataContractRegistry.register(Entity.class);
        DataContractRegistry.register(Meta.class);
    }

    private boolean disable;
    public static final int HEADER_SIZE = 4;
    private long lsn;
    private Append<byte[]> appender;
    private Overwrite<byte[]> overwriter;
    private Meta walMeta;

    public RedoLog(KVDBConfig kvdbConfig) throws IOException {
        disable = kvdbConfig.isWalDisable();
        if (!disable) {
            if (!Files.exists(Paths.get(kvdbConfig.getDbsRootdir()))) {
                FileUtils.makeDirectory(kvdbConfig.getDbsRootdir());
            }
            Path walPath = Paths.get(kvdbConfig.getDbsRootdir(), WAL_FILE);
            if (!Files.exists(walPath)) {
                Files.createFile(walPath);
            }
            this.appender = new Appender(walPath.toString(), kvdbConfig.getWalFlush());
            Path metaPath = Paths.get(kvdbConfig.getDbsRootdir(), META_FILE);
            if (!Files.exists(metaPath)) {
                Files.createFile(metaPath);
            }
            this.overwriter = new Overwriter(metaPath.toString());
            walMeta = readMeta();
            this.lsn = latestLsn();
        }
    }

    @Override
    public long latestLsn() throws IOException {
        long position = appender.size();
        if (position > 0) {
            int length = readHeader(position - HEADER_SIZE);
            Entity entity = get(position - length - HEADER_SIZE * 2, length);
            return entity.getLsn();
        }
        // 如果不存在WAL则返回meta中记录的LSN
        return walMeta.getLsn();
    }

    /**
     * length_header + operations + length_header
     *
     * @param entity
     * @throws IOException
     */
    @Override
    public synchronized long append(Entity entity) throws IOException {
        if (disable) {
            return -1l;
        }
        lsn++;
        entity.setLsn(lsn);
        LOGGER.debug("wal append {} ...", lsn);
        byte[] oprBytes = BinaryProtocol.encode(entity, Entity.class);
        byte[] header = BytesUtils.toBytes(oprBytes.length);
        byte[] bytes = new byte[oprBytes.length + header.length * 2];
        System.arraycopy(header, 0, bytes, 0, header.length);
        System.arraycopy(oprBytes, 0, bytes, header.length, oprBytes.length);
        System.arraycopy(header, 0, bytes, header.length + oprBytes.length, header.length);
        appender.append(bytes);

        return lsn;
    }

    @Override
    public void flush(boolean meta) throws IOException {
        appender.flush(meta);
    }

    @Override
    public long size() throws IOException {
        return appender.size();
    }

    @Override
    public long next(long position) throws IOException {
        int length = readHeader(position);
        if (length > 0) {
            return position + length + HEADER_SIZE * 2;
        } else {
            return -1;
        }
    }

    @Override
    public Entity get(long position, int length) throws IOException {
        if (length > 0) {
            return BinaryProtocol.decode(appender.get(position + HEADER_SIZE, length));
        } else {
            return null;
        }
    }

    @Override
    public Entity get(long position) throws IOException {
        return get(position, readHeader(position));
    }

    @Override
    public Entity get(Long lsn) throws IOException {
        long position = appender.size();
        while (position > 0) {
            int length = readHeader(position - HEADER_SIZE);
            Entity entity = get(position - length - HEADER_SIZE * 2, length);
            if (entity.getLsn().equals(lsn)) {
                return entity;
            }
            position = position - length - 2 * HEADER_SIZE;
        }
        return null;
    }

    @Override
    public long position(Long lsn) throws IOException {
        if (lsn <= 0) {
            return 0;
        }
        long position = appender.size();
        while (position > 0) {
            int length = readHeader(position - HEADER_SIZE);
            Entity entity = get(position - length - HEADER_SIZE * 2, length);
            position = position - length - 2 * HEADER_SIZE;
            if (entity.getLsn().equals(lsn)) {
                return position;
            }
        }
        return -1;
    }

    @Override
    public boolean exists(Long lsn) throws IOException {
        return null != get(lsn);
    }

    @Override
    public synchronized void disable() {
        disable = true;
    }

    @Override
    public synchronized void enable() {
        disable = false;
    }

    private int readHeader(long position) throws IOException {
        byte[] data = appender.get(position, HEADER_SIZE);
        if (data != null && data.length > 0) {
            return BytesUtils.toInt(data);
        } else {
            return -1;
        }
    }

    @Override
    public void close() throws IOException {
        if (null != appender) {
            appender.close();
        }
    }

    @Override
    public boolean metaUpdated() throws IOException {
        return walMeta.getLsn() == lsn;
    }

    @Override
    public Meta readMeta() throws IOException {
        if (null == walMeta) {
            byte[] meta = overwriter.readMeta();
            if (null != meta && meta.length > 0) {
                walMeta = BinaryProtocol.decode(meta);
            } else {
                Meta metaInfo = new MetaInfo(-1);
                writeMeta(metaInfo);
                walMeta = metaInfo;
            }
        }

        return walMeta;
    }

    @Override
    public synchronized void updateMeta(long lsn) throws IOException {
        if (lsn > walMeta.getLsn()) {
            MetaInfo info = new MetaInfo(lsn);
            writeMeta(info);
            walMeta = info;
        }
    }

    @Override
    public synchronized void writeMeta(Meta data) throws IOException {
        overwriter.writeMeta(BinaryProtocol.encode(data, Meta.class));
    }
}
