package com.jd.blockchain.kvdb.wal;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.binaryproto.DataContractRegistry;
import com.jd.blockchain.kvdb.proto.Entity;
import com.jd.blockchain.kvdb.proto.Meta;
import com.jd.blockchain.kvdb.proto.MetaInfo;
import com.jd.blockchain.utils.io.FileUtils;
import com.jd.blockchain.wal.FileLogger;
import com.jd.blockchain.wal.Wal;
import com.jd.blockchain.wal.WalIterator;
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
public class RedoLog {

    private static final String WAL_FILE = "kvdb.wal";
    private static final String META_FILE = "wal.meta";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedoLog.class);

    static {
        DataContractRegistry.register(Entity.class);
        DataContractRegistry.register(Meta.class);
    }

    private long lsn;
    private Wal wal;
    private Overwrite<byte[]> overwriter;
    private Meta walMeta;

    public RedoLog(String path, int flushInterval) throws IOException {

        if (!Files.exists(Paths.get(path))) {
            FileUtils.makeDirectory(path);
        }
        Path walPath = Paths.get(path, WAL_FILE);
        if (!Files.exists(walPath)) {
            Files.createFile(walPath);
        }
        this.wal = new FileLogger(new com.jd.blockchain.wal.WalConfig(flushInterval, true), walPath.toString());
        Path metaPath = Paths.get(path, META_FILE);
        if (!Files.exists(metaPath)) {
            Files.createFile(metaPath);
        }
        this.overwriter = new Overwriter(metaPath.toString());
        walMeta = getMeta();
        this.lsn = latestLsn();
    }

    public long latestLsn() throws IOException {
        WalIterator iterator = wal.backwardIterator(wal.size());
        if (iterator.hasNext()) {
            Entity entity = BinaryProtocol.decode(iterator.next());
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
    public long append(Entity entity) throws IOException {
        if (lsn < 0) {
            lsn = 0;
        }
        lsn++;
        entity.setLsn(lsn);
        LOGGER.debug("wal append {}", lsn);
        wal.append(BinaryProtocol.encode(entity, Entity.class));

        return lsn;
    }

    public void flush() throws IOException {
        wal.flush();
    }

    public long size() throws IOException {
        return wal.size();
    }

    public Entity get(long position) throws IOException {
        byte[] data = wal.get(position);
        return null != data ? BinaryProtocol.decode(data) : null;
    }

    public Entity query(long lsn) throws IOException {
        WalIterator iterator = wal.backwardIterator(wal.size());
        while (iterator.hasNext()) {
            Entity entity = BinaryProtocol.decode(iterator.next());
            if (entity.getLsn() == lsn) {
                return entity;
            }
        }
        return null;
    }

    /**
     * 数据在日志文件中的起始位置
     *
     * @param lsn
     * @return
     * @throws IOException
     */
    public long position(long lsn) throws IOException {
        if (lsn <= 0) {
            return -1;
        }
        WalIterator iterator = wal.backwardIterator(wal.size());
        while (iterator.hasNext()) {
            Entity entity = BinaryProtocol.decode(iterator.next());
            if (entity.getLsn() == lsn) {
                return iterator.position();
            }
        }
        return -1;
    }

    public void close() throws IOException {
        if (null != wal) {
            wal.close();
        }
    }

    public boolean metaUpdated() {
        return walMeta.getLsn() == lsn;
    }

    public Meta getMeta() throws IOException {
        if (null == walMeta) {
            byte[] meta = overwriter.read();
            if (null != meta && meta.length > 0) {
                walMeta = BinaryProtocol.decode(meta);
            } else {
                walMeta = new MetaInfo(-1);
                writeMeta(walMeta);
            }
        }

        return walMeta;
    }

    public void updateMeta(long lsn) throws IOException {
        if (null == walMeta) {
            return;
        }
        if (walMeta.getLsn() == -1 || lsn == walMeta.getLsn() + 1) {
            MetaInfo info = new MetaInfo(lsn);
            writeMeta(info);
            walMeta = info;
        }
    }

    private void writeMeta(Meta data) throws IOException {
        overwriter.write(BinaryProtocol.encode(data, Meta.class));
    }

    public Iterator entityIterator(long position) throws IOException {
        return new EntityIterator(wal.forwardIterator(position));
    }

    class EntityIterator implements Iterator {

        private WalIterator iterator;

        public EntityIterator(WalIterator iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() throws IOException {
            return iterator.hasNext();
        }

        @Override
        public Entity next() throws IOException {
            return BinaryProtocol.decode(iterator.next());
        }
    }

}
