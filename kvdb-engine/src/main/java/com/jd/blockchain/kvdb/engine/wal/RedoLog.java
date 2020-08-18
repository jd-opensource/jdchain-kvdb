package com.jd.blockchain.kvdb.engine.wal;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.binaryproto.DataContractRegistry;
import com.jd.blockchain.kvdb.engine.proto.Entity;
import com.jd.blockchain.wal.FileLogger;
import com.jd.blockchain.wal.Wal;
import com.jd.blockchain.wal.WalConfig;
import com.jd.blockchain.wal.WalIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class RedoLog {

    private static final String WAL_FILE = "kvdb.wal";
    private static final String META_FILE = "wal.meta";

    private static final Logger LOGGER = LoggerFactory.getLogger(RedoLog.class);

    static {
        DataContractRegistry.register(Entity.class);
    }

    private long lsn;
    private Wal wal;
    private Overwrite<Long> overwriter;
    private long checkPoint = -1l;

    public RedoLog(String path, int flushInterval) throws IOException {
        this.wal = new FileLogger(new WalConfig(flushInterval, true), Paths.get(path, WAL_FILE).toString());
        this.overwriter = new CheckpointWriter(Paths.get(path, META_FILE));
        this.checkPoint = getCheckpoint();
        this.lsn = latestLsn();
    }

    public long latestLsn() throws IOException {
        WalIterator iterator = wal.backwardIterator(wal.size());
        if (iterator.hasNext()) {
            Entity entity = BinaryProtocol.decode(iterator.next());
            return entity.getLsn();
        }
        // 如果不存在WAL则返回meta中记录的LSN
        return checkPoint;
    }

    /**
     * length_header + operations + length_header
     *
     * @param entity
     * @throws IOException
     */
    public long append(Entity entity) throws IOException {
        if (lsn < 0) {
            lsn = 1;
        } else {
            lsn++;
        }
        entity.setLsn(lsn);
        LOGGER.debug("wal append {}", lsn);
        wal.append(BinaryProtocol.encode(entity, Entity.class));

        return lsn;
    }

    public void flush() throws IOException {
        wal.flush();
    }

    public void clear() throws IOException {
        wal.clear();
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
        if (null != overwriter) {
            overwriter.close();
        }
    }

    public boolean updated() {
        return checkPoint == lsn;
    }

    public long getCheckpoint() throws IOException {
        if (checkPoint < 0) {
            checkPoint = overwriter.read();
        }

        return checkPoint;
    }

    public void setCheckpoint(long lsn) throws IOException {
        if (lsn <= 0) {
            return;
        }
        if ((checkPoint == -1 && lsn == 1) || lsn == checkPoint + 1) {
            writeCheckpoint(lsn);
            checkPoint = lsn;
        }
    }

    private void writeCheckpoint(long checkPoint) throws IOException {
        overwriter.write(checkPoint);
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
