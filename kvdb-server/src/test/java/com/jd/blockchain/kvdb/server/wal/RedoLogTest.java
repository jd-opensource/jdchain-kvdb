package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.kvdb.server.config.DBInfo;
import com.jd.blockchain.kvdb.server.config.KVDBConfig;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class RedoLogTest {

    private KVDBConfig kvdbConfig;
    private Wal<Entity, Meta> wal;

    @Before
    public void setUp() throws IOException {
        System.setProperty("logging.path", "logs");
        kvdbConfig = new KVDBConfig(this.getClass().getResource("/wal/config/kvdb.conf").getFile());
        wal = new RedoLog(kvdbConfig);
    }

    @After
    public void tearDown() throws IOException {
        wal.close();
//        FileUtils.deletePath(new File(kvdbConfig.getDbsRootdir()), true);
    }

    @Test
    public void testWrite() throws IOException {
        DBInfo dbInfo = new DBInfo();
        dbInfo.setName("test");
        dbInfo.setPartitions(4);
        dbInfo.setDbRootdir(kvdbConfig.getDbsRootdir());
        dbInfo.setEnable(true);
        Long lsn = wal.append(WalEntity.newCreateDatabaseEntity(dbInfo));
        wal.flush(true);
        Assert.assertTrue(wal.exists(lsn));
        Entity entity = wal.get(lsn);
        Assert.assertTrue(WalCommand.CREATE_DATABASE.equals(entity.getCommand()));

        Assert.assertFalse(wal.exists(lsn + lsn));
    }

    @Test
    public void testRead() throws IOException {
        wal.latestLsn();
        long position = 0;
        long i = 0;
        while (true) {
            long next = wal.next(position);
            if (next == -1) {
                break;
            }
            Entity e = wal.get(position, (int) (next - position - RedoLog.HEADER_SIZE * 2));
            System.out.println(e.getLsn());
            System.out.println(e.getCommand().toString());
            for (KV kv : e.getKVs()) {
                System.out.println(BytesUtils.toString(kv.getKey()));
                System.out.println(kv.getValue());
                i++;
            }
            position = next;
        }

        System.out.println("total count : " + i);

    }

}
