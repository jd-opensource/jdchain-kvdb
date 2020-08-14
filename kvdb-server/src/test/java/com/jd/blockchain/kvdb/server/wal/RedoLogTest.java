//package com.jd.blockchain.kvdb.server.wal;
//
//import com.jd.blockchain.kvdb.server.config.DBInfo;
//import com.jd.blockchain.kvdb.server.config.KVDBConfig;
//import com.jd.blockchain.utils.io.BytesUtils;
//import com.jd.blockchain.utils.io.FileUtils;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//
//public class RedoLogTest {
//
//    private KVDBConfig kvdbConfig;
//    private RedoLog wal;
//
//    @Before
//    public void setUp() throws IOException {
//        System.setProperty("logging.path", "logs");
//        kvdbConfig = new KVDBConfig(this.getClass().getResource("/wal/config/kvdb.conf").getFile());
//        wal = new RedoLog(kvdbConfig);
//    }
//
//    @After
//    public void tearDown() throws IOException {
//        wal.close();
//        FileUtils.deletePath(new File(kvdbConfig.getDbsRootdir()), true);
//    }
//
//    @Test
//    public void test() throws IOException {
//        testWrite();
//        testRead();
//    }
//
//    public void testWrite() throws IOException {
//        String db = "test";
//        DBInfo dbInfo = new DBInfo();
//        dbInfo.setName(db);
//        dbInfo.setPartitions(4);
//        dbInfo.setDbRootdir(kvdbConfig.getDbsRootdir());
//        dbInfo.setEnable(true);
//        Long lsn = wal.append(WalEntity.newCreateDatabaseEntity(dbInfo));
//        wal.flush();
//        Entity entity = wal.query(lsn);
//        Assert.assertNotNull(entity);
//        Assert.assertTrue(WalCommand.CREATE_DATABASE.equals(entity.getCommand()));
//
//        lsn = wal.append(WalEntity.newPutEntity(db, new WalKV("k".getBytes(), "v".getBytes())));
//        wal.flush();
//        entity = wal.query(lsn);
//        Assert.assertNotNull(entity);
//        Assert.assertTrue(WalCommand.PUT.equals(entity.getCommand()));
//
//        lsn = wal.append(WalEntity.newDisableDatabaseEntity(db));
//        wal.flush();
//        entity = wal.query(lsn);
//        Assert.assertNotNull(entity);
//        Assert.assertTrue(WalCommand.DISABLE_DATABASE.equals(entity.getCommand()));
//
//        lsn = wal.append(WalEntity.newEnableDatabaseEntity(db));
//        wal.flush();
//        entity = wal.query(lsn);
//        Assert.assertNotNull(entity);
//        Assert.assertTrue(WalCommand.ENABLE_DATABASE.equals(entity.getCommand()));
//
//        lsn = wal.append(WalEntity.newDropDatabaseEntity(db));
//        wal.flush();
//        entity = wal.query(lsn);
//        Assert.assertNotNull(entity);
//        Assert.assertTrue(WalCommand.DROP_DATABASE.equals(entity.getCommand()));
//
//        Assert.assertNull(wal.query(lsn + lsn));
//    }
//
//    public void testRead() throws IOException {
//        long i = 0;
//        Iterator iterator = wal.entityIterator(0);
//        while (iterator.hasNext()) {
//            Entity e = iterator.next();
//            System.out.println(e.getLsn());
//            System.out.println(e.getCommand().toString());
//            for (KV kv : e.getKVs()) {
//                System.out.println(BytesUtils.toString(kv.getKey()));
//                System.out.println(kv.getValue());
//                i++;
//            }
//        }
//
//        System.out.println("total count : " + i);
//
//    }
//
//}
