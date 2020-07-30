package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.kvdb.server.config.DBInfo;
import com.jd.blockchain.kvdb.server.config.DBList;
import com.jd.blockchain.utils.io.BytesUtils;

/**
 * 一次commit所有KV操作集合
 */
public class WalEntity implements Entity {

    private String db;
    private Long lsn;
    private WalCommand command;
    private KV[] kvs;

    public WalEntity(String db, WalCommand command, KV[] kvs) {
        this.db = db;
        this.command = command;
        this.kvs = kvs;
    }

    @Override
    public String getDB() {
        return db;
    }

    @Override
    public Long getLsn() {
        return lsn;
    }

    @Override
    public WalCommand getCommand() {
        return command;
    }

    @Override
    public KV[] getKVs() {
        return kvs;
    }

    @Override
    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    public static WalEntity newCreateDatabaseEntity(DBInfo dbInfo) {
        return new WalEntity(dbInfo.getName(), WalCommand.CREATE_DATABASE,
                new KV[]{
                        new WalKV(DBList.PROPERTITY_ROOTDIR.getBytes(), BytesUtils.toBytes(dbInfo.getDbRootdir())),
                        new WalKV(DBList.PROPERTITY_PARTITIONS.getBytes(), BytesUtils.toBytes(dbInfo.getPartitions())),
                        new WalKV(DBList.PROPERTITY_ENABLE.getBytes(), BytesUtils.toBytes(dbInfo.isEnable()))
                });
    }

    public static WalEntity newEnableDatabaseEntity(String db) {
        return new WalEntity(db, WalCommand.ENABLE_DATABASE, null);
    }

    public static WalEntity newDisableDatabaseEntity(String db) {
        return new WalEntity(db, WalCommand.DISABLE_DATABASE, null);
    }

    public static WalEntity newDropDatabaseEntity(String db) {
        return new WalEntity(db, WalCommand.DROP_DATABASE, null);
    }

    public static WalEntity newPutEntity(String db, KV... kvs) {
        return new WalEntity(db, WalCommand.PUT, kvs);
    }
}
