package com.jd.blockchain.kvdb.server.wal;

/**
 * 一次commit所有KV操作集合
 */
public class WalEntity implements Entity {

    private String id;
    private WalCommand command;
    private KV[] kvs;

    public WalEntity(String id, WalCommand command, KV[] kvs) {
        this.id = id;
        this.command = command;
        this.kvs = kvs;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public WalCommand getCommand() {
        return command;
    }

    @Override
    public KV[] getKVs() {
        return kvs;
    }

    public static WalEntity newCreateDatabaseEntity(String id) {
        return new WalEntity(id, WalCommand.CREATE_DATABASE, null);
    }

    public static WalEntity newEnableDatabaseEntity(String id) {
        return new WalEntity(id, WalCommand.ENABLE_DATABASE, null);
    }

    public static WalEntity newDisableDatabaseEntity(String id) {
        return new WalEntity(id, WalCommand.DISABLE_DATABASE, null);
    }

    public static WalEntity newDropDatabaseEntity(String id) {
        return new WalEntity(id, WalCommand.DROP_DATABASE, null);
    }

    public static WalEntity newPutEntity(String id, KV... kvs) {
        return new WalEntity(id, WalCommand.PUT, kvs);
    }
}
