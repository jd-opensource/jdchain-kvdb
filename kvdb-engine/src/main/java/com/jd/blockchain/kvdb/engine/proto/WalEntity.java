package com.jd.blockchain.kvdb.engine.proto;

/**
 * 一次commit所有KV操作集合
 */
public class WalEntity implements Entity {

    private KV[] kvs;

    public WalEntity(KV[] kvs) {
        this.kvs = kvs;
    }

    @Override
    public KV[] getKVs() {
        return kvs;
    }

    public static WalEntity newPutEntity(KV... kvs) {
        return new WalEntity(kvs);
    }
}
