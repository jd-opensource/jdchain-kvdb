package com.jd.blockchain.kvdb.proto;

/**
 * 一次commit所有KV操作集合
 */
public class WalEntity implements Entity {

    private Long lsn;
    private KV[] kvs;

    public WalEntity(KV[] kvs) {
        this.kvs = kvs;
    }

    @Override
    public Long getLsn() {
        return lsn;
    }

    @Override
    public KV[] getKVs() {
        return kvs;
    }

    @Override
    public void setLsn(Long lsn) {
        this.lsn = lsn;
    }

    public static WalEntity newPutEntity(KV... kvs) {
        return new WalEntity(kvs);
    }
}
