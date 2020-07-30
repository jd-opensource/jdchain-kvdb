package com.jd.blockchain.kvdb.server.wal;

public class MetaInfo implements Meta {

    private long lsn;

    public MetaInfo(long lsn) {
        this.lsn = lsn;
    }

    public void setLsn(long lsn) {
        this.lsn = lsn;
    }

    @Override
    public Long getLsn() {
        return lsn;
    }
}
