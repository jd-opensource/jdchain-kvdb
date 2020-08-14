package com.jd.blockchain.kvdb.wal;

public class RedoLogConfig {

    // 是否禁用WAL
    private boolean walDisable;
    // 刷盘机制：<=-1跟随系统，0实时刷盘，>0定时刷盘
    private int walFlush;

    public RedoLogConfig(boolean walDisable, int walFlush) {
        this.walDisable = walDisable;
        this.walFlush = walFlush;
    }

    public boolean isWalDisable() {
        return walDisable;
    }

    public int getWalFlush() {
        return walFlush;
    }
}
