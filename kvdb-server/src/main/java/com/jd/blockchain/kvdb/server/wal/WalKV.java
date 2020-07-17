package com.jd.blockchain.kvdb.server.wal;

/**
 * Key-Value
 */
public class WalKV implements KV {
    private byte[] key;
    private byte[] value;

    public WalKV(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public byte[] getValue() {
        return value;
    }
}
