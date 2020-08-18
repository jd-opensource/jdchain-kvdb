package com.jd.blockchain.kvdb.engine.proto;

/**
 * Key-Value
 */
public class KVItem implements KV {
    private byte[] key;
    private byte[] value;

    public KVItem(byte[] key, byte[] value) {
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
