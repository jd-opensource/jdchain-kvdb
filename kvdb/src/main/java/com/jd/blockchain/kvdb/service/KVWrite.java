package com.jd.blockchain.kvdb.service;

public interface KVWrite {

	void set(String key, String value);

	void set(byte[] key, byte[] value);

}