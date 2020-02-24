package com.jd.blockchain.kvdb;

public interface KVRead {

	byte[] get(byte[] key);

	String get(String key);

}