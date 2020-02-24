package com.jd.blockchain.kvdb.service;

public interface KVRead {

	byte[] get(byte[] key);

	String get(String key);

}