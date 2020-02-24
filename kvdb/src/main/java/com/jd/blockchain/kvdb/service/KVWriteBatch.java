package com.jd.blockchain.kvdb.service;

public interface KVWriteBatch extends KVWrite {
	
	void commit();
	
}
