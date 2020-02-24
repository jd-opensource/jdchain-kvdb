package com.jd.blockchain.kvdb;

public interface KVWriteBatch extends KVWrite {
	
	void commit();
	
}
