package com.jd.blockchain.kvdb.service;

public interface KVStorage extends KVWrite, KVRead {
	
	KVWriteBatch beginBatch();
	
}