package com.jd.blockchain.kvdb.service.rocksdb;

import java.util.concurrent.CountDownLatch;

import com.jd.blockchain.kvdb.service.KVWriteBatch;
import com.jd.blockchain.utils.io.BytesUtils;

class KVWriteClusterBatch implements KVWriteBatch {

	private KVWritePartition[] taskPartitions;

	private Partitioner partitioner;

	KVWriteClusterBatch(KVWritePartition[] taskPartitions, Partitioner partitioner) {
		this.taskPartitions = taskPartitions;
		this.partitioner = partitioner;
	}

	@Override
	public void set(String key, String value) {
		set(BytesUtils.toBytes(key), BytesUtils.toBytes(value));
	}

	@Override
	public void set(byte[] key, byte[] value) {
		int pid = partitioner.partition(key);
		taskPartitions[pid].set(key, value);
	}

	@Override
	public void commit() {
		// TODO: 非线程安全，需要修正；
		CountDownLatch completedLatch = new CountDownLatch(taskPartitions.length);
		for (KVWritePartition parti : taskPartitions) {
			parti.commit(completedLatch);
		}
		try {
			completedLatch.await();
		} catch (InterruptedException e) {
		}
	}

}