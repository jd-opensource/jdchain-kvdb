package com.jd.blockchain.kvdb.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import com.jd.blockchain.kvdb.service.DBInstance;
import com.jd.blockchain.kvdb.service.KVStorage;
import com.jd.blockchain.kvdb.service.KVWriteBatch;
import com.jd.blockchain.kvdb.service.rocksdb.RocksDBCluster;
import com.jd.blockchain.kvdb.service.rocksdb.RocksDBProxy;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;

@Component
public class DBTester implements ApplicationRunner {
	public static Logger LOGGER = LoggerFactory.getLogger(DBTester.class);

	@Value("${test.cluster}")
	private boolean useCluster;
	@Value("${test.partitions}")
	private int partitions;
	@Value("${test.threads}")
	private int threads;
	@Value("${test.batch}")
	private int batch;

	@Override
	public void run(ApplicationArguments args) throws Exception {

		LOGGER.info("start rocks db test... [cluster={}][test.partitions={}][test.threads={}][test.batch={}]",
				useCluster, partitions, threads, batch);

		DBInstance[] dbs;
		if (useCluster) {
			dbs = new DBInstance[1];
			dbs[0] = initDBCluster("./testdbcluster", partitions);
		} else {
			dbs = new DBInstance[partitions];
			for (int i = 0; i < partitions; i++) {
				String path = "./testdb" + i;
				dbs[i] = initDB(path);
			}
		}

		// 单数据库的读写测试；
//		singleDBReadWriteTest(count, key, value, db);

		long startTime = System.currentTimeMillis();
		AtomicLong endTimeReporter = new AtomicLong();
		AtomicInteger taskCounter = new AtomicInteger(threads);
		// 多数据库的并发读写测试；
		CountDownLatch completedLatch = new CountDownLatch(1);
		int dbCount = dbs.length;
		for (int i = 0; i < threads; i++) {
			int dbIdx = i % dbCount;
			Thread thrd = new Thread(
					new MultiDBTestTask(i, batch, dbs[dbIdx], taskCounter, endTimeReporter, completedLatch));
			thrd.start();
		}

		try {
			completedLatch.await();
		} catch (InterruptedException e) {
		}
		statistics(batch * threads, endTimeReporter.get() - startTime,
				String.format("RocksDB 性能测试-多库并行[%s%s库-%s线程]", useCluster ? "集群-" : "", partitions, threads));

		// 清理磁盘；
		for (int i = 0; i < dbCount; i++) {
			dbs[i].drop();
		}
	}

	@SuppressWarnings("unused")
	private static void singleDBReadWriteTest(long count,int prefix, RocksDBProxy db) {
		// 测试写入；
		testWrite(count,prefix, db);

		// 测试读取；
		testRead(count, db);

		// 测试读取；
		testMayExist(count, db);
	}

	public static RocksDBProxy initDB(String path) {
		if (FileUtils.existDirectory(path)) {
			FileUtils.deleteFile(path);
		}
		RocksDBProxy db = RocksDBProxy.open(path);
		db.set("INIT-KEY", "INIT-VALUE");

		return db;
	}

	public static RocksDBCluster initDBCluster(String path, int partitions) {
		if (FileUtils.existDirectory(path)) {
			FileUtils.deleteFile(path);
		}
		RocksDBCluster db = RocksDBCluster.open(path, partitions);
		db.set("INIT-KEY", "INIT-VALUE");

		return db;
	}

	private static void testWrite(long count, int prefix, KVStorage db) {
		long startTs = System.currentTimeMillis();

		byte[] value = BytesUtils.toBytes("TEST-VALUE");
		KVWriteBatch batch = db.beginBatch();
		for (int i = 0; i < count; i++) {
			byte[] key = BytesUtils.toBytes(prefix + i);
			batch.set(key, value);
		}
		batch.commit();

		long elapsedTs = System.currentTimeMillis() - startTs;
		statistics(count, elapsedTs, "[RocksDB 性能测试-写入操作]");
	}

	private static void testRead(long count, RocksDBProxy db) {
		long startTs = System.currentTimeMillis();

		for (int i = 0; i < count; i++) {
			byte[] key = BytesUtils.toBytes(i);
			db.get(key);
		}

		long elapsedTs = System.currentTimeMillis() - startTs;
		statistics(count, elapsedTs, "[RocksDB 性能测试-读取操作]");
	}

	private static void testMayExist(long count, RocksDBProxy db) {
		long startTs = System.currentTimeMillis();

		for (int i = 0; i < count; i++) {
			byte[] key = BytesUtils.toBytes(i);
			db.exist(key);
		}

		long elapsedTs = System.currentTimeMillis() - startTs;
		statistics(count, elapsedTs, "[RocksDB 性能测试-存在性操作]");
	}

	private static void statistics(long messageCount, long elapsedTime, String title) {

		double tps = messageCount * 1000.00D / elapsedTime;

		StringBuilder info = new StringBuilder();
		info.append("\r\n**********************************************\r\n");
		info.append(String.format("%s -- 操作总数：%s; 操作耗时：%s ms; TPS：%.2f; ", title, messageCount, elapsedTime, tps));
		info.append("\r\n**********************************************\r\n");

//		LOGGER.info("performance test completed!\r\n{}", info.toString());
		System.out.print(info.toString());
	}

	private static class MultiDBTestTask implements Runnable {

		private int taskId;
		
		private long count;
		private KVStorage db;
		
		private AtomicInteger taskCounter;

		private AtomicLong endTimeReporter;

		private CountDownLatch completedLatch;

		public MultiDBTestTask(int taskId, long count, KVStorage db, AtomicInteger taskCounter, AtomicLong endTimeReporter,
				CountDownLatch completedLatch) {
			this.taskId = taskId;
			this.count = count;
			this.db = db;
			this.taskCounter = taskCounter;
			this.endTimeReporter = endTimeReporter;
			this.completedLatch = completedLatch;
		}

		@Override
		public void run() {
			// 测试写入；
			try {
				DBTester.testWrite(count, taskId, db);

				int n = taskCounter.decrementAndGet();
				if (n == 0) {
					endTimeReporter.set(System.currentTimeMillis());
					completedLatch.countDown();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
