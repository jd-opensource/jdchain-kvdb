package com.jd.blockchain.kvdb.service.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompressionType;
import org.rocksdb.MutableColumnFamilyOptions;
import org.rocksdb.MutableColumnFamilyOptions.MutableColumnFamilyOptionsBuilder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jd.blockchain.kvdb.service.DBException;
import com.jd.blockchain.kvdb.service.DBInstance;
import com.jd.blockchain.kvdb.service.KVWriteBatch;
import com.jd.blockchain.utils.io.BytesUtils;
import com.jd.blockchain.utils.io.FileUtils;

public class RocksDBProxy extends DBInstance {

	private static Logger LOGGER = LoggerFactory.getLogger(RocksDBProxy.class);

	private WriteOptions writeOptions;

	private RocksDB db;

	protected String path;

	public String getPath() {
		return path;
	}

	private RocksDBProxy(RocksDB db, String path) {
		this.db = db;
		this.path = path;
		this.writeOptions = initWriteOptions();

	}

	private static WriteOptions initWriteOptions() {
		WriteOptions options = new WriteOptions();
		options.setDisableWAL(false);
		options.setNoSlowdown(false);
		return options;
	}

	private static Options initDBOptions() {
		Options options = new Options();
		options.setCreateIfMissing(true);
		return options;
	}

	private static MutableColumnFamilyOptions initColumnFamilyOptions() {
		return MutableColumnFamilyOptions.builder()
				.setWriteBufferSize(32 * 1024 * 1024)
				.setMaxWriteBufferNumber(4)
				.build();
	}

	private static void initDB(RocksDB db) throws RocksDBException {
		ColumnFamilyHandle defaultColumnFamily = db.getDefaultColumnFamily();
		db.setOptions(defaultColumnFamily, initColumnFamilyOptions());
	}

	public static RocksDBProxy open(String path) {
		try {
			RocksDB db = RocksDB.open(initDBOptions(), path);

			initDB(db);

			return new RocksDBProxy(db, path);
		} catch (RocksDBException e) {
			throw new DBException(e.getMessage(), e);
		}
	}

	public boolean exist(byte[] key) {
		return db.keyMayExist(key, new StringBuilder());
	}

	@Override
	public byte[] get(byte[] key) {
		try {
			return db.get(key);
		} catch (RocksDBException e) {
			throw new DBException(e.getMessage(), e);
		}
	}

	@Override
	public KVWriteBatch beginBatch() {
		return new RocksDBWriteBatch(db);
	}

	@Override
	public void set(byte[] key, byte[] value) {
		try {

			db.put(writeOptions, key, value);
		} catch (RocksDBException e) {
			throw new DBException(e.getMessage(), e);
		}
	}

	private static class RocksDBWriteBatch implements KVWriteBatch {

		private RocksDB db;
		private WriteBatch batch;

		public RocksDBWriteBatch(RocksDB db) {
			this.db = db;
			this.batch = new WriteBatch();
		}

		@Override
		public void set(String key, String value) {
			set(BytesUtils.toBytes(key), BytesUtils.toBytes(value));
		}

		@Override
		public void set(byte[] key, byte[] value) {
			try {
				batch.put(key, value);
			} catch (RocksDBException e) {
				throw new DBException(e.getMessage(), e);
			}
		}

		@Override
		public void commit() {
			try {
				db.write(new WriteOptions(), batch);
			} catch (RocksDBException e) {
				throw new DBException(e.getMessage(), e);
			}
		}

	}

	@Override
	public void close() {
		if (db != null) {
			try {
				db.close();
			} catch (Exception e) {
				LOGGER.error("Error occurred while closing rocksdb[" + path + "]", e);
			} finally {
				db = null;
			}
		}
	}

	@Override
	public synchronized void drop() {
		if (db != null) {
			try {
				close();
				FileUtils.deleteFile(path);
			} catch (Exception e) {
				LOGGER.error("Error occurred while dropping rocksdb[" + path + "]", e);
			}
		}
	}

}
