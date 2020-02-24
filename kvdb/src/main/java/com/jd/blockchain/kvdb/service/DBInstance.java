package com.jd.blockchain.kvdb.service;

import com.jd.blockchain.utils.io.BytesUtils;

public abstract class DBInstance implements KVStorage {

	@Override
	public String get(String key) {
		return BytesUtils.toString(get(BytesUtils.toBytes(key)));
	}

	@Override
	public void set(String key, String value) {
		set(BytesUtils.toBytes(key), BytesUtils.toBytes(value));
	}

	/**
	 * 关闭数据库；<p>
	 * 
	 * 注：关闭过程中可能引发的异常将被处理而不会被抛出；
	 */
	public abstract void close();

	/**
	 *  移除数据库；<p>
	 * 
	 * 注：移除过程中可能引发的异常将被处理而不会被抛出；
	 */
	public abstract void drop();
}