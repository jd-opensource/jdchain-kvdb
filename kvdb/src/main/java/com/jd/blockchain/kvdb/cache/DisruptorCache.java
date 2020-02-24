package com.jd.blockchain.kvdb.cache;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

public class DisruptorCache {

	private int poolSize;

	private int poolNumber;

	private Disruptor<KVItem> disruptor;

	private RingBuffer<KVItem> buffer;

	public DisruptorCache(int poolSize, KVHandle handle) {
		this.poolSize = poolSize;
//		this.poolNumber = poolNumber;
		disruptor = new Disruptor<KVItem>(new KVItemFactory(), poolSize, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new Thread(r);
			}
		});

		disruptor.handleEventsWith(new KVItemHandle(handle));
		buffer = disruptor.getRingBuffer();
	}

	public void start() {
		disruptor.start();
	}

	public void shutdown() {
		disruptor.shutdown();
	}

	public void add(byte[] key, byte[] value) {
		long seq = buffer.next();
		try {
			KVItem item = buffer.get(seq);
			item.key = key;
			item.value = value;
		} finally {
			buffer.publish(seq);
		}
	}

	private static class KVItemHandle implements EventHandler<KVItem> {

		private KVHandle handle;

		public KVItemHandle(KVHandle handle) {
			this.handle = handle;
		}

		@Override
		public void onEvent(KVItem event, long sequence, boolean endOfBatch) throws Exception {
			handle.handle(event.key, event.value);
			event.key = null;
			event.value = null;
		}

	}

	private static class PoolHandle implements EventHandler<KVItemPool> {

		private KVHandle handle;

		public PoolHandle(KVHandle handle) {
			this.handle = handle;
		}

		@Override
		public void onEvent(KVItemPool event, long sequence, boolean endOfBatch) throws Exception {
			event.handle(handle);
			event.clear();
		}

	}

	private static class KVItemFactory implements EventFactory<KVItem> {

		public KVItemFactory() {
		}

		@Override
		public KVItem newInstance() {
			return new KVItem();
		}

	}

	private static class PoolFactory implements EventFactory<KVItemPool> {

		private int poolSize;

		public PoolFactory(int poolSize) {
			this.poolSize = poolSize;
		}

		@Override
		public KVItemPool newInstance() {
			return new KVItemPool(poolSize);
		}

	}

	private static class KVItem {
		public byte[] key;

		public byte[] value;
	}

	private static class KVItemPool {

		private int size;

		private byte[][] keys;

		private byte[][] values;

		private AtomicInteger lengthCounter;

		public KVItemPool(int size) {
			this.size = size;
			this.keys = new byte[size][];
			this.values = new byte[size][];
			this.lengthCounter = new AtomicInteger();
		}

		public boolean add(byte[] key, byte[] value) {
			int index = lengthCounter.getAndIncrement();
			if (index >= size) {
				return false;
			}
			keys[index] = key;
			values[index] = value;
			return true;
		}

		public void clear() {
			for (int i = 0; i < size; i++) {
				keys[i] = null;
				values[i] = null;
			}
			lengthCounter.set(0);
		}

		public void handle(KVHandle handle) {
			int len = lengthCounter.get();
			for (int i = 0; i < len; i++) {
				handle.handle(keys[i], values[i]);
			}
		}
	}

	public static interface KVHandle {

		void handle(byte[] key, byte[] value);

	}

}
