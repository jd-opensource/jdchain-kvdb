package com.jd.blockchain.kvdb.test;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import com.jd.blockchain.kvdb.service.rocksdb.RocksDBProxy;

//@Component
public class SimpleTest implements ApplicationRunner {

	@Override
	public void run(ApplicationArguments args) throws Exception {
		String path = "./testdb";
		
		RocksDBProxy db = DBTester.initDB(path);
		
		
		int n = 1000;
		
		Thread thrd1 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < n; i++) {
						db.set("A"+i, "VALUE_A");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		Thread thrd2 = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					for (int i = 0; i < n; i++) {
						db.set("B", "VALUE_B");
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		
		thrd1.start();
		thrd2.start();
		
		thrd1.join();
		thrd2.join();
		
		System.out.println("Test completed.");
	}

}
