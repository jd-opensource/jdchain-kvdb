package com.jd.blockchain.kvdb.benchmark;

import com.jd.blockchain.kvdb.protocol.client.ClientConfig;

import utils.ArgumentSet;

import java.util.concurrent.CountDownLatch;

public class KVDBBenchmark {

    private static final String HOST = "-h";
    private static final String PORT = "-p";

    // 数据库
    private static final String DB = "-db";
    // 并发数
    private static final String CLIENTS = "-c";
    // 请求总数
    private static final String REQUESTS = "-n";
    // KV 数据字节数
    private static final String KV_DATA_SIZE = "-ds";
    // 是否开启批量操作
    private static final String BATCH = "-b";
    // 一次批量提交数量
    private static final String BATCH_SIZE = "-bs";
    // 长连接
    private static final String KEEPALIVE = "-k";

    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 7078;
    private static final int DEFAULT_CLIENT = 20;
    private static final int DEFAULT_REQUESTS = 100000;
    private static final boolean DEFAULT_BATCH = false;
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final boolean DEFAULT_KEEP_ALIVE = true;
    private static final int DEFAULT_KV_DATA_SIZE = 16;

    public String host;
    public int port;
    public String db;
    public int clients;
    public int requests;
    public boolean batch;
    public int batchSize;
    public int kvDataSize;
    public boolean keepAlive;

    public KVDBBenchmark(String[] args) {
        ArgumentSet arguments = ArgumentSet.resolve(args, ArgumentSet.setting().prefix(HOST, PORT, CLIENTS, REQUESTS, KV_DATA_SIZE, KEEPALIVE, BATCH, BATCH_SIZE, DB));
        ArgumentSet.ArgEntry hostArg = arguments.getArg(HOST);
        if (null != hostArg) {
            this.host = hostArg.getValue();
        } else {
            this.host = DEFAULT_HOST;
        }
        ArgumentSet.ArgEntry portArg = arguments.getArg(PORT);
        if (null != portArg) {
            this.port = Integer.valueOf(portArg.getValue());
        } else {
            this.port = DEFAULT_PORT;
        }
        ArgumentSet.ArgEntry clientsArg = arguments.getArg(CLIENTS);
        if (null != clientsArg) {
            this.clients = Integer.valueOf(clientsArg.getValue());
        } else {
            this.clients = DEFAULT_CLIENT;
        }
        ArgumentSet.ArgEntry requestsArg = arguments.getArg(REQUESTS);
        if (null != requestsArg) {
            this.requests = Integer.valueOf(requestsArg.getValue());
        } else {
            this.requests = DEFAULT_REQUESTS;
        }
        ArgumentSet.ArgEntry kaArg = arguments.getArg(KEEPALIVE);
        if (null != kaArg) {
            this.keepAlive = Boolean.valueOf(kaArg.getValue());
        } else {
            this.keepAlive = DEFAULT_KEEP_ALIVE;
        }
        ArgumentSet.ArgEntry batchArg = arguments.getArg(BATCH);
        if (null != batchArg) {
            this.batch = Boolean.valueOf(batchArg.getValue());
        } else {
            this.batch = DEFAULT_BATCH;
        }
        ArgumentSet.ArgEntry batchSizeArg = arguments.getArg(BATCH_SIZE);
        if (null != batchSizeArg) {
            this.batchSize = Integer.valueOf(batchSizeArg.getValue());
        } else {
            this.batchSize = DEFAULT_BATCH_SIZE;
        }
        ArgumentSet.ArgEntry kvDataSizeArg = arguments.getArg(KV_DATA_SIZE);
        if (null != kvDataSizeArg) {
            this.kvDataSize = Integer.valueOf(kvDataSizeArg.getValue());
        } else {
            this.kvDataSize = DEFAULT_KV_DATA_SIZE;
        }
        ArgumentSet.ArgEntry dbArg = arguments.getArg(DB);
        if (null == dbArg) {
            System.out.println("please set -db parameter");
            System.exit(0);
        } else {
            this.db = dbArg.getValue();
        }
    }

    public static void main(String[] args) {
        try {
            KVDBBenchmark bm = new KVDBBenchmark(args);
            ClientConfig config = new ClientConfig(bm.host, bm.port, bm.db);
            config.setKeepAlive(bm.keepAlive);
            Thread[] runners = new Thread[bm.clients];
            CountDownLatch cdl = new CountDownLatch(runners.length);
            for (int i = 0; i < runners.length; i++) {
                runners[i] = new Thread(new BenchmarkRunner(config, bm, cdl));
            }
            long startTime = System.currentTimeMillis();
            for (int i = 0; i < runners.length; i++) {
                runners[i].start();
            }
            cdl.await();
            long endTime = System.currentTimeMillis();
            String result = String.format("requests:%d, clients:%d, batch:%s, batch_size:%d, kv_data_size:%dbytes, times:%dms, tps:%f",
                    bm.requests * bm.clients, bm.clients, bm.batch, bm.batchSize, bm.kvDataSize, endTime - startTime, bm.requests * bm.clients / ((endTime - startTime) / 1000d));
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
