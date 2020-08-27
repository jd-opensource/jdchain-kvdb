package com.jd.blockchain.kvdb.benchmark;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.ArgumentSet;
import com.jd.blockchain.utils.Bytes;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

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

    private String host;
    private int port;
    private String db;
    private int clients;
    private int requests;
    private boolean batch;
    private int batchSize;
    private int kvDataSize;
    private boolean keepAlive;

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
            AtomicLong requests = new AtomicLong(bm.requests);
            AtomicLong failCount = new AtomicLong(0);
            CountDownLatch startCdl = new CountDownLatch(1);
            CountDownLatch endCdl = new CountDownLatch(bm.clients);
            for (int i = 0; i < bm.clients; i++) {
                final int index = i;
                new Thread(() -> {
                    try {
                        KVDBClient client = new KVDBClient(config);
                        if (bm.batch && bm.keepAlive) {
                            client.batchBegin();
                        }
                        try {
                            startCdl.await();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        long j = 0;
                        while (requests.getAndDecrement() > 0) {
                            try {
                                Bytes kv = randomBytes(bm.kvDataSize);
                                if (bm.batch && bm.keepAlive) {
                                    client.put(kv, kv, true);
                                } else {
                                    client.put(kv, kv);
                                }
                            } catch (KVDBException e) {
                                failCount.incrementAndGet();
                                e.printStackTrace();
                            }
                            j++;
                            if (bm.batch && bm.keepAlive && bm.batchSize == j) {
                                client.batchCommit();
                                j = 0;
                            }
                        }
                        if (j > 0 && bm.batch && bm.keepAlive) {
                            client.batchCommit();
                        }
                        endCdl.countDown();
                        client.close();
                    } catch (KVDBException e) {
                        e.printStackTrace();
                    }
                }).start();
            }

            long startTime = System.currentTimeMillis();
            startCdl.countDown();
            try {
                endCdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            String result = String.format("requests:%d, clients:%d, batch:%s, batch_size:%d, kv_data_size:%dbytes, times:%dms, errors:%d, tps:%f",
                    bm.requests, bm.clients, bm.batch, bm.batchSize, bm.kvDataSize, endTime - startTime, failCount.get(), bm.requests / ((endTime - startTime) / 1000d));
            System.out.println(result);
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static final String STRING = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static Random random = new Random();

    public static Bytes randomBytes(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++)
            sb.append(STRING.charAt(random.nextInt(STRING.length())));

        return Bytes.fromString(sb.toString());
    }

}
