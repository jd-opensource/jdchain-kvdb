package com.jd.blockchain.kvdb.benchmark;

import com.jd.blockchain.kvdb.client.KVDBClient;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

import utils.Bytes;
import utils.io.BytesUtils;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

public class BenchmarkRunner implements Runnable {

    private KVDBBenchmark bm;
    private KVDBClient client;
    private CountDownLatch cdl;

    public BenchmarkRunner(ClientConfig config, KVDBBenchmark bm, CountDownLatch cdl) throws KVDBException {
        this.bm = bm;
        this.cdl = cdl;
        this.client = new KVDBClient(config);
    }

    @Override
    public void run() {
        try {
            if (bm.batch && bm.keepAlive) {
                client.batchBegin();
            }
            long i = 0;
            long j = 0;
            while (i < bm.requests) {
                i++;
                try {
                    Bytes kv = randomBytes(bm.kvDataSize);
                    if (bm.batch && bm.keepAlive) {
                        client.put(kv, kv, true);
                    } else {
                        client.put(kv, kv);
                    }
                } catch (KVDBException e) {
                    e.printStackTrace();
                }
                j++;
                if (bm.batch && bm.keepAlive && bm.batchSize == j) {
                    client.batchCommit();
                    client.batchBegin();
                    j = 0;
                }
            }
            if (j > 0 && bm.batch && bm.keepAlive) {
                client.batchCommit();
            }

            cdl.countDown();
            client.close();
        } catch (KVDBException e) {
            e.printStackTrace();
        }
    }

    private static final String STRING = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static Random random = new Random();

    public static Bytes randomBytes(int size) {
        if (size <= 0) {
            return new Bytes(BytesUtils.EMPTY_BYTES);
        }
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++)
            sb.append(STRING.charAt(random.nextInt(STRING.length())));

        return Bytes.fromString(sb.toString());
    }
}
