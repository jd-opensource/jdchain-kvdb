package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.client.partition.Partitioner;
import com.jd.blockchain.kvdb.client.partition.SimpleMurmur3HashPartitioner;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class KVDBCluster implements KVDBClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBCluster.class);
    private ExecutorService executor;
    private Partitioner partition;
    private KVDBSingle[] clients;

    public KVDBCluster(ClientConfig... clientConfigs) throws KVDBException {
        if (null != clientConfigs) {
            partition = new SimpleMurmur3HashPartitioner(clientConfigs.length);
            clients = new KVDBSingle[clientConfigs.length];
            for (int i = 0; i < clientConfigs.length; i++) {
                clients[i] = new KVDBSingle(clientConfigs[i]);
            }
            executor = Executors.newFixedThreadPool(clientConfigs.length);
        } else {
            throw new KVDBException("no client config present");
        }
    }

    @Override
    public void start() {
        for (KVDBSingle client : clients) {
            client.start();
        }
    }

    @Override
    public void stop() {
        for (KVDBSingle client : clients) {
            client.stop();
        }
    }

    @Override
    public boolean select(long timeout, int db) throws KVDBException {
        for (KVDBSingle client : clients) {
            if (!client.select(timeout, db)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean select(int db) throws KVDBException {
        for (KVDBSingle client : clients) {
            if (!client.select(db)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean exists(long timeout, Bytes key) throws KVDBException {
        return clients[partition.partition(key.toBytes())].exists(timeout, key);
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        return clients[partition.partition(key.toBytes())].exists(key);
    }

    /**
     * TODO multiple keys optimization
     * @param timeout
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean[] exists(long timeout, Bytes... keys) throws KVDBException {
        boolean[] oks = new boolean[keys.length];
        int i = 0;
        for (Bytes key : keys) {
            oks[i] = clients[partition.partition(key.toBytes())].exists(timeout, key);
            i++;
        }
        return oks;
    }

    /**
     * TODO multiple keys optimization
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        boolean[] oks = new boolean[keys.length];
        int i = 0;
        for (Bytes key : keys) {
            oks[i] = clients[partition.partition(key.toBytes())].exists(key);
            i++;
        }
        return oks;
    }

    @Override
    public Bytes get(long timeout, Bytes key) throws KVDBException {
        return clients[partition.partition(key.toBytes())].get(timeout, key);
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return clients[partition.partition(key.toBytes())].get(key);
    }

    /**
     * TODO multiple keys optimization
     * @param timeout
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public Bytes[] get(long timeout, Bytes... keys) throws KVDBException {
        Bytes[] vs = new Bytes[keys.length];
        int i = 0;
        for (Bytes key : keys) {
            vs[i] = clients[partition.partition(key.toBytes())].get(timeout, key);
            i++;
        }
        return vs;
    }

    /**
     * TODO multiple keys optimization
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        Bytes[] vs = new Bytes[keys.length];
        int i = 0;
        for (Bytes key : keys) {
            vs[i] = clients[partition.partition(key.toBytes())].get(key);
            i++;
        }
        return vs;
    }

    /**
     * TODO multiple k-k pairs optimization
     * @param timeout
     * @param kvs
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean put(long timeout, Bytes... kvs) throws KVDBException {
        if (kvs.length % 2 != 0) {
            throw new KVDBException("keys and values must in pairs");
        }
        for (int i = 0; i < kvs.length; ) {
            if (!clients[partition.partition(kvs[i].toBytes())].put(timeout, kvs[i], kvs[i + 1])) {
                return false;
            }
            i += 2;
        }
        return true;
    }

    /**
     * TODO multiple keys optimization
     * @param kvs
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean put(Bytes... kvs) throws KVDBException {
        if (kvs.length % 2 != 0) {
            throw new KVDBException("keys and values must in pairs");
        }
        for (int i = 0; i < kvs.length; ) {
            if (!clients[partition.partition(kvs[i].toBytes())].put(kvs[i], kvs[i + 1])) {
                return false;
            }
            i += 2;
        }
        return true;
    }

    @Override
    public boolean batchBegin(long timeout) throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchBegin(timeout);
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchBegin error", e);
                }
            });
        }
        try {
            cdl.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchBegin error", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchBegin();
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchBegin error", e);
                }
            });
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchBegin error", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean batchAbort(long timeout) throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchAbort(timeout);
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchAbort error", e);
                }
            });
        }
        try {
            cdl.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchAbort error", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchAbort();
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchAbort error", e);
                }
            });
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchAbort error", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean batchCommit(long timeout) throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchCommit(timeout);
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchCommit error", e);
                }
            });
        }
        try {
            cdl.await(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchCommit error", e);
            return false;
        }
        return true;
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(clients.length);
        for (int i = 0; i < clients.length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    clients[index].batchCommit();
                    cdl.countDown();
                } catch (KVDBException e) {
                    LOGGER.error("cluster batchCommit error", e);
                }
            });
        }
        try {
            cdl.await();
        } catch (InterruptedException e) {
            LOGGER.error("cluster batchCommit error", e);
            return false;
        }
        return true;
    }
}
