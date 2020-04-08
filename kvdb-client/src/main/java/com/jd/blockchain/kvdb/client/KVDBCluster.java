package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.client.partition.Partitioner;
import com.jd.blockchain.kvdb.client.partition.SimpleMurmur3HashPartitioner;
import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.Info;
import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KVDBCluster implements KVDBOperator {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBCluster.class);
    private ExecutorService executor;
    private Partitioner partition;

    private volatile String currentDB;
    private Map<String, KVDBSingle[]> operatorsMapping = new HashMap<>();

    // TODO 连接数优化
    public KVDBCluster(String name, ClusterInfo[] cluster) throws KVDBException {
        if (null != cluster || cluster.length == 0) {
            currentDB = name;
            partition = new SimpleMurmur3HashPartitioner(cluster.length);
            int size = 0;
            for (ClusterInfo info : cluster) {
                KVDBSingle[] operators = new KVDBSingle[info.getURLs().length];
                for (int i = 0; i < info.getURLs().length; i++) {
                    size++;
                    KVDBURI uri = new KVDBURI(info.getURLs()[i]);
                    operators[i] = new KVDBSingle(new NettyClient(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase())));
                }
                operatorsMapping.put(info.getName(), operators);
            }
            executor = Executors.newFixedThreadPool(size);
        } else {
            throw new KVDBException("no cluster config present");
        }
    }

    private KVDBSingle[] getCurrentOperators() {
        return operatorsMapping.get(currentDB);
    }

    @Override
    public void close() {
        for (KVDBSingle[] clients : operatorsMapping.values()) {
            for (KVDBSingle client : clients) {
                client.close();
            }
        }
    }

    @Override
    public synchronized boolean use(String db) throws KVDBException {
        if (!operatorsMapping.containsKey(db)) {
            throw new KVDBException("db not config in cluster");
        }

        currentDB = db;

        return true;
    }

    @Override
    public boolean createDatabase(String db) throws KVDBException {
        throw new KVDBException("un support in cluster");
    }

    @Override
    public Info info() throws KVDBException {
        KVDBSingle client = null;
        for (KVDBSingle[] clients : operatorsMapping.values()) {
            client = clients[0];
            break;
        }
        return client.info();
    }

    @Override
    public String[] showDatabases() throws KVDBException {
        return operatorsMapping.keySet().toArray(new String[operatorsMapping.size()]);
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        return getCurrentOperators()[partition.partition(key.toBytes())].exists(key);
    }

    /**
     * TODO multiple keys optimization
     *
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        boolean[] oks = new boolean[keys.length];
        int i = 0;
        for (Bytes key : keys) {
            oks[i] = getCurrentOperators()[partition.partition(key.toBytes())].exists(key);
            i++;
        }
        return oks;
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return getCurrentOperators()[partition.partition(key.toBytes())].get(key);
    }

    /**
     * TODO multiple keys optimization
     *
     * @param keys
     * @return
     * @throws KVDBException
     */
    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        Bytes[] vs = new Bytes[keys.length];
        int i = 0
                ;
        for (Bytes key : keys) {
            vs[i] = getCurrentOperators()[partition.partition(key.toBytes())].get(key);
            i++;
        }
        return vs;
    }

    /**
     * TODO multiple keys optimization
     *
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
            if (!getCurrentOperators()[partition.partition(kvs[i].toBytes())].put(kvs[i], kvs[i + 1])) {
                return false;
            }
            i += 2;
        }
        return true;
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(getCurrentOperators().length);
        for (int i = 0; i < getCurrentOperators().length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    getCurrentOperators()[index].batchBegin();
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
    public boolean batchAbort() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(getCurrentOperators().length);
        for (int i = 0; i < getCurrentOperators().length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    getCurrentOperators()[index].batchAbort();
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
    public boolean batchCommit() throws KVDBException {
        CountDownLatch cdl = new CountDownLatch(getCurrentOperators().length);
        for (int i = 0; i < getCurrentOperators().length; i++) {
            final int index = i;
            executor.execute(() -> {
                try {
                    getCurrentOperators()[index].batchCommit();
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
