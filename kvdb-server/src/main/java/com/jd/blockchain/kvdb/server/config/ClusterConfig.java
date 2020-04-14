package com.jd.blockchain.kvdb.server.config;

import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class ClusterConfig {

    private static final String PROPERTITY_PREFIX = "cluster";
    private static final String PROPERTITY_SEPARATOR = ".";
    private static final String PROPERTITY_PARTITIONS = "partitions";

    private Map<String, String[]> cluster = new HashMap<>();

    public ClusterConfig(String configFile, DBList dbList) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));
        Set<String> clusterNames = new HashSet<>();
        for (Object key : properties.keySet()) {
            clusterNames.add(((String) key).split("\\.")[1]);
        }
        Set<String> databasesInDblist = dbList.getDatabaseNameSet();
        Map<String, String> databaseClusterMapping = new HashMap<>();
        for (String clusterName : clusterNames) {
            int partitions = Integer.parseInt(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + clusterName + PROPERTITY_SEPARATOR + PROPERTITY_PARTITIONS));
            String[] urls = new String[partitions];
            for (int i = 0; i < partitions; i++) {
                urls[i] = properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + clusterName + PROPERTITY_SEPARATOR + i);
                KVDBURI uri = new KVDBURI(urls[i]);
                if (!databasesInDblist.contains(uri.getDatabase())) {
                    throw new KVDBException("database :  " + uri.getDatabase() + " not created or enabled");
                }
                if (databaseClusterMapping.containsKey(urls[i])) {
                    throw new KVDBException("multiple clusters include database : " + uri.getDatabase());
                }
                databaseClusterMapping.put(urls[i], clusterName);
            }
            if (cluster.containsKey(clusterName)) {
                throw new KVDBException("duplicate cluster name : " + clusterName);
            }
            cluster.put(clusterName, urls);
        }
    }

    public Map<String, String[]> getCluster() {
        return cluster;
    }
}
