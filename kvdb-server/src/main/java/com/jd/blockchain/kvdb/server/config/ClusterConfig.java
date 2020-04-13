package com.jd.blockchain.kvdb.server.config;

import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class ClusterConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfig.class);

    private static final String PROPERTITY_PREFIX = "cluster";
    private static final String PROPERTITY_SEPARATOR = ".";
    private static final String PROPERTITY_PARTITIONS = "partitions";

    private Map<String, String[]> cluster = new TreeMap<>();

    public ClusterConfig(String configFile) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));
        Set<String> clusterNames = new HashSet<>();
        for (Object key : properties.keySet()) {
            clusterNames.add(((String) key).split("\\.")[1]);
        }
        for (String clusterName : clusterNames) {
            int partitions = Integer.parseInt(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + clusterName + PROPERTITY_SEPARATOR + PROPERTITY_PARTITIONS));
            String[] urls = new String[partitions];
            for (int i = 0; i < partitions; i++) {
                urls[i] = properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + clusterName + PROPERTITY_SEPARATOR + i);
            }
            cluster.put(clusterName, urls);
        }
    }

    public Map<String, String[]> getCluster() {
        return cluster;
    }

    /**
     * Compare two cluster settings
     * TODO support localhost
     *
     * @param localCluster
     * @param remoteCluster
     * @return
     */
    public static boolean equals(ClusterInfo[] localCluster, ClusterInfo[] remoteCluster) {
        if (null == localCluster
                || null == remoteCluster
                || localCluster.length == 0
                || localCluster.length != remoteCluster.length) {
            return false;
        }
        Map<String, String[]> localMapping = new HashMap<>();
        Map<String, String[]> remoteMapping = new HashMap<>();
        for (int i = 0; i < localCluster.length; i++) {
            localMapping.put(localCluster[i].getName(), localCluster[i].getURLs());
            remoteMapping.put(remoteCluster[i].getName(), remoteCluster[i].getURLs());
        }
        for (Map.Entry<String, String[]> entry : localMapping.entrySet()) {
            if (!remoteMapping.containsKey(entry.getKey())) {
                return false;
            }
            String[] localURLs = entry.getValue();
            String[] remoteURLs = remoteMapping.get(entry.getKey());
            Arrays.sort(localURLs);
            Arrays.sort(remoteURLs);
            if (!Arrays.equals(localURLs, remoteURLs)) {
                return false;
            }
        }

        return true;
    }
}
