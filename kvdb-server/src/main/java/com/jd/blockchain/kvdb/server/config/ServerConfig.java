package com.jd.blockchain.kvdb.server.config;

import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.ClusterItem;
import com.jd.blockchain.kvdb.protocol.KVDBClusterInfo;
import com.jd.blockchain.kvdb.protocol.KVDBClusterItem;
import com.jd.blockchain.utils.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ServerConfig {

    private static final String CONFIG_DIR = "config";
    private static final String SYSTEM_DIR = "system";
    private static final String KVDB_CONFIG = "kvdb.conf";
    private static final String CLUSTER_CONFIG = "cluster.conf";
    private static final String DBLIST = "dblist";

    private KVDBConfig kvdbConfig;
    private ClusterConfig clusterConfig;
    private DBList dbList;

    public ServerConfig(String home) throws IOException {
        String kvdbConfigFile = null;
        String clusterConfigFile = null;
        String dblistFile = null;
        if (!StringUtils.isEmpty(home)) {
            File file = new File(home);
            kvdbConfigFile = file.getAbsolutePath() + File.separator + CONFIG_DIR + File.separator + KVDB_CONFIG;
            clusterConfigFile = file.getAbsolutePath() + File.separator + CONFIG_DIR + File.separator + CLUSTER_CONFIG;
            dblistFile = file.getAbsolutePath() + File.separator + SYSTEM_DIR + File.separator + DBLIST;
        }
        kvdbConfig = new KVDBConfig(kvdbConfigFile);
        clusterConfig = new ClusterConfig(clusterConfigFile);
        dbList = new DBList(dblistFile, kvdbConfig);
    }

    /**
     * Return db information config in dblist
     *
     * @return
     */
    public DBInfo[] getDBInfos() {
        return dbList.getDBInfos();
    }

    /**
     * Return cluster information config in cluster.conf
     *
     * @return
     */
    public Map<String, ClusterItem> getClusterMapping() {
        Map<String, ClusterItem> clusterMapping = new HashMap<>();
        for (Map.Entry<String, String[]> cluster : clusterConfig.getCluster().entrySet()) {
            clusterMapping.put(cluster.getKey(), new KVDBClusterItem(cluster.getKey(), cluster.getValue()));
        }

        return clusterMapping;
    }

    public ClusterInfo getClusterInfoList() {
        Map<String, String[]> clusterMapping = clusterConfig.getCluster();
        ClusterItem[] clusterItems = new ClusterItem[clusterMapping.size()];
        int i = 0;
        for (Map.Entry<String, String[]> cluster : clusterMapping.entrySet()) {
            clusterItems[i] = new KVDBClusterItem(cluster.getKey(), cluster.getValue());
            i++;
        }

        return new KVDBClusterInfo(clusterItems);
    }

    public KVDBConfig getKvdbConfig() {
        return kvdbConfig;
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public DBList getDbList() {
        return dbList;
    }

}
