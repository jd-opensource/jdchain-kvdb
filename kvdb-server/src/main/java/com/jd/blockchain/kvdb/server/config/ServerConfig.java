package com.jd.blockchain.kvdb.server.config;

import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.KVDBClusterInfo;
import com.jd.blockchain.utils.StringUtils;

import java.io.File;
import java.io.IOException;
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
        if (kvdbConfig.isClusterMode()) {
            clusterConfig = new ClusterConfig(clusterConfigFile);
        }
        dbList = new DBList(dblistFile, kvdbConfig);
    }

    /**
     * Whether in cluster mode.
     *
     * @return
     */
    public boolean isClusterMode() {
        return kvdbConfig.isClusterMode();
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
    public Map<String, String[]> getClusterMapping() {
        return clusterConfig.getCluster();
    }

    public ClusterInfo[] getClusterInfoList() {
        Map<String, String[]> clusterMapping = getClusterMapping();
        ClusterInfo[] clusterInfos = new ClusterInfo[clusterMapping.size()];
        int i = 0;
        for (Map.Entry<String, String[]> cluster : clusterMapping.entrySet()) {
            KVDBClusterInfo clusterInfo = new KVDBClusterInfo();
            clusterInfo.setName(cluster.getKey());
            clusterInfo.setURLs(cluster.getValue());
            clusterInfos[i] = clusterInfo;
            i++;
        }

        return clusterInfos;
    }

    /**
     * Return URLs in the giving cluster
     *
     * @return
     */
    public String[] getClusterURLs(String clusterName) {
        return getClusterMapping().get(clusterName);
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
