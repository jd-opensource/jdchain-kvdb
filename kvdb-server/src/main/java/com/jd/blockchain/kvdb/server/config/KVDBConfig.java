package com.jd.blockchain.kvdb.server.config;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

/**
 * kvdb-server配置
 */
public class KVDBConfig {

    // 默认数据库保存路径
    private static final String DEFAULT_DB_PATH = "../dbs";
    // 默认地址
    private static final String DEFAULT_HOST = "0.0.0.0";
    // 默认端口
    private static final int DEFAULT_PORT = 7078;
    // 默认分片数
    private static final int DEFAULT_DB_PARTITIONS = 4;

    // 配置文件完整路径
    private String configFile;
    // 服务器地址
    private String host;
    // 服务器端口
    private int port;
    // 全局数据库保存目录
    private String dbsRootdir;
    // 全局数据实例分片数
    private int dbsPartitions;

    public KVDBConfig(String configFile) throws IOException {
        this.configFile = configFile;
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));
        this.host = properties.getProperty("server.host", DEFAULT_HOST);
        this.port = Integer.parseInt(properties.getProperty("server.port", String.valueOf(DEFAULT_PORT)));
        this.dbsRootdir = properties.getProperty("dbs.rootdir", DEFAULT_DB_PATH);
        this.dbsPartitions = Integer.parseInt(properties.getProperty("dbs.partitions", String.valueOf(DEFAULT_DB_PARTITIONS)));
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDbsRootdir() {
        return dbsRootdir;
    }

    public void setDbsRootdir(String dbsRootdir) {
        this.dbsRootdir = dbsRootdir;
    }

    public int getDbsPartitions() {
        return dbsPartitions;
    }

    public void setDbsPartitions(int dbsPartitions) {
        this.dbsPartitions = dbsPartitions;
    }

    /**
     * 创建数据库，创建数据库所对应目录
     *
     * @param database
     * @throws IOException
     */
    public void createDatabase(String database) throws IOException {
        FileWriter fw = null;
        try {
            fw = new FileWriter(configFile, true);
            fw.write("\n" + String.format("db.%s.enable=true", database));
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (null != fw) {
                    fw.close();
                }
            } catch (IOException e) {
            }
        }
    }
}
