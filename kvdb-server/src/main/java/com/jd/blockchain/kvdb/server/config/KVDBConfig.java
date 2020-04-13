package com.jd.blockchain.kvdb.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

public class KVDBConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBConfig.class);

    private static final String DEFAULT_DB_PATH = "dbs";
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 7078;
    private static final int DEFAULT_DB_PARTITIONS = 4;

    private String configFile;
    private String host;
    private int port;
    private String dbsRootdir;
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
