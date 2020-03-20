package com.jd.blockchain.kvdb.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ServerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConfig.class);

    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final String DEFAULT_DB_PATH = "kvdb";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6380;
    private static final int DEFAULT_DB_SIZE = 8;
    private static final int DEFAULT_DB_PARTITIONS = 5;

    private String host;
    private int port;
    private int bufferSize;
    private String dbPath;
    private int dbSize;
    private int dbPartition;

    public ServerConfig(String configFile) {
        try {
            Properties properties = new Properties();
            properties.load(this.getClass().getClassLoader().getResourceAsStream(configFile));
            this.host = properties.getProperty("server.host", DEFAULT_HOST);
            this.port = Integer.parseInt(properties.getProperty("server.port", String.valueOf(DEFAULT_PORT)));
            this.bufferSize = Integer.parseInt(properties.getProperty("buffer_size", String.valueOf(DEFAULT_BUFFER_SIZE)));
            this.dbPath = properties.getProperty("db.path", DEFAULT_DB_PATH);
            this.dbSize = Integer.parseInt(properties.getProperty("db.size", String.valueOf(DEFAULT_DB_SIZE)));
            this.dbPartition = Integer.parseInt(properties.getProperty("db.partition", String.valueOf(DEFAULT_DB_PARTITIONS)));
        } catch (Exception e) {
            LOGGER.error("load config exception", e);
        }
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getDbPath() {
        return dbPath;
    }

    public void setDbPath(String dbPath) {
        this.dbPath = dbPath;
    }

    public int getDbSize() {
        return dbSize;
    }

    public void setDbSize(int dbSize) {
        this.dbSize = dbSize;
    }

    public int getDbPartition() {
        return dbPartition;
    }

    public void setDbPartition(int dbPartition) {
        this.dbPartition = dbPartition;
    }
}
