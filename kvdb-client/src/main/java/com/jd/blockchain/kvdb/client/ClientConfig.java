package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.utils.ArgumentSet;

public class ClientConfig {

    private static final String HOST = "-h";
    private static final String PORT = "-p";
    private static final String TIMEOUT = "-t";
    private static final String BUFFER_SIZE = "-bs";
    private static final String RETRY_TIME = "-rt";
    private static final String KEEP_ALIVE = "-k";
    private static final String DB = "-db";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6380;
    private static final int DEFAULT_TIMEOUT = 60000;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_RETRY_TIMES = 5;
    private static final boolean DEFAULT_KEEP_ALIVE = true;
    private static final int DEFAULT_DB = 0;

    private String host;
    private int port;
    private int timeout;
    private int bufferSize;
    private int retryTimes;
    private boolean keepAlive;
    private int db;

    public ClientConfig(String[] args) {
        ArgumentSet arguments = ArgumentSet.resolve(args, ArgumentSet.setting().prefix(HOST, PORT, TIMEOUT, BUFFER_SIZE, RETRY_TIME, KEEP_ALIVE, DB));
        ArgumentSet.ArgEntry hostArg = arguments.getArg(HOST);
        if (null != hostArg) {
            this.host = hostArg.getValue();
        } else {
            this.host = DEFAULT_HOST;
        }
        ArgumentSet.ArgEntry portArg = arguments.getArg(PORT);
        if (null != portArg) {
            this.port = Integer.valueOf(portArg.getValue());
        } else {
            this.port = DEFAULT_PORT;
        }
        ArgumentSet.ArgEntry timeoutArg = arguments.getArg(TIMEOUT);
        if (null != timeoutArg) {
            this.timeout = Integer.valueOf(timeoutArg.getValue());
        } else {
            this.timeout = DEFAULT_TIMEOUT;
        }
        ArgumentSet.ArgEntry bufSizeArg = arguments.getArg(BUFFER_SIZE);
        if (null != bufSizeArg) {
            this.bufferSize = Integer.valueOf(bufSizeArg.getValue());
        } else {
            this.bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ArgumentSet.ArgEntry rtArg = arguments.getArg(RETRY_TIME);
        if (null != rtArg) {
            this.retryTimes = Integer.valueOf(rtArg.getValue());
        } else {
            this.retryTimes = DEFAULT_RETRY_TIMES;
        }
        ArgumentSet.ArgEntry kaArg = arguments.getArg(KEEP_ALIVE);
        if (null != kaArg) {
            this.keepAlive = Integer.valueOf(kaArg.getValue()) == 1 ? true : false;
        } else {
            this.keepAlive = DEFAULT_KEEP_ALIVE;
        }
        ArgumentSet.ArgEntry dbArg = arguments.getArg(DB);
        if (null != dbArg) {
            this.db = Integer.valueOf(dbArg.getValue());
        } else {
            this.db = DEFAULT_DB;
        }
    }

    public ClientConfig(String host, int port) {
        this(host, port, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, DEFAULT_KEEP_ALIVE, DEFAULT_DB);
    }

    public ClientConfig(String host, int port, int db) {
        this(host, port, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, DEFAULT_KEEP_ALIVE, db);
    }

    public ClientConfig(String host, int port, int timeout, int bufferSize, int retryTimes, boolean keepAlive, int db) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.bufferSize = bufferSize;
        this.retryTimes = retryTimes;
        this.keepAlive = keepAlive;
        this.db = db;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public boolean getKeepAlive() {
        return keepAlive;
    }

    public void setKeepAlive(boolean keepAlive) {
        this.keepAlive = keepAlive;
    }

    public int getDb() {
        return db;
    }

    public void setDb(int db) {
        this.db = db;
    }
}
