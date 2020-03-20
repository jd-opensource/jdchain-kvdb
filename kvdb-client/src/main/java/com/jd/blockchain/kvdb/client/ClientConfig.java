package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.utils.ArgumentSet;

public class ClientConfig {

    private static final String HOST = "-h";
    private static final String PORT = "-p";
    private static final String TIMEOUT = "-t";
    private static final String BUFFER_SIZE = "-bs";
    private static final String RETRY_TIME = "-rt";
    private static final String KEEP_ALIVE = "-k";
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_PORT = 6380;
    private static final int DEFAULT_TIMEOUT = 60000;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_RETRY_TIMES = 5;
    private static final boolean DEFAULT_KEEP_ALIVE = true;

    private String host;
    private int port;
    private int timeout;
    private int bufferSize;
    private int retryTimes;
    private boolean keepAlive;

    public ClientConfig(String[] args) {
        ArgumentSet arguments = ArgumentSet.resolve(args, ArgumentSet.setting().prefix(HOST, PORT, TIMEOUT, BUFFER_SIZE, RETRY_TIME, KEEP_ALIVE));
        ArgumentSet.ArgEntry hostArg = arguments.getArg(HOST);
        if (null != hostArg) {
            this.host = hostArg.getValue();
        } else {
            this.host = DEFAULT_HOST;
        }
        ArgumentSet.ArgEntry portArg = arguments.getArg(PORT);
        if (null != hostArg) {
            this.port = Integer.valueOf(portArg.getValue());
        } else {
            this.port = DEFAULT_PORT;
        }
        ArgumentSet.ArgEntry timeoutArg = arguments.getArg(TIMEOUT);
        if (null != hostArg) {
            this.timeout = Integer.valueOf(timeoutArg.getValue());
        } else {
            this.timeout = DEFAULT_TIMEOUT;
        }
        ArgumentSet.ArgEntry bufSizeArg = arguments.getArg(BUFFER_SIZE);
        if (null != hostArg) {
            this.bufferSize = Integer.valueOf(bufSizeArg.getValue());
        } else {
            this.bufferSize = DEFAULT_BUFFER_SIZE;
        }
        ArgumentSet.ArgEntry rtArg = arguments.getArg(RETRY_TIME);
        if (null != hostArg) {
            this.retryTimes = Integer.valueOf(rtArg.getValue());
        } else {
            this.retryTimes = DEFAULT_RETRY_TIMES;
        }
        ArgumentSet.ArgEntry kaArg = arguments.getArg(KEEP_ALIVE);
        if (null != hostArg) {
            this.keepAlive = Integer.valueOf(kaArg.getValue()) == 1 ? true : false;
        } else {
            this.keepAlive = DEFAULT_KEEP_ALIVE;
        }
    }

    public ClientConfig(String host, int port) {
        this(host, port, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, DEFAULT_KEEP_ALIVE);
    }

    public ClientConfig(String host, int port, int timeout) {
        this(host, port, timeout, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, DEFAULT_KEEP_ALIVE);
    }

    public ClientConfig(String host, int port, boolean keepAlive) {
        this(host, port, DEFAULT_TIMEOUT, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, keepAlive);
    }

    public ClientConfig(String host, int port, int timeout, boolean keepAlive) {
        this(host, port, timeout, DEFAULT_BUFFER_SIZE, DEFAULT_RETRY_TIMES, keepAlive);
    }

    public ClientConfig(String host, int port, int timeout, int bufferSize, int retryTimes, boolean keepAlive) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
        this.bufferSize = bufferSize;
        this.retryTimes = retryTimes;
        this.keepAlive = keepAlive;
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
}
