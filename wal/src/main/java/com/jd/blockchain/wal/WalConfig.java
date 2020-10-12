package com.jd.blockchain.wal;

/**
 * WAL 配置参数
 */
public class WalConfig {

    private static final int DEFAULT_FLUSH_INTERVAL = 1;
    private static final boolean DEFAULT_FLUSH_METADATA = false;
    private static final boolean DEFAULT_TRUNCATE_EXISTING = false;
    // 旧日志清理阈值，默认超过100M清理
    private static final long DEFAULT_MAX_CHECKPOINT_SIZE = 104857600;

    private String logFile;
    // 是否清空已存在文件内容
    private boolean truncateExisting;
    // 刷新时间配置， -1跟随系统；0实时同步刷盘；n(n>0)间隔n秒刷盘
    private int flushInterval;
    // 是否刷新元信息
    private boolean flushMetaData;
    // 快照超过多大时清理
    private long maxCheckpointSize;

    public WalConfig(String logFile) {
        this(logFile, DEFAULT_FLUSH_INTERVAL, DEFAULT_FLUSH_METADATA);
    }

    public WalConfig(String logFile, int flushInterval) {
        this(logFile, flushInterval, DEFAULT_FLUSH_METADATA);
    }

    public WalConfig(String logFile, int flushInterval, boolean flushMetaData) {
        this(logFile, flushInterval, flushMetaData, DEFAULT_TRUNCATE_EXISTING);
    }

    public WalConfig(String logFile, int flushInterval, boolean flushMetaData, boolean truncateExisting) {
        this(logFile, flushInterval, flushMetaData, truncateExisting, DEFAULT_MAX_CHECKPOINT_SIZE);
    }

    public WalConfig(String logFile, int flushInterval, boolean flushMetaData, boolean truncateExisting, long maxCheckpointSize) {
        this.logFile = logFile;
        this.flushInterval = flushInterval;
        this.flushMetaData = flushMetaData;
        this.truncateExisting = truncateExisting;
        this.maxCheckpointSize = maxCheckpointSize;
    }

    public String getLogFile() {
        return logFile;
    }

    public int getFlushInterval() {
        return flushInterval;
    }

    public boolean isFlushMetaData() {
        return flushMetaData;
    }

    public boolean isTruncateExisting() {
        return truncateExisting;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public void setTruncateExisting(boolean truncateExisting) {
        this.truncateExisting = truncateExisting;
    }

    public void setFlushInterval(int flushInterval) {
        this.flushInterval = flushInterval;
    }

    public void setFlushMetaData(boolean flushMetaData) {
        this.flushMetaData = flushMetaData;
    }

    public long getMaxCheckpointSize() {
        return maxCheckpointSize;
    }

    public void setMaxCheckpointSize(long maxCheckpointSize) {
        this.maxCheckpointSize = maxCheckpointSize;
    }
}
