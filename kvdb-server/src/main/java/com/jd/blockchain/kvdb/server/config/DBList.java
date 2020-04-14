package com.jd.blockchain.kvdb.server.config;

import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * Default databases auto created after server started.
 */
public class DBList {

    private static final String PROPERTITY_PREFIX = "db";
    private static final String PROPERTITY_SEPARATOR = ".";
    private static final String PROPERTITY_ENABLE = "enable";
    private static final String PROPERTITY_ROOTDIR = "rootdir";
    private static final String PROPERTITY_PARTITIONS = "partitions";

    private Map<String, DBInfo> dbs = new HashMap<>();

    public DBList(String configFile, KVDBConfig kvdbConfig) throws IOException {
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));
        Set<String> dbNames = new HashSet<>();
        for (Object key : properties.keySet()) {
            String dbName = ((String) key).split("\\.")[1];
            if (dbNames.contains(dbName)) {
                throw new KVDBException("duplicate database name : " + dbName);
            }
            dbNames.add(dbName);
        }
        for (String dbName : dbNames) {
            DBInfo config = new DBInfo();
            config.setName(dbName);
            config.setEnable(Boolean.parseBoolean(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_ENABLE)));
            if (!config.isEnable()) {
                continue;
            }
            config.setDbRootdir(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_ROOTDIR, kvdbConfig.getDbsRootdir()));
            config.setPartitions(Integer.parseInt(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_PARTITIONS, String.valueOf(kvdbConfig.getDbsPartitions()))));
            dbs.put(dbName, config);
        }
    }

    public DBInfo[] getDatabaseArray() {
        return dbs.values().toArray(new DBInfo[dbs.size()]);
    }

    public Set<String> getDatabaseNameSet() {
        return dbs.keySet();
    }

}
