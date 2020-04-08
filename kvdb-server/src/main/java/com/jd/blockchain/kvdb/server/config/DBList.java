package com.jd.blockchain.kvdb.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * Default databases auto created after server started.
 */
public class DBList {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBList.class);

    private static final String PROPERTITY_PREFIX = "db";
    private static final String PROPERTITY_SEPARATOR = ".";
    private static final String PROPERTITY_ENABLE = "enable";
    private static final String PROPERTITY_ROOTDIR = "rootdir";
    private static final String PROPERTITY_PARTITIONS = "partitions";

    private String configFile;
    private DBInfo[] dbs;

    public DBList(String configFile, KVDBConfig kvdbConfig) throws IOException {
        this.configFile = configFile;
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));
        Set<String> dbNames = new HashSet<>();
        for (Object key : properties.keySet()) {
            dbNames.add(((String) key).split("\\.")[1]);
        }
        dbs = new DBInfo[dbNames.size()];
        int i = 0;
        for (String dbName : dbNames) {
            DBInfo config = new DBInfo();
            config.setName(dbName);
            config.setEnable(Boolean.parseBoolean(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_ENABLE)));
            config.setDbRootdir(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_ROOTDIR, kvdbConfig.getDbsRootdir()));
            config.setPartitions(Integer.parseInt(properties.getProperty(PROPERTITY_PREFIX + PROPERTITY_SEPARATOR + dbName + PROPERTITY_SEPARATOR + PROPERTITY_PARTITIONS, String.valueOf(kvdbConfig.getDbsPartitions()))));
            dbs[i] = config;
            i++;
        }
    }

    public DBInfo[] getDBInfos() {
        return dbs;
    }

}
