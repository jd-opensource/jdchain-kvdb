package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.config.ClusterConfig;
import com.jd.blockchain.kvdb.server.config.DBList;
import com.jd.blockchain.kvdb.server.config.KVDBConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ConfigTest {

    @Test
    public void testKVDBConfig() throws IOException {
        KVDBConfig config = new KVDBConfig(this.getClass().getResource("/config/kvdb.conf").getFile());
        Assert.assertEquals("0.0.0.0", config.getHost());
        Assert.assertEquals(7078, config.getPort());
        Assert.assertEquals("../dbs", config.getDbsRootdir());
        Assert.assertEquals(4, config.getDbsPartitions());
        Assert.assertFalse(config.isClusterMode());
    }

    @Test
    public void testDBListConfig() throws IOException {
        KVDBConfig kvdbConfig = new KVDBConfig(this.getClass().getResource("/config/kvdb.conf").getFile());
        DBList config = new DBList(this.getClass().getResource("/system/dblist").getFile(), kvdbConfig);
        Assert.assertEquals(2, config.getDBInfos().length);
        Assert.assertTrue(config.getDBInfos()[0].isEnable());
        Assert.assertEquals("../dbs", config.getDBInfos()[0].getDbRootdir());
        Assert.assertEquals(4, config.getDBInfos()[0].getPartitions());
    }

    @Test
    public void testClusterConfig() throws IOException {
        ClusterConfig config = new ClusterConfig(this.getClass().getResource("/config/cluster.conf").getFile());
        Assert.assertEquals(2, config.getCluster().size());
        Assert.assertEquals(2, config.getCluster().get("test").length);
    }

}