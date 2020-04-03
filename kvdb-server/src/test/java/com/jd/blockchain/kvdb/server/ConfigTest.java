package com.jd.blockchain.kvdb.server;

import org.junit.Assert;
import org.junit.Test;

public class ConfigTest {

    @Test
    public void testConfig() {
        /**
         * Default configs:
         *  server.host=localhost
         *  server.port=6380
         *  buffer_size=1048576
         *  db.path=db
         *  db.size=4
         *  db.partition=4
         */
        ServerConfig config = new ServerConfig();
        Assert.assertEquals("localhost", config.getHost());
        Assert.assertEquals(6380, config.getPort());
        Assert.assertEquals(1048576, config.getBufferSize());
        Assert.assertEquals("db", config.getDbPath());
        Assert.assertEquals(4, config.getDbSize());
        Assert.assertEquals(4, config.getDbPartition());
    }

}
