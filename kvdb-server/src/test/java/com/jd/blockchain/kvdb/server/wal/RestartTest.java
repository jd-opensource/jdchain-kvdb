package com.jd.blockchain.kvdb.server.wal;

import com.jd.blockchain.kvdb.server.KVDBServer;
import com.jd.blockchain.kvdb.server.KVDBServerContext;
import com.jd.blockchain.kvdb.server.config.ServerConfig;
import org.junit.Test;

public class RestartTest {

    @Test
    public void testRestart() throws Exception {
        KVDBServerContext context = new KVDBServerContext(new ServerConfig(this.getClass().getResource("/wal").getFile()));
        KVDBServer server = new KVDBServer(context);
        server.start();
    }

}
