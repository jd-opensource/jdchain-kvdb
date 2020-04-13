package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

@Ignore
public class ServerTest {

    @Test
    public void test() throws Exception {
        DefaultServerContext context = new DefaultServerContext(new ServerConfig(this.getClass().getResource("/").getFile()));
        KVDBServer server = new KVDBServer(context);
        server.start();
        server.stop();

        FileUtils.deletePath(new File(context.getConfig().getKvdbConfig().getDbsRootdir()), true);
    }
}
