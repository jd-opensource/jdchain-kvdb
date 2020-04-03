package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.utils.io.FileUtils;
import org.junit.Test;

import java.io.File;

public class ServerTest {

    @Test
    public void test() {
        DefaultServerContext context = new DefaultServerContext();
        KVDBServer server = new KVDBServer(context);
        server.start();
        server.stop();

        FileUtils.deletePath(new File(context.getConfig().getDbPath()), true);
    }
}
