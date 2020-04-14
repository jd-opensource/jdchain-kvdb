package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.KVDBURI;
import org.junit.Assert;
import org.junit.Test;

public class URITest {

    @Test
    public void test() {
        KVDBURI uri1 = new KVDBURI("kvdb://localhost/7078/test");
        KVDBURI uri2 = new KVDBURI("kvdb://127.0.0.1/7078/test");
        Assert.assertTrue(uri1.isLocalhost());
        Assert.assertTrue(uri2.isLocalhost());
    }
}
