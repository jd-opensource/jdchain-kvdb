package com.jd.blockchain.kvdb.protocol;

import org.junit.Assert;
import org.junit.Test;

public class URITest {

    String pattern = "(https?|ftp|file)://[-A-Za-z0-9+&@#/%?=~_|!:,.;]+[-A-Za-z0-9+&@#/%=~_|]";

    @Test
    public void testDatabase() {

        KVDBURI uri1 = new KVDBURI("kvdb://localhost:7078/test");
        Assert.assertTrue(uri1.isLocalhost());
    }

    @Test
    public void testLocalhost() {
        KVDBURI uri1 = new KVDBURI("kvdb://localhost:7078/test");
        KVDBURI uri2 = new KVDBURI("kvdb://127.0.0.1:7078/test");
        Assert.assertTrue(uri1.isLocalhost());
        Assert.assertTrue(uri2.isLocalhost());
    }
}
