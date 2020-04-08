package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.KVDBURI;
import org.junit.Assert;
import org.junit.Test;

public class URITest {

    @Test
    public void test() {
        Assert.assertTrue(KVDBURI.isLocalhost("localhost"));
        Assert.assertTrue(KVDBURI.isLocalhost("127.0.0.1"));
//        Assert.assertTrue(KVDBURI.isLocalhost("192.168.1.5"));
    }
}
