package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.Info;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CommandTest {

    @Test
    public void test() throws KVDBException, InterruptedException {
        KVDBClient client = new KVDBClient(new ClientConfig("localhost", 7078, "test"));
        Info info = client.info();
        System.out.println(info);
    }
}
