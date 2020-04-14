package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.ClusterInfo;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class CommandTest {

    @Test
    public void test() throws KVDBException {
        KVDBClient client = new KVDBClient(new ClientConfig("localhost", 7078, "test"));
        ClusterInfo[] info = client.clusterInfo();
        System.out.println(info);
    }
}
