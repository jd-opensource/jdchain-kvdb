package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

/**
 * 集群配置
 */
@DataContract(code = Constants.CLUSTER_INFO)
public interface ClusterInfo {

    @DataField(order = 1, primitiveType = PrimitiveType.INT32)
    int size();

    @DataField(order = 2, list = true, refContract = true)
    ClusterItem[] getClusterItems();

    /**
     * 匹配集群配置
     *
     * @param localPort
     * @param remoteURI
     * @param remoteClusterInfo
     * @return
     */
    boolean match(int localPort, KVDBURI remoteURI, ClusterInfo remoteClusterInfo);
}
