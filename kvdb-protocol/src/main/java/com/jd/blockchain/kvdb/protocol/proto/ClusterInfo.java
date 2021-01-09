package com.jd.blockchain.kvdb.protocol.proto;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;
import com.jd.blockchain.kvdb.protocol.KVDBURI;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;

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
    boolean match(int localPort, KVDBURI remoteURI, ClusterInfo remoteClusterInfo) throws KVDBException;
}
