package com.jd.blockchain.kvdb.protocol.proto;

import com.jd.binaryproto.DataContract;
import com.jd.binaryproto.DataField;
import com.jd.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

/**
 * 集群配置项
 */
@DataContract(code = Constants.CLUSTER_ITEM)
public interface ClusterItem {

    @DataField(order = 0, primitiveType = PrimitiveType.TEXT)
    String getName();

    @DataField(order = 1, list = true, primitiveType = PrimitiveType.TEXT)
    String[] getURLs();

}
