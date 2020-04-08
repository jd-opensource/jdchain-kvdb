package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

@DataContract(code = Constants.INFO)
public interface Info {

    @DataField(order = 0, primitiveType = PrimitiveType.BOOLEAN)
    boolean isClusterMode();

    @DataField(order = 1, list = true, refContract = true)
    ClusterInfo[] getCluster();

}
