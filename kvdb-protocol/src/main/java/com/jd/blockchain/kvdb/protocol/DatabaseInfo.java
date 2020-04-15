package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;

/**
 * 数据库实例信息
 */
@DataContract(code = Constants.INFO)
public interface DatabaseInfo {

    @DataField(order = 0, primitiveType = PrimitiveType.BOOLEAN)
    boolean isClusterMode();

    @DataField(order = 1, refContract = true)
    ClusterItem getClusterItem();

}
