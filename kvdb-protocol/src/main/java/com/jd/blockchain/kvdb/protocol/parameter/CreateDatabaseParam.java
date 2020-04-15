package com.jd.blockchain.kvdb.protocol.parameter;

import com.jd.blockchain.binaryproto.DataContract;
import com.jd.blockchain.binaryproto.DataField;
import com.jd.blockchain.binaryproto.PrimitiveType;
import com.jd.blockchain.kvdb.protocol.Constants;

/**
 * 创建数据库参数
 */
@DataContract(code = Constants.PARAMETER_CREATE_DATABASE)
public interface CreateDatabaseParam {

    /**
     * @return 数据库名称
     */
    @DataField(order = 0, primitiveType = PrimitiveType.TEXT)
    String getName();

    void setName(String name);

    /**
     * @return 数据库根目录
     */
    @DataField(order = 1, primitiveType = PrimitiveType.TEXT)
    String getRootDir();

    void setRootDir(String rootDir);

    /**
     * @return 数据库分片数量
     */
    @DataField(order = 2, primitiveType = PrimitiveType.INT32)
    Integer getPartitions();

    void setPartitions(int partitions);
}
