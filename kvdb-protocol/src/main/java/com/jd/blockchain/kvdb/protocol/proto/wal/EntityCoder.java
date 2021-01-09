package com.jd.blockchain.kvdb.protocol.proto.wal;

import com.jd.binaryproto.BinaryProtocol;
import com.jd.blockchain.wal.WalDataCoder;

public class EntityCoder implements WalDataCoder<Entity> {

    @Override
    public byte[] encode(Entity entity) {
        return BinaryProtocol.encode(entity, Entity.class);
    }

    @Override
    public Entity decode(byte[] bytes) {
        return BinaryProtocol.decode(bytes);
    }

    public static EntityCoder getInstance() {
        return WalEntityCoderHolder.INSTANCE;
    }

    private static class WalEntityCoderHolder {
        private static final EntityCoder INSTANCE = new EntityCoder();
    }
}
