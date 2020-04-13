package com.jd.blockchain.kvdb.protocol;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.binaryproto.DataContractRegistry;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class KVDBEncoder extends MessageToByteEncoder<Message> {

    static {
        DataContractRegistry.register(Message.class);
        DataContractRegistry.register(Command.class);
        DataContractRegistry.register(MessageContent.class);
        DataContractRegistry.register(Response.class);
        DataContractRegistry.register(DBInfo.class);
        DataContractRegistry.register(ClusterInfo.class);
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Message msg, ByteBuf out) {
        out.writeBytes(BinaryProtocol.encode(msg, Message.class));
    }
}
