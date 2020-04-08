package com.jd.blockchain.kvdb.protocol;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.SocketChannel;

public interface KVDBHandler {
    void channel(SocketChannel channel);

    void connected(ChannelHandlerContext ctx);

    void disconnected(ChannelHandlerContext ctx);

    void receive(ChannelHandlerContext ctx, Message message);

}
