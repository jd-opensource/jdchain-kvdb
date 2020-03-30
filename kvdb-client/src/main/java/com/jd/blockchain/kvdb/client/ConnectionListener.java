package com.jd.blockchain.kvdb.client;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.util.concurrent.TimeUnit;

public class ConnectionListener implements ChannelFutureListener {
    private final KVDBSingle client;

    public ConnectionListener(KVDBSingle client) {
        this.client = client;
    }

    @Override
    public void operationComplete(ChannelFuture future) {
        if (!future.isSuccess()) {
            future.channel().eventLoop().schedule(client::start, 1L, TimeUnit.SECONDS);
        }
    }
}
