package com.jd.blockchain.kvdb.protocol.client;

import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.utils.Bytes;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NettyClient implements KVDBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyClient.class);

    private Bootstrap bootstrap;
    private EventLoopGroup workerGroup;
    private ChannelFuture future;
    private ChannelHandlerContext context;

    private final ClientConfig config;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private ConnectedCallback connectedCallback;

    private ConcurrentHashMap<String, ChannelPromise> promises = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Response> responses = new ConcurrentHashMap<>();

    public NettyClient(ClientConfig config) {
        this(config, null);
    }

    public NettyClient(ClientConfig config, ConnectedCallback connectedCallback) {
        this.config = config;
        this.connectedCallback = connectedCallback;
        start();
    }

    protected void start() {
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors());
        bootstrap = new Bootstrap().group(workerGroup)
                .channel(NioSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_RCVBUF, config.getBufferSize())
                .option(ChannelOption.SO_SNDBUF, config.getBufferSize())
                .option(ChannelOption.SO_KEEPALIVE, true)
                .handler(new KVDBInitializerHandler(this));

        future = connect().addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                future.channel().eventLoop().schedule(NettyClient.this::start, 1L, TimeUnit.SECONDS);
            }
        });
    }

    private ChannelFuture connect() {
        LOGGER.info("trying to connect");
        return bootstrap.connect(config.getHost(), config.getPort());
    }

    public void stop() {
        try {
            if (future != null) {
                future.channel().close().syncUninterruptibly();
                future = null;
            }
        } finally {
            if (workerGroup != null) {
                workerGroup.shutdownGracefully().syncUninterruptibly();
                workerGroup = null;
            }
        }
    }

    @Override
    public void channel(SocketChannel channel) {
        LOGGER.info("init channel: {}:{}", config.getHost(), config.getPort());
        channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(config.getBufferSize(), 0, 4, 0, 4))
                .addLast("kvdbDecoder", new KVDBDecoder())
                .addLast(new LengthFieldPrepender(4, 0, false))
                .addLast("kvdbEncoder", new KVDBEncoder())
                .addLast(new KVDBConnectionHandler(this));
    }

    @Override
    public void connected(ChannelHandlerContext ctx) {
        LOGGER.info("channel active");
        this.context = ctx;
        if (null != connectedCallback) {
            executorService.submit(() -> connectedCallback.onConnected());
        }
    }

    @Override
    public void disconnected(ChannelHandlerContext ctx) {
        LOGGER.info("client disconected from server: {}:{}", config.getHost(), config.getPort());
        if (this.context != null) {
            this.context = null;
            if (future != null) {
                future.channel().eventLoop().schedule(this::start, 1L, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    public void receive(ChannelHandlerContext ctx, Message message) {
        ChannelPromise promise = promises.get(message.getId());
        if (null != promise) {
            synchronized (this) {
                promise = promises.get(message.getId());
                if (null != promise) {
                    responses.put(message.getId(), (Response) message.getContent());
                    promise.setSuccess();
                }
            }
        }
    }

    public Response send(Message message) {
        if (context == null) {
            throw new KVDBException("channel context not ready");
        }
        ChannelPromise promise = this.context.newPromise();
        promises.put(message.getId(), promise);
        writeAndFlush(message);
        try {
            promise.await(config.getTimeout());
            Response response = responses.get(message.getId());
            if (null == response) {
                for (int i = 0; i < config.getRetryTimes(); i++) {
                    LOGGER.info("retry, id:{}, times:{}", message.getId(), (i + 1));
                    promise.await(config.getTimeout());
                    response = responses.get(message.getId());
                    if (null != response) {
                        break;
                    }
                }
            }
            return response;
        } catch (InterruptedException e) {
            return new KVDBResponse(Constants.ERROR, Bytes.fromString("interrupted"));
        } finally {
            synchronized (this) {
                promises.remove(message.getId());
                responses.remove(message.getId());
            }
        }
    }

    private void writeAndFlush(Object message) {
        if (context != null) {
            context.writeAndFlush(message);
        }
    }

}
