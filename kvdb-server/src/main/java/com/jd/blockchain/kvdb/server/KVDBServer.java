package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.server.handler.*;
import com.jd.blockchain.utils.ArgumentSet;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Log4J2LoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import static com.jd.blockchain.kvdb.protocol.Command.CommandType.*;

public class KVDBServer implements KVDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBServer.class);

    private static final String CONFIG_FILE = "-c";
    private static final String DEFAULT_CONFIG_FILE = "server.properties";

    private final DefaultServerContext serverContext;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture future;

    public KVDBServer(DefaultServerContext serverContext) {
        this.serverContext = serverContext;
        bindExecutors();
    }

    private void bindExecutors() {
        serverContext.addExecutor(SELECT.getCommand(), new SelectExecutor());
        serverContext.addExecutor(EXISTS.getCommand(), new ExistsExecutor());
        serverContext.addExecutor(GET.getCommand(), new GetExecutor());
        serverContext.addExecutor(PUT.getCommand(), new PutExecutor());
        serverContext.addExecutor(BATCH_BEGIN.getCommand(), new BatchBeginExecutor());
        serverContext.addExecutor(BATCH_ABORT.getCommand(), new BatchAbortExecutor());
        serverContext.addExecutor(BATCH_COMMIT.getCommand(), new BatchCommitExecutor());
    }

    public void start() {
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

        ServerBootstrap bootstrap = new ServerBootstrap();
        InternalLoggerFactory.setDefaultFactory(Log4J2LoggerFactory.INSTANCE);
        bootstrap.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new KVDBInitializerHandler(this))
                .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_RCVBUF, serverContext.getConfig().getBufferSize())
                .option(ChannelOption.SO_SNDBUF, serverContext.getConfig().getBufferSize())
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        future = bootstrap.bind(serverContext.getHost(), serverContext.getPort());
        // Bind and start to accept incoming connections.
        future.syncUninterruptibly();

        LOGGER.info("server started: {}:{}", serverContext.getHost(), serverContext.getPort());
    }

    public void stop() {
        try {
            if (future != null) {
                closeFuture(future.channel().close());
            }
            future = null;
        } finally {
            workerGroup = closeWorker(workerGroup);
            bossGroup = closeWorker(bossGroup);
        }

        serverContext.stop();

        LOGGER.info("server stopped");
    }

    private void closeFuture(Future<?> future) {
        LOGGER.debug("closing future");
        future.syncUninterruptibly();
        LOGGER.debug("future closed");
    }

    private EventLoopGroup closeWorker(EventLoopGroup worker) {
        if (worker != null) {
            closeFuture(worker.shutdownGracefully());
        }
        return null;
    }

    private String sourceKey(Channel channel) {
        InetSocketAddress remoteAddress = (InetSocketAddress) channel.remoteAddress();
        return remoteAddress.getHostName() + ":" + remoteAddress.getPort();
    }

    public void channel(SocketChannel channel) {
        LOGGER.debug("new channel: {}", sourceKey(channel));

        channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                .addLast("kvdbDecoder", new KVDBDecoder())
                .addLast(new LengthFieldPrepender(4, 0, false))
                .addLast("kvdbEncoder", new KVDBEecoder())
                .addLast(new KVDBConnectionHandler(this));
    }

    public void connected(ChannelHandlerContext ctx) {
        String sourceKey = sourceKey(ctx.channel());
        LOGGER.debug("client connected: {}", sourceKey);
        getSession(ctx, sourceKey);
    }

    private Session getSession(ChannelHandlerContext ctx, String sourceKey) {
        return serverContext.getSession(sourceKey, key -> new DefaultSession(key, ctx, serverContext.getDB(0)));
    }

    public void disconnected(ChannelHandlerContext ctx) {
        String sourceKey = sourceKey(ctx.channel());

        LOGGER.debug("client disconnected: {}", sourceKey);

        serverContext.removeSession(sourceKey);
    }

    public void receive(ChannelHandlerContext ctx, Message message) {
        String sourceKey = sourceKey(ctx.channel());

        LOGGER.debug("message received: {}", sourceKey);

        serverContext.processCommand(sourceKey, message);
    }

    public static void main(String[] args) {
        ArgumentSet arguments = ArgumentSet.resolve(args, ArgumentSet.setting().prefix(CONFIG_FILE));
        ArgumentSet.ArgEntry configArg = arguments.getArg(CONFIG_FILE);
        String configFile = DEFAULT_CONFIG_FILE;
        if (null != configArg) {
            configFile = configArg.getValue();
        }
        new KVDBServer(new DefaultServerContext(configFile)).start();
    }
}
