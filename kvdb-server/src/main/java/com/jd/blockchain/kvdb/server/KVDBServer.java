package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.binaryproto.BinaryProtocol;
import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.client.ClientConfig;
import com.jd.blockchain.kvdb.protocol.client.NettyClient;
import com.jd.blockchain.kvdb.server.config.ClusterConfig;
import com.jd.blockchain.kvdb.server.executor.*;
import com.jd.blockchain.utils.Bytes;
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
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.jd.blockchain.kvdb.protocol.Command.CommandType.*;

public class KVDBServer implements KVDBHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBServer.class);

    private final DefaultServerContext serverContext;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture future;

    /**
     * Whether this server is ready to service.
     * After cluster confirmed ready will be set to true.
     */
    private boolean ready = false;

    public KVDBServer(DefaultServerContext serverContext) {
        this.serverContext = serverContext;
        bindExecutors();
    }

    private void bindExecutors() {
        serverContext.addExecutor(USE.getCommand(), new UseExecutor());
        serverContext.addExecutor(SHOW_DATABASES.getCommand(), new ShowDatabasesExecutor());
        serverContext.addExecutor(CREATE_DATABASE.getCommand(), new CreateDatabaseExecutor());
        serverContext.addExecutor(CLUSTER_INFO.getCommand(), new ClusterInfoExecutor());
        serverContext.addExecutor(EXISTS.getCommand(), new ExistsExecutor());
        serverContext.addExecutor(GET.getCommand(), new GetExecutor());
        serverContext.addExecutor(PUT.getCommand(), new PutExecutor());
        serverContext.addExecutor(BATCH_BEGIN.getCommand(), new BatchBeginExecutor());
        serverContext.addExecutor(BATCH_ABORT.getCommand(), new BatchAbortExecutor());
        serverContext.addExecutor(BATCH_COMMIT.getCommand(), new BatchCommitExecutor());
        serverContext.addExecutor(UNKNOWN.getCommand(), new UnknowExecutor());
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
                .option(ChannelOption.SO_RCVBUF, 1024 * 1024)
                .option(ChannelOption.SO_SNDBUF, 1024 * 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        future = bootstrap.bind(serverContext.getConfig().getKvdbConfig().getHost(), serverContext.getConfig().getKvdbConfig().getPort());
        // Bind and start to accept incoming connections.
        future.syncUninterruptibly();

        LOGGER.info("server started: {}:{}", serverContext.getConfig().getKvdbConfig().getHost(), serverContext.getConfig().getKvdbConfig().getPort());

        // Confirm cluster settings
        clusterConfirm();

        ready = true;
    }

    private void clusterConfirm() {
        boolean confirmed = false;
        LOGGER.info("cluster confirming ... ");
        ClusterInfo[] localClusterInfo = serverContext.getConfig().getClusterInfoList();
        if (localClusterInfo.length == 0) {
            return;
        }
        while (!confirmed) {
            Set<String> confirmedHosts = new HashSet<>();
            for (ClusterInfo entry : localClusterInfo) {
                boolean ok = true;
                for (String url : entry.getURLs()) {
                    KVDBURI uri = new KVDBURI(url);
                    // Skip self and confirmed
                    if (!(KVDBURI.isLocalhost(uri.getHost()) && uri.getPort() == serverContext.getConfig().getKvdbConfig().getPort())
                            && !confirmedHosts.contains(uri.getHost() + uri.getPort())) {
                        NettyClient client = null;
                        try {
                            LOGGER.info("cluster confirm {}", url);
                            CountDownLatch cdl = new CountDownLatch(1);
                            client = new NettyClient(new ClientConfig(uri.getHost(), uri.getPort(), uri.getDatabase()), () -> cdl.countDown());
                            cdl.await();
                            Response response = client.send(KVDBMessage.clusterInfo());
                            if (null == response || response.getCode() == Constants.ERROR) {
                                ok = false;
                                break;
                            }
                            Bytes[] clusterInfos = response.getResult();
                            ClusterInfo[] infos = new ClusterInfo[clusterInfos.length];
                            for (int i = 0; i < clusterInfos.length; i++) {
                                infos[i] = BinaryProtocol.decodeAs(clusterInfos[i].toBytes(), ClusterInfo.class);
                            }
                            if (!ClusterConfig.equals(localClusterInfo, infos)) {
                                ok = false;
                                break;
                            }
                            confirmedHosts.add(uri.getHost() + uri.getPort());
                        } catch (Exception e) {
                            ok = false;
                            LOGGER.error("cluster confirm {} error", url, e);
                        } finally {
                            if (null != client) {
                                client.stop();
                            }
                        }
                    }
                }
                confirmed = ok;
                if (!ok) {
                    break;
                }
            }
            if (!confirmed) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    LOGGER.error("sleep interrupted", e);
                }
            }
        }
        LOGGER.info("cluster confirmed");
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
                .addLast("kvdbEncoder", new KVDBEncoder())
                .addLast(new KVDBConnectionHandler(this));
    }

    public void connected(ChannelHandlerContext ctx) {
        String sourceKey = sourceKey(ctx.channel());
        LOGGER.debug("client connected: {}", sourceKey);
        getSession(ctx, sourceKey);
    }

    private Session getSession(ChannelHandlerContext ctx, String sourceKey) {
        return serverContext.getSession(sourceKey, key -> new DefaultSession(key, ctx));
    }

    public void disconnected(ChannelHandlerContext ctx) {
        String sourceKey = sourceKey(ctx.channel());

        LOGGER.debug("client disconnected: {}", sourceKey);

        serverContext.removeSession(sourceKey);
    }

    public void receive(ChannelHandlerContext ctx, Message message) {
        String sourceKey = sourceKey(ctx.channel());

        LOGGER.debug("message received: {}", sourceKey);

        /**
         * Only info command can be execute when the server has started but not ready.
         * For the info command may be send in cluster confirming.
         */
        if (!ready && !((Command) message.getContent()).getName().equals(CLUSTER_INFO.getCommand())) {
            ctx.writeAndFlush(KVDBMessage.error(message.getId(), "server not ready"));
        } else {
            serverContext.processCommand(sourceKey, message);
        }
    }
}
