package com.jd.blockchain.kvdb.client;

import com.jd.blockchain.kvdb.protocol.*;
import com.jd.blockchain.kvdb.protocol.exception.KVDBException;
import com.jd.blockchain.kvdb.protocol.exception.KVDBTimeoutException;
import com.jd.blockchain.utils.Bytes;
import com.jd.blockchain.utils.io.BytesUtils;
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
import java.util.concurrent.TimeUnit;

public class KVDBClient implements KVDB, DBOperations {

    private static final Logger LOGGER = LoggerFactory.getLogger(KVDBClient.class);

    private Bootstrap bootstrap;
    private EventLoopGroup workerGroup;
    private ChannelFuture future;
    private ChannelHandlerContext context;

    private final ClientConfig config;

    private ConcurrentHashMap<String, ChannelPromise> promises = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Response> responses = new ConcurrentHashMap<>();

    public KVDBClient(String host, int port) {
        this.config = new ClientConfig(host, port);
    }

    public KVDBClient(ClientConfig config) {
        this.config = config;
    }

    public void start() {
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

        future = connect().addListener(new ConnectionListener(this));
    }

    private ChannelFuture connect() {
        LOGGER.info("trying to connect");
        return bootstrap.connect(config.getHost(), config.getPort()).syncUninterruptibly();
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
        LOGGER.info("connected to server: {}:{}", config.getHost(), config.getPort());
        channel.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024 * 1024, 0, 4, 0, 4))
                .addLast("kvdbDecoder", new KVDBDecoder())
                .addLast(new LengthFieldPrepender(4, 0, false))
                .addLast("kvdbEncoder", new KVDBEecoder())
                .addLast(new KVDBConnectionHandler(this));
    }

    @Override
    public void connected(ChannelHandlerContext ctx) {
        LOGGER.info("channel active");
        this.context = ctx;
        if (config.getDb() > 0) {
            LOGGER.info("switch to db {}", config.getDb());
            context.writeAndFlush(KVDBMessage.select(Bytes.fromInt(config.getDb())));
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

    public Response send(Message message, long milliseconds) {
        ChannelPromise promise = this.context.newPromise();
        promises.put(message.getId(), promise);
        writeAndFlush(message);
        try {
            promise.await(milliseconds);
            Response response = responses.get(message.getId());
            if (null == response) {
                for (int i = 0; i < config.getRetryTimes(); i++) {
                    LOGGER.info("retry, id:{}, times:{}", message.getId(), (i + 1));
                    promise.await(milliseconds);
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

    @Override
    public boolean select(long timeout, int db) throws KVDBException {
        Response response = send(KVDBMessage.select(Bytes.fromInt(db)), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean select(int db) throws KVDBException {
        return select(config.getTimeout(), db);
    }

    @Override
    public boolean exists(Bytes key) throws KVDBException {
        return exists(config.getTimeout(), key);
    }

    @Override
    public boolean exists(long timeout, Bytes key) throws KVDBException {
        Response response = send(KVDBMessage.exists(key), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return BytesUtils.toInt(response.getResult()[0].toBytes()) == 1;
    }

    @Override
    public boolean[] exists(long timeout, Bytes... keys) throws KVDBException {
        Response response = send(KVDBMessage.exists(keys), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        boolean[] results = new boolean[keys.length];
        for (int i = 0; i < keys.length; i++) {
            results[i] = BytesUtils.toInt(response.getResult()[i].toBytes()) == 1;
        }

        return results;
    }

    @Override
    public boolean[] exists(Bytes... keys) throws KVDBException {
        return exists(config.getTimeout(), keys);
    }

    @Override
    public Bytes get(long timeout, Bytes key) throws KVDBException {
        Response response = send(KVDBMessage.get(key), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return response.getResult()[0];
    }

    @Override
    public Bytes get(Bytes key) throws KVDBException {
        return get(config.getTimeout(), key);
    }

    @Override
    public Bytes[] get(long timeout, Bytes... keys) throws KVDBException {
        Response response = send(KVDBMessage.get(keys), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return response.getResult();
    }

    @Override
    public Bytes[] get(Bytes... keys) throws KVDBException {
        return get(config.getTimeout(), keys);
    }

    @Override
    public boolean put(long timeout, Bytes... kvs) throws KVDBException {
        if (kvs.length % 2 != 0) {
            throw new KVDBException("keys and values must in pairs");
        }
        Response response = send(KVDBMessage.put(kvs), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean put(Bytes... kvs) throws KVDBException {
        return put(config.getTimeout(), kvs);
    }

    @Override
    public boolean batchBegin(long timeout) throws KVDBException {
        Response response = send(KVDBMessage.batchBegin(), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchBegin() throws KVDBException {
        return batchBegin(config.getTimeout());
    }

    @Override
    public boolean batchAbort(long timeout) throws KVDBException {
        Response response = send(KVDBMessage.batchAbort(), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchAbort() throws KVDBException {
        return batchAbort(config.getTimeout());
    }

    @Override
    public boolean batchCommit(long timeout) throws KVDBException {
        Response response = send(KVDBMessage.batchCommit(), timeout);
        if (null == response) {
            throw new KVDBTimeoutException("time out");
        } else if (response.getCode() == Constants.ERROR) {
            throw new KVDBException(BytesUtils.toString(response.getResult()[0].toBytes()));
        }

        return true;
    }

    @Override
    public boolean batchCommit() throws KVDBException {
        return batchCommit(config.getTimeout());
    }
}
