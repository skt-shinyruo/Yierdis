package yier.bubu.redis.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import yier.bubu.redis.protocol.RespArray;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespDecoder;
import yier.bubu.redis.protocol.RespEncoder;
import yier.bubu.redis.protocol.RespObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class YierdisClient implements AutoCloseable {
    private final EventLoopGroup group;
    private final Channel channel;
    private final BlockingQueue<RespObject> responses;

    private final Object requestLock = new Object();

    private YierdisClient(EventLoopGroup group, Channel channel, BlockingQueue<RespObject> responses) {
        this.group = group;
        this.channel = channel;
        this.responses = responses;
    }

    public static YierdisClient connect(String host, int port) throws InterruptedException {
        Objects.requireNonNull(host, "host");

        EventLoopGroup group = new NioEventLoopGroup(1);
        BlockingQueue<RespObject> responses = new LinkedBlockingQueue<>();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("respDecoder", new RespDecoder())
                                .addLast("respEncoder", new RespEncoder())
                                .addLast("clientHandler", new ClientHandler(responses));
                    }
                });

        Channel channel = bootstrap.connect(host, port).sync().channel();
        return new YierdisClient(group, channel, responses);
    }

    public RespObject execute(List<byte[]> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }

        // Simple 1-at-a-time request/response model (no pipelining).
        synchronized (requestLock) {
            responses.clear();
            channel.writeAndFlush(toRespCommand(args)).sync();

            RespObject resp = responses.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (resp == null) {
                throw new IllegalStateException("Timeout waiting for response");
            }
            return resp;
        }
    }

    public RespObject executeUtf8(List<String> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        List<byte[]> out = new ArrayList<>(args.size());
        for (String a : args) {
            out.add(a == null ? null : a.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return execute(out, timeoutMillis);
    }

    private static RespArray toRespCommand(List<byte[]> args) {
        List<RespObject> items = new ArrayList<>(args.size());
        for (byte[] a : args) {
            // Commands are sent as array of bulk strings in RESP2.
            items.add(RespBulkString.ofBytes(a));
        }
        return RespArray.of(items);
    }

    @Override
    public void close() {
        try {
            channel.close().syncUninterruptibly();
        } finally {
            group.shutdownGracefully();
        }
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<RespObject> {
        private final BlockingQueue<RespObject> responses;

        private ClientHandler(BlockingQueue<RespObject> responses) {
            this.responses = responses;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RespObject msg) {
            responses.offer(msg);
        }
    }
}

