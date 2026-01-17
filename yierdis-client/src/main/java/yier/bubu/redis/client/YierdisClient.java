package yier.bubu.redis.client;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.protocol.RespFrame;
import yier.bubu.redis.protocol.RespWriter;
import yier.bubu.redis.protocol.netty.NettyRespFrame;
import yier.bubu.redis.protocol.netty.RespDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public final class YierdisClient implements AutoCloseable {
    private final EventLoopGroup group;
    private final Channel channel;
    private final BlockingQueue<NettyRespFrame> responses;

    private final Object requestLock = new Object();
    private volatile boolean closed;

    private YierdisClient(EventLoopGroup group, Channel channel, BlockingQueue<NettyRespFrame> responses) {
        this.group = group;
        this.channel = channel;
        this.responses = responses;
    }

    public static YierdisClient connect(String host, int port) throws InterruptedException {
        Objects.requireNonNull(host, "host");

        EventLoopGroup group = new NioEventLoopGroup(1);
        BlockingQueue<NettyRespFrame> responses = new LinkedBlockingQueue<>();

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("respDecoder", new RespDecoder())
                                .addLast("clientHandler", new ClientHandler(responses));
                    }
                });

        Channel channel = bootstrap.connect(host, port).sync().channel();
        return new YierdisClient(group, channel, responses);
    }

    public RespFrame execute(List<byte[]> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }

        // Simple 1-at-a-time request/response model (no pipelining).
        synchronized (requestLock) {
            if (closed) {
                throw new IllegalStateException("Client is closed");
            }
            if (!channel.isActive()) {
                closed = true;
                throw new IllegalStateException("Connection is not active");
            }
            drainAndCloseResponses();
            ByteBuf out = channel.alloc().buffer();
            try {
                ByteBuf buf = out;
                BytesSink sink = (src, srcIndex, len) -> buf.writeBytes(src, srcIndex, len);
                RespWriter w = new RespWriter(sink);
                w.arrayHeader(args.size());
                for (int i = 0; i < args.size(); i++) {
                    w.bulkString(args.get(i));
                }
                channel.writeAndFlush(out).sync();
                out = null;
            } finally {
                if (out != null) {
                    out.release();
                }
            }

            NettyRespFrame resp = responses.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (resp == null) {
                // RESP 是严格 FIFO 的 request/response 配对。超时意味着连接进入未知状态：
                // 服务端可能稍后返回本次请求的响应，继续复用连接会导致后续请求响应错配。
                closeSilently();
                throw new IllegalStateException("Timeout waiting for response (connection closed to prevent response desync)");
            }
            return resp;
        }
    }

    public RespFrame executeUtf8(List<String> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        List<byte[]> out = new ArrayList<>(args.size());
        for (String a : args) {
            out.add(a == null ? null : a.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
        return execute(out, timeoutMillis);
    }

    @Override
    public void close() {
        closeSilently();
    }

    private void closeSilently() {
        if (closed) {
            return;
        }
        closed = true;
        drainAndCloseResponses();
        try {
            if (channel != null) {
                channel.close().syncUninterruptibly();
            }
        } catch (Throwable ignored) {
            // ignore
        } finally {
            try {
                group.shutdownGracefully();
            } catch (Throwable ignored) {
                // ignore
            }
        }
    }

    private void drainAndCloseResponses() {
        for (; ; ) {
            NettyRespFrame frame = responses.poll();
            if (frame == null) {
                return;
            }
            try {
                frame.close();
            } catch (Throwable ignored) {
                // ignore
            }
        }
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<NettyRespFrame> {
        private final BlockingQueue<NettyRespFrame> responses;

        private ClientHandler(BlockingQueue<NettyRespFrame> responses) {
            this.responses = responses;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyRespFrame msg) {
            responses.offer(msg);
        }
    }
}
