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
import yier.bubu.redis.protocol.RespAttribute;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespObjectParser;
import yier.bubu.redis.protocol.RespPush;
import yier.bubu.redis.protocol.netty.NettyRespFrame;
import yier.bubu.redis.protocol.netty.RespDecoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class YierdisClient implements AutoCloseable {
    private static final int DEFAULT_RESPONSE_QUEUE_CAPACITY = 16;
    private static final int DEFAULT_PUSH_QUEUE_CAPACITY = 16;

    private final EventLoopGroup group;
    private final Channel channel;
    private final BlockingQueue<ResponseEvent> responses;
    private final BlockingQueue<NettyRespFrame> pushes;
    private final AtomicReference<Throwable> terminalError;

    private final Object requestLock = new Object();
    private volatile boolean closed;

    private YierdisClient(
            EventLoopGroup group,
            Channel channel,
            BlockingQueue<ResponseEvent> responses,
            BlockingQueue<NettyRespFrame> pushes,
            AtomicReference<Throwable> terminalError
    ) {
        this.group = group;
        this.channel = channel;
        this.responses = responses;
        this.pushes = pushes;
        this.terminalError = terminalError;
    }

    public static YierdisClient connect(String host, int port) throws InterruptedException {
        Objects.requireNonNull(host, "host");

        EventLoopGroup group = new NioEventLoopGroup(1);
        BlockingQueue<ResponseEvent> responses = new LinkedBlockingQueue<>(DEFAULT_RESPONSE_QUEUE_CAPACITY);
        BlockingQueue<NettyRespFrame> pushes = new LinkedBlockingQueue<>(DEFAULT_PUSH_QUEUE_CAPACITY);
        AtomicReference<Throwable> terminalError = new AtomicReference<>(null);
        AtomicBoolean terminalEnqueued = new AtomicBoolean(false);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast("respDecoder", new RespDecoder())
                                .addLast("clientHandler", new ClientHandler(responses, pushes, terminalError, terminalEnqueued));
                    }
                });

        Channel channel = bootstrap.connect(host, port).sync().channel();
        return new YierdisClient(group, channel, responses, pushes, terminalError);
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
            Throwable terminal = terminalError.get();
            if (terminal != null) {
                closeSilently();
                throw new IllegalStateException("Connection is closed", terminal);
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

            ResponseEvent event = responses.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (event == null) {
                // RESP 是严格 FIFO 的 request/response 配对。超时意味着连接进入未知状态：
                // 服务端可能稍后返回本次请求的响应，继续复用连接会导致后续请求响应错配。
                closeSilently();
                throw new IllegalStateException("Timeout waiting for response (connection closed to prevent response desync)");
            }
            if (event.isTerminal()) {
                closeSilently();
                Throwable err = event.error();
                if (err instanceof RuntimeException re) {
                    throw re;
                }
                throw new IllegalStateException(err == null ? "Connection closed" : err.getMessage(), err);
            }
            return event.frame();
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

    /**
     * Polls an out-of-band RESP3 push message.
     * <p>
     * Push messages may arrive at any time and do not participate in request/response FIFO pairing.
     *
     * @return the received push frame, or {@code null} if timed out
     */
    public RespFrame pollPush(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }
        if (closed) {
            throw new IllegalStateException("Client is closed");
        }
        Throwable terminal = terminalError.get();
        if (terminal != null) {
            closeSilently();
            throw new IllegalStateException("Connection is closed", terminal);
        }
        if (!channel.isActive()) {
            closed = true;
            throw new IllegalStateException("Connection is not active");
        }

        NettyRespFrame frame = pushes.poll(timeoutMillis, TimeUnit.MILLISECONDS);
        return frame;
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
        drainAndClosePushes();
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
            ResponseEvent event = responses.poll();
            if (event == null) {
                return;
            }
            if (event.isTerminal()) {
                continue;
            }
            try {
                event.frame().close();
            } catch (Throwable ignored) {
                // ignore
            }
        }
    }

    private void drainAndClosePushes() {
        for (; ; ) {
            NettyRespFrame frame = pushes.poll();
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

    private static final class ResponseEvent {
        private final NettyRespFrame frame;
        private final Throwable error;

        private ResponseEvent(NettyRespFrame frame, Throwable error) {
            this.frame = frame;
            this.error = error;
        }

        static ResponseEvent frame(NettyRespFrame frame) {
            return new ResponseEvent(Objects.requireNonNull(frame, "frame"), null);
        }

        static ResponseEvent terminal(Throwable error) {
            Throwable e = error == null ? new IllegalStateException("Connection closed") : error;
            return new ResponseEvent(null, e);
        }

        boolean isTerminal() {
            return error != null;
        }

        NettyRespFrame frame() {
            return frame;
        }

        Throwable error() {
            return error;
        }
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<NettyRespFrame> {
        private final BlockingQueue<ResponseEvent> responses;
        private final BlockingQueue<NettyRespFrame> pushes;
        private final AtomicReference<Throwable> terminalError;
        private final AtomicBoolean terminalEnqueued;

        private ClientHandler(
                BlockingQueue<ResponseEvent> responses,
                BlockingQueue<NettyRespFrame> pushes,
                AtomicReference<Throwable> terminalError,
                AtomicBoolean terminalEnqueued
        ) {
            this.responses = responses;
            this.pushes = pushes;
            this.terminalError = terminalError;
            this.terminalEnqueued = terminalEnqueued;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, NettyRespFrame msg) {
            if (msg == null) {
                return;
            }
            if (isPushFrame(msg)) {
                if (!pushes.offer(msg)) {
                    // Drop push messages when the push queue is full; avoid ByteBuf leaks.
                    closeFrameQuietly(msg);
                }
                return;
            }
            if (!responses.offer(ResponseEvent.frame(msg))) {
                // Overflow: close the received frame to avoid ByteBuf leaks, then close the connection to prevent desync.
                closeFrameQuietly(msg);
                signalTerminal(new IllegalStateException("Response queue overflow (connection closed)"));
                ctx.close();
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            signalTerminal(new IllegalStateException("Connection closed"));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            signalTerminal(cause);
            ctx.close();
        }

        private void signalTerminal(Throwable cause) {
            Throwable err = cause == null ? new IllegalStateException("Connection closed") : cause;
            terminalError.compareAndSet(null, err);
            if (!terminalEnqueued.compareAndSet(false, true)) {
                return;
            }
            offerTerminal(err);
        }

        private void offerTerminal(Throwable err) {
            if (responses.offer(ResponseEvent.terminal(err))) {
                return;
            }
            // Best-effort: drop one queued frame to make room for the terminal signal.
            ResponseEvent dropped = responses.poll();
            if (dropped != null && !dropped.isTerminal()) {
                closeFrameQuietly(dropped.frame());
            }
            responses.offer(ResponseEvent.terminal(err));
        }

        private static boolean isPushFrame(NettyRespFrame frame) {
            if (frame == null) {
                return false;
            }
            io.netty.buffer.ByteBuf buf;
            try {
                buf = frame.unwrap();
            } catch (Throwable ignored) {
                buf = null;
            }
            if (buf != null) {
                int i = buf.readerIndex();
                if (i >= 0 && i < buf.writerIndex()) {
                    byte first = buf.getByte(i);
                    if (first == '>') {
                        return true;
                    }
                    if (first != '|') {
                        return false;
                    }
                }
            }
            try {
                RespObject obj = RespObjectParser.parse(frame);
                if (obj instanceof RespPush) {
                    return true;
                }
                if (obj instanceof RespAttribute attr) {
                    return attr.value() instanceof RespPush;
                }
                return false;
            } catch (RuntimeException e) {
                // Best-effort: if we cannot parse the frame, treat it as a normal response and let callers decide.
                return false;
            }
        }

        private static void closeFrameQuietly(NettyRespFrame frame) {
            try {
                frame.close();
            } catch (Throwable ignored) {
                // ignore
            }
        }
    }
}
