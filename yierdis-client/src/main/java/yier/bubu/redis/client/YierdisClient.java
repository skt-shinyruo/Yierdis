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
import yier.bubu.redis.protocol.json.JsonValue;
import yier.bubu.redis.protocol.netty.JsonLineDecoder;
import yier.bubu.redis.protocol.v1.CustomProtocolV1ReplyParser;
import yier.bubu.redis.protocol.v1.CustomProtocolV1RequestEncoder;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Netty-based client for Yierdis custom protocol v1.
 * <p>
 * Request framing: {@code <len>:<json>\n}
 * Reply framing: NDJSON (one JSON object per line).
 * <p>
 * This client keeps a simple 1-at-a-time request/response model (no pipelining).
 */
public final class YierdisClient implements AutoCloseable {
    private static final int DEFAULT_RESPONSE_QUEUE_CAPACITY = 16;
    private static final int DEFAULT_MAX_REPLY_LINE_BYTES = 1024 * 1024; // 1 MiB

    private final EventLoopGroup group;
    private final Channel channel;
    private final BlockingQueue<ResponseEvent> responses;
    private final AtomicReference<Throwable> terminalError;

    private final Object requestLock = new Object();
    private volatile boolean closed;

    private YierdisClient(
            EventLoopGroup group,
            Channel channel,
            BlockingQueue<ResponseEvent> responses,
            AtomicReference<Throwable> terminalError
    ) {
        this.group = group;
        this.channel = channel;
        this.responses = responses;
        this.terminalError = terminalError;
    }

    public static YierdisClient connect(String host, int port) throws InterruptedException {
        Objects.requireNonNull(host, "host");

        EventLoopGroup group = new NioEventLoopGroup(1);
        BlockingQueue<ResponseEvent> responses = new LinkedBlockingQueue<>(DEFAULT_RESPONSE_QUEUE_CAPACITY);
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
                                .addLast("jsonLineDecoder", new JsonLineDecoder(DEFAULT_MAX_REPLY_LINE_BYTES))
                                .addLast("clientHandler", new ClientHandler(responses, terminalError, terminalEnqueued));
                    }
                });

        try {
            Channel channel = bootstrap.connect(host, port).sync().channel();
            return new YierdisClient(group, channel, responses, terminalError);
        } catch (Throwable t) {
            try {
                group.shutdownGracefully().syncUninterruptibly();
            } catch (Throwable closeFailure) {
                t.addSuppressed(closeFailure);
            }
            throw t;
        }
    }

    public JsonReply execute(List<byte[]> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0");
        }

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

            // 1-at-a-time request/response model: drain anything unexpected to keep the pairing strict.
            drainAndCloseResponses();

            byte[] frame = CustomProtocolV1RequestEncoder.encodeRequestFrame(args);
            ByteBuf out = channel.alloc().buffer(frame.length);
            try {
                out.writeBytes(frame);
                channel.writeAndFlush(out).sync();
                out = null;
            } finally {
                if (out != null) {
                    out.release();
                }
            }

            ResponseEvent event = responses.poll(timeoutMillis, TimeUnit.MILLISECONDS);
            if (event == null) {
                // Reply pairing is FIFO. A timeout means we can't safely reuse the connection.
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

            byte[] line = event.line();
            try {
                return new JsonReply(CustomProtocolV1ReplyParser.parse(line));
            } catch (RuntimeException e) {
                closeSilently();
                throw new IllegalStateException("Invalid JSON reply (connection closed to prevent desync)", e);
            }
        }
    }

    public JsonReply executeUtf8(List<String> args, long timeoutMillis) throws InterruptedException {
        Objects.requireNonNull(args, "args");
        List<byte[]> out = new ArrayList<>(args.size());
        for (String a : args) {
            out.add(a == null ? null : a.getBytes(StandardCharsets.UTF_8));
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
            ResponseEvent event = responses.poll();
            if (event == null) {
                return;
            }
        }
    }

    public record JsonReply(byte[] line, JsonValue envelope) {
        public JsonReply {
            Objects.requireNonNull(line, "line");
            Objects.requireNonNull(envelope, "envelope");
            line = line.clone();
        }

        public JsonReply(CustomProtocolV1ReplyParser.ParsedReply reply) {
            this(reply.line(), reply.envelope());
        }

        @Override
        public byte[] line() {
            return line.clone();
        }

        public String lineUtf8() {
            return new String(line, StandardCharsets.UTF_8);
        }
    }

    private static final class ResponseEvent {
        private final byte[] line;
        private final Throwable error;

        private ResponseEvent(byte[] line, Throwable error) {
            this.line = line;
            this.error = error;
        }

        static ResponseEvent line(byte[] line) {
            return new ResponseEvent(Objects.requireNonNull(line, "line"), null);
        }

        static ResponseEvent terminal(Throwable error) {
            Throwable e = error == null ? new IllegalStateException("Connection closed") : error;
            return new ResponseEvent(null, e);
        }

        boolean isTerminal() {
            return error != null;
        }

        byte[] line() {
            return line;
        }

        Throwable error() {
            return error;
        }
    }

    private static final class ClientHandler extends SimpleChannelInboundHandler<byte[]> {
        private final BlockingQueue<ResponseEvent> responses;
        private final AtomicReference<Throwable> terminalError;
        private final AtomicBoolean terminalEnqueued;

        private ClientHandler(
                BlockingQueue<ResponseEvent> responses,
                AtomicReference<Throwable> terminalError,
                AtomicBoolean terminalEnqueued
        ) {
            this.responses = Objects.requireNonNull(responses, "responses");
            this.terminalError = Objects.requireNonNull(terminalError, "terminalError");
            this.terminalEnqueued = Objects.requireNonNull(terminalEnqueued, "terminalEnqueued");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, byte[] msg) {
            if (msg == null) {
                return;
            }
            if (!responses.offer(ResponseEvent.line(msg))) {
                Throwable overflow = new IllegalStateException("Response queue overflow");
                if (terminalError.compareAndSet(null, overflow)) {
                    enqueueTerminal(overflow);
                }
                if (ctx != null) {
                    ctx.close();
                }
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            enqueueTerminal(terminalError.get());
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (terminalError.compareAndSet(null, cause)) {
                enqueueTerminal(cause);
            }
            if (ctx != null) {
                ctx.close();
            }
        }

        private void enqueueTerminal(Throwable cause) {
            if (!terminalEnqueued.compareAndSet(false, true)) {
                return;
            }
            responses.offer(ResponseEvent.terminal(cause));
        }
    }
}
