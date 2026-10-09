package yier.bubu.redis.client.internal;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import yier.bubu.redis.client.ConnectionSettings;
import yier.bubu.redis.client.exception.ConnectionException;
import yier.bubu.redis.protocol.resp.RespClientCodec;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * 一条连接底下的 Netty 通道。帧规则仍调用不依赖 Netty 的 {@link RespClientCodec}。
 * 回复按到达顺序交给 {@link Listener}，调用方线程不读 socket。
 */
public final class CommandChannel {
    public interface Listener {
        void onReply(RespClientCodec.RespReply reply);

        void onTransportFailure(Throwable cause);
    }

    private final EventLoopGroup group;
    private final boolean ownsGroup;
    private final Channel channel;
    private final Listener listener;
    private volatile boolean writable = true;
    private boolean failed;

    private CommandChannel(EventLoopGroup group, boolean ownsGroup, Channel channel, Listener listener) {
        this.group = group;
        this.ownsGroup = ownsGroup;
        this.channel = channel;
        this.listener = listener;
    }

    public static CommandChannel open(
            ConnectionSettings settings,
            EventLoopGroup group,
            boolean ownsGroup,
            long connectTimeoutMillis,
            Listener listener
    ) {
        EventLoopGroup eventLoops = group;
        boolean createdGroup = false;
        if (eventLoops == null) {
            eventLoops = new NioEventLoopGroup(settings.ioThreadCount());
            createdGroup = true;
            ownsGroup = true;
        }
        Listener[] published = new Listener[]{listener};
        try {
            Bootstrap bootstrap = new Bootstrap()
                    .group(eventLoops)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, toTimeoutMillis(connectTimeoutMillis))
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socket) {
                            socket.pipeline().addLast(new ReplyDecoder(published));
                        }
                    });
            ChannelFuture connect = bootstrap.connect(new InetSocketAddress(settings.host(), settings.port()));
            if (!connect.await(connectTimeoutMillis, TimeUnit.MILLISECONDS) || !connect.isSuccess()) {
                Throwable cause = connect.cause() == null
                        ? new IOException("connect timed out")
                        : connect.cause();
                connect.channel().close();
                throw new ConnectionException(
                        "failed to connect to " + settings.host() + ":" + settings.port(), cause);
            }
            return new CommandChannel(eventLoops, ownsGroup || createdGroup, connect.channel(), listener);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            if (createdGroup) {
                eventLoops.shutdownGracefully();
            }
            throw new ConnectionException(
                    "failed to connect to " + settings.host() + ":" + settings.port(), e);
        } catch (RuntimeException e) {
            if (createdGroup) {
                eventLoops.shutdownGracefully();
            }
            throw e;
        }
    }

    public EventLoop eventLoop() {
        return channel.eventLoop();
    }

    public boolean isWritable() {
        return writable && channel.isWritable();
    }

    boolean ownsGroup() {
        return ownsGroup;
    }

    EventLoopGroup group() {
        return group;
    }

    /**
     * 在这条连接的事件循环上写出已经编好的帧。调用方线程的等待在 {@code Connection} 提交时完成，这里不再等一次。
     */
    public void acceptWrite(byte[] payload) {
        if (!channel.eventLoop().inEventLoop()) {
            throw new IllegalStateException("write must run on the connection event loop");
        }
        writeOnEventLoop(payload);
    }

    public void shutdownOwnedGroup() {
        if (!ownsGroup) {
            return;
        }
        if (channel.eventLoop().inEventLoop()) {
            group.shutdownGracefully();
            return;
        }
        group.shutdownGracefully().syncUninterruptibly();
    }

    private void writeOnEventLoop(byte[] payload) {
        if (!channel.isActive()) {
            throw new ConnectionException("connection is closed", null);
        }
        // 高水位之后 isWritable 为 false。这里拒绝新帧，避免在内存里无界排队。
        if (!channel.isWritable()) {
            throw new IllegalStateException("connection is not writable");
        }
        ByteBuf buffer = channel.alloc().buffer(payload.length);
        buffer.writeBytes(payload);
        channel.writeAndFlush(buffer).addListener(future -> {
            if (!future.isSuccess()) {
                failTransport(future.cause());
            }
        });
    }

    private void failTransport(Throwable cause) {
        if (failed) {
            return;
        }
        failed = true;
        writable = false;
        listener.onTransportFailure(cause == null ? new IOException("connection closed") : cause);
        if (channel.isOpen()) {
            channel.close();
        }
    }

    public void closeChannel() {
        if (channel.eventLoop().inEventLoop()) {
            if (channel.isOpen()) {
                channel.close();
            }
            return;
        }
        channel.close().syncUninterruptibly();
    }

    private static int toTimeoutMillis(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        return timeoutMillis > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) timeoutMillis;
    }

    private static final class ReplyDecoder extends ChannelInboundHandlerAdapter {
        private final Listener[] listener;
        private ByteBuf cumulation;

        private ReplyDecoder(Listener[] listener) {
            this.listener = listener;
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            ByteBuf inbound = (ByteBuf) message;
            append(ctx, inbound);
            inbound.release();
            readAvailable(ctx);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            readAvailable(ctx);
            Listener target = listener[0];
            if (target != null) {
                target.onTransportFailure(new IOException("connection closed"));
            }
            releaseCumulation();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            Listener target = listener[0];
            if (target != null) {
                target.onTransportFailure(cause);
            }
            ctx.close();
        }

        private void append(ChannelHandlerContext ctx, ByteBuf inbound) {
            int readable = inbound.readableBytes();
            if (readable == 0) {
                return;
            }
            if (cumulation == null) {
                cumulation = ctx.alloc().buffer(readable);
            } else if (cumulation.writableBytes() < readable) {
                cumulation.capacity(cumulation.writerIndex() + readable);
            }
            cumulation.writeBytes(inbound);
        }

        private void readAvailable(ChannelHandlerContext ctx) {
            Listener target = listener[0];
            if (target == null || cumulation == null) {
                return;
            }
            while (cumulation.isReadable()) {
                int mark = cumulation.readerIndex();
                try {
                    RespClientCodec.RespReply reply = RespClientCodec.readReplyWithRawText(
                            new ByteBufInputStream(cumulation, false),
                            RespProtocolLimits.DEFAULT_MAX_BULK_BYTES
                    );
                    target.onReply(reply);
                } catch (IOException e) {
                    cumulation.readerIndex(mark);
                    if (isIncomplete(e)) {
                        break;
                    }
                    target.onTransportFailure(e);
                    ctx.close();
                    break;
                }
            }
            if (cumulation != null && !cumulation.isReadable()) {
                releaseCumulation();
            }
        }

        private static boolean isIncomplete(IOException failure) {
            String message = failure.getMessage();
            return message != null && message.contains("unexpected EOF");
        }

        private void releaseCumulation() {
            if (cumulation != null) {
                cumulation.release();
                cumulation = null;
            }
        }
    }
}
