package yier.bubu.redis.client;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.NettyCommandExecutor;
import yier.bubu.redis.YierdisFastCommandHandler;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.db.YierdisDb;
import yier.bubu.redis.protocol.RespBulkString;
import yier.bubu.redis.protocol.RespError;
import yier.bubu.redis.protocol.RespObject;
import yier.bubu.redis.protocol.RespSimpleString;
import yier.bubu.redis.protocol.netty.RespCommandDecoder;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class YierdisClientTest {
    @Test
    public void pingWorks() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                RespObject resp = client.execute(Arrays.asList(b("PING")), 1000);
                Assert.assertTrue(resp instanceof RespSimpleString);
                Assert.assertEquals("PONG", ((RespSimpleString) resp).value());
            }
        }
    }

    @Test
    public void setGetAreBinarySafeOverTcp() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                byte[] key = new byte[]{0, (byte) 0xFF, 'k'};
                byte[] value = new byte[]{0, 1, (byte) 0xFF, '\n'};

                RespObject set = client.execute(Arrays.asList(b("SET"), key, value), 1000);
                Assert.assertTrue(set instanceof RespSimpleString);

                RespObject get = client.execute(Arrays.asList(b("GET"), key), 1000);
                Assert.assertTrue(get instanceof RespBulkString);
                Assert.assertArrayEquals(value, ((RespBulkString) get).data());
            }
        }
    }

    @Test
    public void unknownCommandReturnsRespError() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                RespObject resp = client.execute(Arrays.asList(b("NOPE")), 1000);
                Assert.assertTrue(resp instanceof RespError);
                Assert.assertTrue(((RespError) resp).message().startsWith("ERR unknown command"));
            }
        }
    }

    @Test
    public void executeRejectsNonPositiveTimeout() throws Exception {
        try (TestServer server = TestServer.start()) {
            try (YierdisClient client = YierdisClient.connect("127.0.0.1", server.port())) {
                try {
                    client.execute(Arrays.asList(b("PING")), 0);
                    Assert.fail("Expected IllegalArgumentException");
                } catch (IllegalArgumentException e) {
                    Assert.assertTrue(e.getMessage().contains("timeoutMillis"));
                }
            }
        }
    }

    private static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static final class TestServer implements AutoCloseable {
        private final EventLoopGroup bossGroup;
        private final EventLoopGroup workerGroup;
        private final EventExecutorGroup commandGroup;
        private final NettyCommandExecutor executor;
        private final Channel serverChannel;
        private final YierdisDb db;

        private TestServer(
                EventLoopGroup bossGroup,
                EventLoopGroup workerGroup,
                EventExecutorGroup commandGroup,
                NettyCommandExecutor executor,
                Channel serverChannel,
                YierdisDb db
        ) {
            this.bossGroup = bossGroup;
            this.workerGroup = workerGroup;
            this.commandGroup = commandGroup;
            this.executor = executor;
            this.serverChannel = serverChannel;
            this.db = db;
        }

        static TestServer start() throws InterruptedException {
            YierdisDb db = new YierdisDb();
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(db);

            EventExecutorGroup commandGroup = new DefaultEventExecutorGroup(1);
            NettyCommandExecutor executor = new NettyCommandExecutor(
                    db,
                    processor,
                    commandGroup.next(),
                    1024,
                    0,
                    256,
                    128,
                    0,
                    0,
                    1024,
                    10
            );
            executor.start();

            EventLoopGroup bossGroup = new NioEventLoopGroup(1);
            EventLoopGroup workerGroup = new NioEventLoopGroup(1);
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast("respCommandDecoder", new RespCommandDecoder())
                                    .addLast("commandHandler", new YierdisFastCommandHandler(executor));
                        }
                    });

            Channel serverChannel = bootstrap.bind(0).sync().channel();
            return new TestServer(bossGroup, workerGroup, commandGroup, executor, serverChannel, db);
        }

        int port() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        @Override
        public void close() {
            try {
                serverChannel.close().syncUninterruptibly();
            } finally {
                executor.shutdownGracefully().syncUninterruptibly();
                executor.executor().submit(db::shutdown).syncUninterruptibly();
                commandGroup.shutdownGracefully().syncUninterruptibly();
                bossGroup.shutdownGracefully().syncUninterruptibly();
                workerGroup.shutdownGracefully().syncUninterruptibly();
            }
        }
    }
}
