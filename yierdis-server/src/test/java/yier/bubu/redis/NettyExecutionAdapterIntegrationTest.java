package yier.bubu.redis;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.YierdisFastCommandProcessor;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.executor.CommandExecutor;
import yier.bubu.redis.executor.CommandExecutorConfig;
import yier.bubu.redis.executor.SchedulingPolicy;
import yier.bubu.redis.protocol.v1.JsonLineReplyWriterFactory;
import yier.bubu.redis.runtime.YierdisInstance;
import yier.bubu.redis.runtime.YierdisInstanceConfig;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class NettyExecutionAdapterIntegrationTest {
    @Test
    public void handlerSubmitsThroughNettyExecutionConnection() {
        try (YierdisInstance instance = YierdisInstance.create(YierdisInstanceConfig.builder().build())) {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(TestDbRouters.forInstance(instance), null);
            NettyExecutionIoAdapter ioAdapter = new NettyExecutionIoAdapter();
            CommandExecutor<NettyExecutionConnection> executor = new CommandExecutor<>(
                    instance::bindToCurrentThread,
                    processor,
                    Runnable::run,
                    new JsonLineReplyWriterFactory(),
                    ioAdapter,
                    new CommandExecutorConfig(16, 0, 256, 128, 0, 0, 128, 10, SchedulingPolicy.FAIR)
            );
            executor.start();

            EmbeddedChannel channel = new EmbeddedChannel(
                    new YierdisFastCommandHandler(executor, new JsonLineReplyWriterFactory())
            );
            try {
                NettyExecutionConnection.getOrCreate(channel, 16, 1024);
                channel.writeInbound(ByteArrayExecutionRequest.fromUtf8("PING", List.of()));

                Assert.assertArrayEquals(
                        "{\"ok\":true,\"result\":\"PONG\"}\n".getBytes(StandardCharsets.UTF_8),
                        readOutbound(channel)
                );
            } finally {
                channel.finishAndReleaseAll();
                executor.close();
            }
        }
    }

    private static byte[] readOutbound(EmbeddedChannel channel) {
        ByteBuf out = channel.readOutbound();
        Assert.assertNotNull("expected reply", out);
        try {
            byte[] bytes = new byte[out.readableBytes()];
            out.readBytes(bytes);
            return bytes;
        } finally {
            out.release();
        }
    }
}
