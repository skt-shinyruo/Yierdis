package yier.bubu.redis.app.server;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.YierdisMemoryStats;

public class ServerCommandParseIsolationTest {
    @Test
    public void everyServerCommandHandlerParsesWithoutCallingItsProvider() throws Exception {
        AtomicInteger providerCalls = new AtomicInteger();
        CommandRegistry registry = new CommandRegistry();
        new ServerCommandModule(throwingProvider(providerCalls)).register(registry);
        registry.seal();
        List<String[]> fixtures = List.of(
                new String[]{"HELLO", "3", "SETNAME", "client"},
                new String[]{"INFO", "health"},
                new String[]{"STATS"}
        );

        Assert.assertEquals(Set.of("HELLO", "INFO", "STATS"),
                Set.of(registry.upperNamesSorted()));
        for (String[] fixture : fixtures) {
            try (ByteArrayExecutionRequest request = request(fixture)) {
                CommandSpec spec = registry.specByUpperName(fixture[0]);
                Assert.assertNotNull(fixture[0], spec);
                var invocation = spec.handler().parse(new CommandArgs(request));
                Assert.assertNotNull(fixture[0], invocation);
            }
        }
        Assert.assertEquals(0, providerCalls.get());
    }

    private static ServerInfoProvider throwingProvider(AtomicInteger calls) {
        return new ServerInfoProvider() {
            @Override
            public RedisReply info(CommandArgs args, CommandSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parse accessed server info provider");
            }

            @Override
            public RedisReply stats(CommandSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parse accessed server info provider");
            }

            @Override
            public YierdisMemoryStats memoryStats(CommandSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parse accessed server info provider");
            }
        };
    }

    private static ByteArrayExecutionRequest request(String[] values) {
        return ByteArrayExecutionRequest.fromUtf8(
                values[0],
                List.of(java.util.Arrays.copyOfRange(values, 1, values.length))
        );
    }
}
