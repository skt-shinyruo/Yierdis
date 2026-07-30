package yier.bubu.redis.integration.command;

import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.storage.api.DbEngine;

import static java.util.Map.entry;

public class CommandParseIsolationTest {
    private record ParseCase(String commandName, CommandArgs args) {
    }

    @Test
    public void stringCommandParsersDoNotAccessRuntimeServices() throws Exception {
        AtomicInteger routerCalls = new AtomicInteger();
        AtomicInteger providerCalls = new AtomicInteger();
        YierdisDbRouter router = throwingRouter(routerCalls);
        ServerInfoProvider provider = throwingProvider(providerCalls);
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(router, provider));

        for (Map.Entry<String, CommandArgs> command : Map.ofEntries(
                entry("SET", argv("SET", "k", "v", "PX", "10", "GET")),
                entry("GET", argv("GET", "k")),
                entry("STRLEN", argv("STRLEN", "k")),
                entry("APPEND", argv("APPEND", "k", "v")),
                entry("SETBIT", argv("SETBIT", "k", "1", "0")),
                entry("GETBIT", argv("GETBIT", "k", "1")),
                entry("BITCOUNT", argv("BITCOUNT", "k", "0", "2")),
                entry("INCR", argv("INCR", "k")),
                entry("DECR", argv("DECR", "k"))
        ).entrySet()) {
            CommandSpec spec = registry.specByUpperName(command.getKey());
            Assert.assertNotNull(command.getKey(), spec);
            CommandInvocation invocation = spec.handler().parse(command.getValue());
            Assert.assertNotNull(command.getKey(), invocation);
        }

        Assert.assertEquals(0, routerCalls.get());
        Assert.assertEquals(0, providerCalls.get());
    }

    @Test
    public void stringCommandParsersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger())
        ));

        for (CommandArgs invalid : new CommandArgs[]{
                argv("SET", "k", "v", "NX", "XX"),
                argv("SET", "k", "v", "EX", "0"),
                argv("SETBIT", "k", "-1", "0"),
                argv("SETBIT", "k", "0", "2"),
                argv("GETBIT", "k", "-1"),
                argv("BITCOUNT", "k", "from", "2"),
                argv("BITCOUNT", "k", "0", "to")
        }) {
            assertParseFailure(registry, invalid);
        }
    }

    @Test
    public void keyspaceCommandParsersDoNotAccessRuntimeServices() throws Exception {
        AtomicInteger routerCalls = new AtomicInteger();
        AtomicInteger providerCalls = new AtomicInteger();
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(routerCalls),
                throwingProvider(providerCalls)
        ));

        for (ParseCase command : List.of(
                parseCase("TYPE", "TYPE", "k"),
                parseCase("MEMORY", "MEMORY", "USAGE", "k"),
                parseCase("MEMORY", "MEMORY", "STATS"),
                parseCase("OBJECT", "OBJECT", "ENCODING", "k"),
                parseCase("KEYS", "KEYS", "*"),
                parseCase("SCAN", "SCAN", "0", "MATCH", "k*", "COUNT", "10"),
                parseCase("DEL", "DEL", "a", "b"),
                parseCase("EXISTS", "EXISTS", "a", "b"),
                parseCase("EXPIRE", "EXPIRE", "k", "10"),
                parseCase("PEXPIRE", "PEXPIRE", "k", "10"),
                parseCase("EXPIREAT", "EXPIREAT", "k", "10"),
                parseCase("PEXPIREAT", "PEXPIREAT", "k", "10"),
                parseCase("PERSIST", "PERSIST", "k"),
                parseCase("TTL", "TTL", "k"),
                parseCase("PTTL", "PTTL", "k")
        )) {
            CommandSpec spec = registry.specByUpperName(command.commandName());
            Assert.assertNotNull(command.commandName(), spec);
            Assert.assertNotNull(command.commandName(), spec.handler().parse(command.args()));
        }

        Assert.assertEquals(0, routerCalls.get());
        Assert.assertEquals(0, providerCalls.get());
    }

    @Test
    public void keyspaceCommandParsersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger())
        ));

        for (CommandArgs invalid : new CommandArgs[]{
                argv("MEMORY", "USAGE"),
                argv("MEMORY", "STATS", "extra"),
                argv("MEMORY", "UNKNOWN"),
                argv("OBJECT", "ENCODING"),
                argv("OBJECT", "UNKNOWN", "k"),
                argv("SCAN", "-1"),
                argv("SCAN", "9223372036854775808"),
                argv("SCAN", "0", "MATCH"),
                argv("SCAN", "0", "COUNT"),
                argv("SCAN", "0", "COUNT", "0"),
                argv("SCAN", "0", "COUNT", "2147483648"),
                argv("EXPIRE", "k", "not-a-number"),
                argv("PEXPIRE", "k", "not-a-number"),
                argv("EXPIREAT", "k", "not-a-number"),
                argv("PEXPIREAT", "k", "not-a-number")
        }) {
            assertParseFailure(registry, invalid);
        }
    }

    private static void assertParseFailure(CommandRegistry registry, CommandArgs args) {
        try {
            CommandSpec spec = registry.specByUpperName(args.utf8(0));
            Assert.assertNotNull(spec);
            spec.handler().parse(args);
            Assert.fail("expected parse failure for " + args.utf8(0));
        } catch (CommandParseException expected) {
            Assert.assertNotNull(expected.replyMessage());
        }
    }

    private static CommandArgs argv(String... values) {
        return CommandArgs.of(ByteArrayExecutionRequest.fromUtf8(values[0], java.util.List.of(
                java.util.Arrays.copyOfRange(values, 1, values.length))));
    }

    private static ParseCase parseCase(String commandName, String... values) {
        return new ParseCase(commandName, argv(values));
    }

    private static YierdisDbRouter throwingRouter(AtomicInteger calls) {
        return new YierdisDbRouter() {
            @Override
            public DbEngine dbFor(DbIndexSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed DB router");
            }

            @Override
            public int databases() {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed DB router");
            }
        };
    }

    private static ServerInfoProvider throwingProvider(AtomicInteger calls) {
        return new ServerInfoProvider() {
            @Override
            public yier.bubu.redis.execution.api.RedisReply info(
                    CommandArgs args,
                    yier.bubu.redis.execution.api.CommandSession session
            ) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed server info provider");
            }

            @Override
            public yier.bubu.redis.execution.api.RedisReply stats(
                    yier.bubu.redis.execution.api.CommandSession session
            ) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed server info provider");
            }

            @Override
            public yier.bubu.redis.storage.api.YierdisMemoryStats memoryStats(
                    yier.bubu.redis.execution.api.CommandSession session
            ) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed server info provider");
            }
        };
    }
}
