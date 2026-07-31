package yier.bubu.redis.integration.command;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.YierdisDbRouter;
import yier.bubu.redis.command.defaults.DefaultCommandModules;
import yier.bubu.redis.command.kernel.CommandRegistries;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.DbIndexSession;
import yier.bubu.redis.storage.api.DbEngine;

public class CommandParseIsolationTest {
    private record ParseCase(String commandName, CommandArgs args) {
    }

    @Test
    public void everyDefaultCommandHandlerParsesWithoutRuntimeServices() throws Exception {
        AtomicInteger routerCalls = new AtomicInteger();
        AtomicInteger providerCalls = new AtomicInteger();
        AtomicInteger governorCalls = new AtomicInteger();
        YierdisDbRouter router = throwingRouter(routerCalls);
        ServerInfoProvider provider = throwingProvider(providerCalls);
        SlowCommandGovernor governor = throwingGovernor(governorCalls);
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(router, provider, governor));
        List<ParseCase> fixtures = validParseCases();
        Set<String> fixtureNames = fixtures.stream()
                .map(ParseCase::commandName)
                .collect(java.util.stream.Collectors.toUnmodifiableSet());

        Assert.assertEquals("parse fixtures must not contain duplicate command names",
                fixtures.size(), fixtureNames.size());
        Assert.assertEquals(Set.of(registry.upperNamesSorted()), fixtureNames);
        for (ParseCase command : fixtures) {
            CommandSpec spec = registry.specByUpperName(command.commandName());
            Assert.assertNotNull(command.commandName(), spec);
            CommandInvocation invocation = spec.handler().parse(command.args());
            Assert.assertNotNull(command.commandName(), invocation);
        }
        CommandSpec memory = registry.specByUpperName("MEMORY");
        Assert.assertNotNull(memory);
        Assert.assertNotNull(memory.handler().parse(argv("MEMORY", "USAGE", "k")));

        Assert.assertEquals(0, routerCalls.get());
        Assert.assertEquals(0, providerCalls.get());
        Assert.assertEquals(0, governorCalls.get());
    }

    @Test
    public void stringCommandHandlersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger()),
                throwingGovernor(new AtomicInteger())
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
    public void keyspaceCommandHandlersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger()),
                throwingGovernor(new AtomicInteger())
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

    @Test
    public void collectionCommandHandlersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger()),
                throwingGovernor(new AtomicInteger())
        ));

        for (CommandArgs invalid : new CommandArgs[]{
                argv("LPOP", "list", "-1"),
                argv("LPOP", "list", "not-a-number"),
                argv("RPOP", "list", "-1"),
                argv("RPOP", "list", "not-a-number"),
                argv("HSCAN", "hash", "-1"),
                argv("HSCAN", "hash", "9223372036854775808"),
                argv("HSCAN", "hash", "0", "MATCH"),
                argv("HSCAN", "hash", "0", "COUNT"),
                argv("HSCAN", "hash", "0", "COUNT", "0"),
                argv("SSCAN", "set", "-1"),
                argv("SSCAN", "set", "9223372036854775808"),
                argv("SSCAN", "set", "0", "MATCH"),
                argv("SSCAN", "set", "0", "COUNT"),
                argv("SSCAN", "set", "0", "COUNT", "0"),
                argv("SSCAN", "set", "0", "NOVALUES")
        }) {
            assertParseFailure(registry, invalid);
        }
    }

    @Test
    public void sortedSetHandlersRejectInvalidUserInputBeforePreparation() {
        CommandRegistry registry = CommandRegistries.from(DefaultCommandModules.create(
                throwingRouter(new AtomicInteger()),
                throwingProvider(new AtomicInteger()),
                throwingGovernor(new AtomicInteger())
        ));

        for (CommandArgs invalid : new CommandArgs[]{
                argv("ZADD", "z", "NaN", "member"),
                argv("ZADD", "z", "Infinity", "member"),
                argv("ZADD", "z", "not-a-score", "member"),
                argv("ZRANGE", "z", "rank", "-1"),
                argv("ZRANGE", "z", "0", "-1", "WITHSCORES", "WITHSCORES"),
                argv("ZREVRANGE", "z", "0", "-1", "UNKNOWN"),
                argv("ZRANGEBYSCORE", "z", "bad", "+inf"),
                argv("ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "bad", "1"),
                argv("ZREMRANGEBYSCORE", "z", "-inf", "bad"),
                argv("ZREMRANGEBYRANK", "z", "bad", "-1"),
                argv("ZSCAN", "z", "-1"),
                argv("ZSCAN", "z", "0", "MATCH"),
                argv("ZSCAN", "z", "0", "COUNT", "0")
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

    private static List<ParseCase> validParseCases() {
        return List.of(
                parseCase("PING", "PING"),
                parseCase("ECHO", "ECHO", "value"),
                parseCase("COMMAND", "COMMAND", "COUNT"),
                parseCase("SELECT", "SELECT", "0"),
                parseCase("QUIT", "QUIT"),
                parseCase("CLIENT", "CLIENT", "GETNAME"),
                parseCase("AUTH", "AUTH", "password"),
                parseCase("FLUSHDB", "FLUSHDB", "SYNC"),
                parseCase("TYPE", "TYPE", "k"),
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
                parseCase("PTTL", "PTTL", "k"),
                parseCase("SET", "SET", "k", "v", "PX", "10", "GET"),
                parseCase("GET", "GET", "k"),
                parseCase("STRLEN", "STRLEN", "k"),
                parseCase("APPEND", "APPEND", "k", "v"),
                parseCase("SETBIT", "SETBIT", "k", "1", "0"),
                parseCase("GETBIT", "GETBIT", "k", "1"),
                parseCase("BITCOUNT", "BITCOUNT", "k", "0", "2"),
                parseCase("INCR", "INCR", "k"),
                parseCase("DECR", "DECR", "k"),
                parseCase("LPUSH", "LPUSH", "list", "a", "b"),
                parseCase("RPUSH", "RPUSH", "list", "a", "b"),
                parseCase("LRANGE", "LRANGE", "list", "0", "-1"),
                parseCase("LPOP", "LPOP", "list", "2"),
                parseCase("RPOP", "RPOP", "list"),
                parseCase("HSET", "HSET", "hash", "field", "value"),
                parseCase("HGET", "HGET", "hash", "field"),
                parseCase("HGETALL", "HGETALL", "hash"),
                parseCase("HLEN", "HLEN", "hash"),
                parseCase("HDEL", "HDEL", "hash", "field"),
                parseCase("HSCAN", "HSCAN", "hash", "0", "MATCH", "f*", "COUNT", "10", "NOVALUES"),
                parseCase("SADD", "SADD", "set", "a", "b"),
                parseCase("SREM", "SREM", "set", "a"),
                parseCase("SMEMBERS", "SMEMBERS", "set"),
                parseCase("SISMEMBER", "SISMEMBER", "set", "a"),
                parseCase("SCARD", "SCARD", "set"),
                parseCase("SSCAN", "SSCAN", "set", "0", "MATCH", "a*", "COUNT", "10"),
                parseCase("ZADD", "ZADD", "z", "1.5", "member"),
                parseCase("ZRANGE", "ZRANGE", "z", "0", "-1", "WITHSCORES", "REV"),
                parseCase("ZREVRANGE", "ZREVRANGE", "z", "0", "-1", "WITHSCORES"),
                parseCase("ZRANGEBYSCORE", "ZRANGEBYSCORE", "z", "-inf", "+inf", "WITHSCORES", "LIMIT", "0", "10"),
                parseCase("ZREVRANGEBYSCORE", "ZREVRANGEBYSCORE", "z", "+inf", "-inf", "LIMIT", "0", "10"),
                parseCase("ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE", "z", "(1", "2"),
                parseCase("ZREMRANGEBYRANK", "ZREMRANGEBYRANK", "z", "0", "-1"),
                parseCase("ZREM", "ZREM", "z", "member"),
                parseCase("ZSCAN", "ZSCAN", "z", "0", "MATCH", "m*", "COUNT", "10"),
                parseCase("PFADD", "PFADD", "h", "value"),
                parseCase("PFCOUNT", "PFCOUNT", "h1", "h2"),
                parseCase("PFMERGE", "PFMERGE", "dest", "source")
        );
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

    private static SlowCommandGovernor throwingGovernor(AtomicInteger calls) {
        return new SlowCommandGovernor() {
            @Override
            public long keysTimeBudgetNanos(CommandSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed slow command governor");
            }

            @Override
            public int keysMaxResults(CommandSession session) {
                calls.incrementAndGet();
                throw new AssertionError("parser accessed slow command governor");
            }
        };
    }
}
