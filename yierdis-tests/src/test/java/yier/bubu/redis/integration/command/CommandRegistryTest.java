package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;

import java.util.Locale;

public class CommandRegistryTest {
    @Test
    public void metadataLookupIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING");
        registry.seal();

        Assert.assertNotNull(registry.specByUpperName("PING"));
        Assert.assertNotNull(registry.specByUpperName("ping"));
        Assert.assertNotNull(registry.specByUpperName("PiNg"));
    }

    @Test
    public void unknownCommandReturnsNull() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING");
        registry.seal();

        Assert.assertNull(registry.specByUpperName("NOPE"));
    }

    @Test
    public void duplicateRegistrationIsRejected() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING");
        try {
            registerNoop(registry, "ping");
            Assert.fail("expected duplicate registration to throw");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void manyRegistrationsRemainAvailableForLookup() {
        CommandRegistry registry = new CommandRegistry();

        String[] names = new String[]{
                "PING",
                "ECHO",
                "HELLO",
                "COMMAND",
                "INFO",
                "SELECT",
                "QUIT",
                "FLUSHDB",
                "SET",
                "GET",
                "DEL",
                "EXPIRE",
                "LPUSH",
                "RPUSH",
                "LRANGE",
                "HSET",
                "HGET",
                "SADD",
                "SMEMBERS",
                "ZADD",
                "ZRANGE",
                "PFADD",
                "PFCOUNT"
        };

        for (String name : names) {
            registerNoop(registry, name);
        }
        registry.seal();

        for (String name : names) {
            Assert.assertNotNull(
                    "expected spec for " + name,
                    registry.specByUpperName(name.toLowerCase(Locale.ROOT))
            );
        }
    }

    private static void registerNoop(CommandRegistry registry, String name) {
        registry.register(new CommandSpec(
                new CommandSyntax(
                        name,
                        CommandArity.exact(1),
                        CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE
                ),
                args -> session -> PreparedCommands.ready(RedisReplies.simpleString("OK"))
        ));
    }

}
