package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.v1.CustomCommand;

import java.util.Locale;

public class CommandRegistryTest {
    @Test
    public void findIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        });

        try (CustomCommand c1 = new CustomCommand("PING", null)) {
            Assert.assertNotNull(registry.find(c1));
        }
        try (CustomCommand c2 = new CustomCommand("ping", null)) {
            Assert.assertNotNull(registry.find(c2));
        }
        try (CustomCommand c3 = new CustomCommand("PiNg", null)) {
            Assert.assertNotNull(registry.find(c3));
        }
    }

    @Test
    public void unknownCommandReturnsNull() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        });

        try (CustomCommand cmd = new CustomCommand("NOPE", null)) {
            Assert.assertNull(registry.find(cmd));
        }
    }

    @Test
    public void duplicateRegistrationIsRejected() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        });
        try {
            registry.register("ping", (cmd, out) -> {
            });
            Assert.fail("expected duplicate registration to throw");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }

    @Test
    public void resizingAndProbingDoesNotBreakLookup() {
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
            registry.register(name, (cmd, out) -> {
            });
        }

        for (String name : names) {
            try (CustomCommand cmd = new CustomCommand(name.toLowerCase(Locale.ROOT), null)) {
                Assert.assertNotNull("expected handler for " + name, registry.find(cmd));
            }
        }
    }
}
