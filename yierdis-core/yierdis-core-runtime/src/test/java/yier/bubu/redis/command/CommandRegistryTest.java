package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.Command;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class CommandRegistryTest {
    private static final CommandDescriptor PING = CommandDescriptor.of(1, 0, 0, 0);

    @Test
    public void findIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        }, PING);

        try (TestCommand c1 = new TestCommand("PING")) {
            Assert.assertNotNull(registry.find(c1));
        }
        try (TestCommand c2 = new TestCommand("ping")) {
            Assert.assertNotNull(registry.find(c2));
        }
        try (TestCommand c3 = new TestCommand("PiNg")) {
            Assert.assertNotNull(registry.find(c3));
        }
    }

    @Test
    public void unknownCommandReturnsNull() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        }, PING);

        try (TestCommand cmd = new TestCommand("NOPE")) {
            Assert.assertNull(registry.find(cmd));
        }
    }

    @Test
    public void duplicateRegistrationIsRejected() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        }, PING);
        try {
            registry.register("ping", (cmd, out) -> {
            }, PING);
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
            }, CommandDescriptor.of(1, 0, 0, 0));
        }

        for (String name : names) {
            try (TestCommand cmd = new TestCommand(name.toLowerCase(Locale.ROOT))) {
                Assert.assertNotNull("expected handler for " + name, registry.find(cmd));
            }
        }
    }

    private static final class TestCommand implements Command {
        private final byte[] commandName;

        private TestCommand(String commandName) {
            this.commandName = commandName.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public int argc() {
            return 1;
        }

        @Override
        public boolean isNull(int index) {
            return false;
        }

        @Override
        public int len(int index) {
            return commandName.length;
        }

        @Override
        public byte byteAt(int index, int offset) {
            return commandName[offset];
        }

        @Override
        public void copyToByteArray(int index, byte[] dst, int dstOff) {
            System.arraycopy(commandName, 0, dst, dstOff, commandName.length);
        }

        @Override
        public byte[] toByteArray(int index) {
            return commandName.clone();
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
