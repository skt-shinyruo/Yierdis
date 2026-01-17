package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespCommandBuilder;
import yier.bubu.redis.protocol.RespFrame;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

public class CommandRegistryTest {
    @Test
    public void findIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        });

        try (RespCommand c1 = command("PING")) {
            Assert.assertNotNull(registry.find(c1));
        }
        try (RespCommand c2 = command("ping")) {
            Assert.assertNotNull(registry.find(c2));
        }
        try (RespCommand c3 = command("PiNg")) {
            Assert.assertNotNull(registry.find(c3));
        }
    }

    @Test
    public void unknownCommandReturnsNull() {
        CommandRegistry registry = new CommandRegistry();
        registry.register("PING", (cmd, out) -> {
        });

        try (RespCommand cmd = command("NOPE")) {
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
            try (RespCommand cmd = command(name.toLowerCase(Locale.ROOT))) {
                Assert.assertNotNull("expected handler for " + name, registry.find(cmd));
            }
        }
    }

    private static RespCommand command(String nameAscii) {
        byte[] bytes = nameAscii.getBytes(StandardCharsets.US_ASCII);
        RespCommand cmd = RespCommandBuilder.acquire(1);
        RespFrame frame = new ByteArrayRespFrame(bytes);
        RespCommandBuilder.setFrame(cmd, frame);
        RespCommandBuilder.setArgSlice(cmd, 0, 0, bytes.length);
        return cmd;
    }

    private static final class ByteArrayRespFrame implements RespFrame {
        private final byte[] bytes;

        private ByteArrayRespFrame(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public int length() {
            return bytes.length;
        }

        @Override
        public byte getByte(int index) {
            return bytes[index];
        }

        @Override
        public void getBytes(int index, byte[] dst, int dstOff, int len) {
            System.arraycopy(bytes, index, dst, dstOff, len);
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
