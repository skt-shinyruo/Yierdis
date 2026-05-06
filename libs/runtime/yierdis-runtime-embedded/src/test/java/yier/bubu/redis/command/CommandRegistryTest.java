package yier.bubu.redis.command;

import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandParsers;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.contract.ByteArrayExecutionRequest;
import yier.bubu.redis.contract.ExecutionRequest;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;

public class CommandRegistryTest {
    private static final CommandDescriptor PING = CommandDescriptor.of(1, 0, 0, 0);

    @Test
    public void findIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING", PING);

        try (ExecutionRequest c1 = request("PING")) {
            Assert.assertNotNull(spec(registry, c1));
        }
        try (ExecutionRequest c2 = request("ping")) {
            Assert.assertNotNull(spec(registry, c2));
        }
        try (ExecutionRequest c3 = request("PiNg")) {
            Assert.assertNotNull(spec(registry, c3));
        }
    }

    @Test
    public void unknownCommandReturnsNull() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING", PING);

        try (ExecutionRequest cmd = request("NOPE")) {
            Assert.assertNull(spec(registry, cmd));
        }
    }

    @Test
    public void duplicateRegistrationIsRejected() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING", PING);
        try {
            registerNoop(registry, "ping", PING);
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
            registerNoop(registry, name, CommandDescriptor.of(1, 0, 0, 0));
        }

        for (String name : names) {
            try (ExecutionRequest cmd = request(name.toLowerCase(Locale.ROOT))) {
                Assert.assertNotNull("expected spec for " + name, spec(registry, cmd));
            }
        }
    }

    private static void registerNoop(CommandRegistry registry, String name, CommandDescriptor descriptor) {
        registry.register(
                name,
                descriptor,
                CommandParsers.exactRequest(1, name.toLowerCase(Locale.ROOT)),
                (request, ctx) -> {
                }
        );
    }

    private static ExecutionRequest request(String commandName) {
        return ByteArrayExecutionRequest.fromUtf8(commandName, List.of());
    }

    private static Object spec(CommandRegistry registry, ExecutionRequest request) {
        try {
            Method method = CommandRegistry.class.getDeclaredMethod("spec", ExecutionRequest.class);
            method.setAccessible(true);
            return method.invoke(registry, request);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access registry spec", e);
        }
    }
}
