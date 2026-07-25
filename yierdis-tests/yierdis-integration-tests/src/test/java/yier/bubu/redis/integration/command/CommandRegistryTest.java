package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.testutil.TestPreparedCommands;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;

public class CommandRegistryTest {
    @Test
    public void findIsCaseInsensitive() {
        CommandRegistry registry = new CommandRegistry();
        registerNoop(registry, "PING");

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
        registerNoop(registry, "PING");

        try (ExecutionRequest cmd = request("NOPE")) {
            Assert.assertNull(spec(registry, cmd));
        }
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
            registerNoop(registry, name);
        }

        for (String name : names) {
            try (ExecutionRequest cmd = request(name.toLowerCase(Locale.ROOT))) {
                Assert.assertNotNull("expected spec for " + name, spec(registry, cmd));
            }
        }
    }

    private static void registerNoop(CommandRegistry registry, String name) {
        registry.register(new CommandDefinition<>(
                new CommandSyntax(
                        name,
                        CommandArity.exact(1),
                        CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE
                ),
                CommandParsers.request(),
                (request, context) -> TestPreparedCommands.simpleString("OK")
        ));
    }

    private static ExecutionRequest request(String commandName) {
        return ByteArrayExecutionRequest.fromUtf8(commandName, List.of());
    }

    private static Object spec(CommandRegistry registry, ExecutionRequest request) {
        try {
            Method method = CommandRegistry.class.getDeclaredMethod("definition", ExecutionRequest.class);
            method.setAccessible(true);
            return method.invoke(registry, request);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access registry definition", e);
        }
    }
}
