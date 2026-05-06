package yier.bubu.redis.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandParsers;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandDescriptorRegistryTest {
    @Test
    public void registryExposesDescriptorAndMultiPolicyThroughUnifiedSpec() throws Exception {
        CommandRegistry registry = new CommandRegistry();
        registry.registerDisallowedInMulti(
                "HELLO",
                CommandDescriptor.of(-1, 0, 0, 0),
                CommandParsers.minRequest(1, "hello"),
                (cmd, ctx) -> ctx.out().simpleString("OK"),
                "ERR HELLO is not allowed in MULTI"
        );

        Method specByUpperName;
        try {
            specByUpperName = CommandRegistry.class.getDeclaredMethod("specByUpperName", String.class);
        } catch (NoSuchMethodException e) {
            Assert.fail("CommandRegistry should expose specByUpperName(String)");
            return;
        }
        specByUpperName.setAccessible(true);

        Object spec = specByUpperName.invoke(registry, "HELLO");
        Assert.assertNotNull(spec);

        Method descriptorMethod;
        Method disallowedInMultiMethod;
        try {
            descriptorMethod = spec.getClass().getDeclaredMethod("descriptor");
            disallowedInMultiMethod = spec.getClass().getDeclaredMethod("disallowedInMultiError");
        } catch (NoSuchMethodException e) {
            Assert.fail("Command spec should expose descriptor() and disallowedInMultiError()");
            return;
        }

        CommandDescriptor descriptor = (CommandDescriptor) descriptorMethod.invoke(spec);
        Assert.assertNotNull(descriptor);
        Assert.assertEquals(-1, descriptor.arity());
        Assert.assertEquals(0, descriptor.firstKeyIndex());
        Assert.assertEquals(0, descriptor.lastKeyIndex());
        Assert.assertEquals(0, descriptor.keyStep());
        Assert.assertEquals("ERR HELLO is not allowed in MULTI", disallowedInMultiMethod.invoke(spec));
    }

    @Test
    public void commandInfoUsesDescriptorFromRegistryRegistration() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(
                    db,
                    registration -> registration.register(
                            "HELLO",
                            CommandDescriptor.of(7, 2, 2, 1),
                            CommandParsers.minRequest(1, "hello"),
                            (cmd, ctx) -> ctx.out().simpleString("OK")
                    )
            );

            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyArray info = (ReplyArray) client.execute(Arrays.asList(
                        b("COMMAND"),
                        b("INFO"),
                        b("HELLO")
                ));

                Assert.assertNotNull(info.values());
                Assert.assertEquals(1, info.values().size());
                assertCommandInfo(info.values().get(0), "hello", 7, 2, 2, 1);
            }
        });
    }

    @Test
    public void registrySpecsRemainAuthoritativeForBuiltInAndExtraMetadata() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(
                    db,
                    registration -> {
                        registration.register(
                                "INFO",
                                CommandDescriptor.of(-1, 0, 0, 0),
                                CommandParsers.minRequest(1, "info"),
                                (cmd, ctx) -> ctx.out().simpleString("OK")
                        );
                        registration.registerDisallowedInMulti(
                                "HELLO",
                                CommandDescriptor.of(-1, 0, 0, 0),
                                CommandParsers.minRequest(1, "hello"),
                                (cmd, ctx) -> ctx.out().simpleString("OK"),
                                "ERR HELLO is not allowed in MULTI"
                        );
                    }
            );
            CommandRegistry registry = registryOf(processor);
            Assert.assertEquals(-1, specByUpperName(registry, "INFO").descriptor().arity());
            Assert.assertEquals("ERR HELLO is not allowed in MULTI", specByUpperName(registry, "HELLO").disallowedInMultiError());
            Assert.assertNotNull(specByUpperName(registry, "SET").descriptor());
        });
    }

    private static void assertCommandInfo(
            ReplyObject reply,
            String expectedName,
            long expectedArity,
            long expectedFirstKey,
            long expectedLastKey,
            long expectedStep
    ) {
        Assert.assertTrue(reply instanceof ReplyArray);
        ReplyArray info = (ReplyArray) reply;
        Assert.assertNotNull(info.values());
        Assert.assertEquals(6, info.values().size());
        Assert.assertEquals(expectedName, ((ReplyBulkString) info.values().get(0)).asString());
        Assert.assertEquals(expectedArity, ((ReplyInteger) info.values().get(1)).value());

        ReplyArray flags = (ReplyArray) info.values().get(2);
        Assert.assertNotNull(flags.values());
        Assert.assertTrue(flags.values().isEmpty());

        Assert.assertEquals(expectedFirstKey, ((ReplyInteger) info.values().get(3)).value());
        Assert.assertEquals(expectedLastKey, ((ReplyInteger) info.values().get(4)).value());
        Assert.assertEquals(expectedStep, ((ReplyInteger) info.values().get(5)).value());
    }

    private static CommandRegistry registryOf(YierdisFastCommandProcessor processor) {
        try {
            Field field = YierdisFastCommandProcessor.class.getDeclaredField("registry");
            field.setAccessible(true);
            return (CommandRegistry) field.get(processor);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access processor registry", e);
        }
    }

    private static yier.bubu.redis.command.api.CommandSpec<?> specByUpperName(CommandRegistry registry, String name) {
        try {
            Method method = CommandRegistry.class.getDeclaredMethod("specByUpperName", String.class);
            method.setAccessible(true);
            return (yier.bubu.redis.command.api.CommandSpec<?>) method.invoke(registry, name);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access registry specByUpperName", e);
        }
    }
}
