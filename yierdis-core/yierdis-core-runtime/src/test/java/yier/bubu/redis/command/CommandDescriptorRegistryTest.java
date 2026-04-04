package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

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
                (cmd, ctx) -> ctx.out().simpleString("OK"),
                CommandDescriptor.of(-1, 0, 0, 0),
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
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                    db,
                    null,
                    SlowCommandGovernor.DEFAULT,
                    registration -> registration.register(
                            "HELLO",
                            (cmd, ctx) -> ctx.out().simpleString("OK"),
                            CommandDescriptor.of(7, 2, 2, 1)
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
}
