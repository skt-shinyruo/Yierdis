package yier.bubu.redis.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandMetadataRegressionTest {
    @Test
    public void registrationInterfaceExposesUnifiedCommandSpecRegistration() throws Exception {
        Class<?> specType;
        try {
            specType = Class.forName("yier.bubu.redis.command.CommandSpec");
        } catch (ClassNotFoundException e) {
            Assert.fail("CommandSpec should exist as the unified registration contract");
            return;
        }

        try {
            Assert.assertNotNull(CommandModule.Registration.class.getMethod("register", String.class, specType));
        } catch (NoSuchMethodException e) {
            Assert.fail("CommandModule.Registration should expose register(String, CommandSpec)");
        }
    }

    @Test
    public void commandInfoKeepsMetadataForBuiltInAndExtraCommands() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = new YierdisFastCommandProcessor(
                    db,
                    null,
                    SlowCommandGovernor.DEFAULT,
                    registration -> registration.register(
                            "HELLO",
                            (cmd, ctx) -> ctx.out().simpleString("OK"),
                            CommandDescriptor.of(-1, 0, 0, 0)
                    )
            );
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyArray info = (ReplyArray) client.execute(Arrays.asList(
                        b("COMMAND"),
                        b("INFO"),
                        b("GET"),
                        b("HELLO")
                ));

                Assert.assertNotNull(info.values());
                Assert.assertEquals(2, info.values().size());
                assertCommandInfo(info.values().get(0), "get", 2, 1, 1, 1);
                assertCommandInfo(info.values().get(1), "hello", -1, 0, 0, 0);
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
