package yier.bubu.redis.integration.command;

import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
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
    public void registrationInterfaceExposesNameFreeCommandSpecRegistration() throws Exception {
        Class<?> specType;
        try {
            specType = Class.forName("yier.bubu.redis.command.api.CommandSpec");
        } catch (ClassNotFoundException e) {
            Assert.fail("CommandSpec should exist as the unified registration contract");
            return;
        }

        try {
            Assert.assertNotNull(CommandModule.Registration.class.getMethod("register", specType));
        } catch (NoSuchMethodException e) {
            Assert.fail("CommandModule.Registration should expose register(CommandSpec)");
        }

        Class<?> parserType = Class.forName("yier.bubu.redis.command.api.CommandParser");
        Class<?> handlerType = Class.forName("yier.bubu.redis.command.api.CommandHandler");
        Assert.assertNotNull(CommandSpec.class.getMethod(
                "of",
                CommandSyntax.class,
                parserType,
                handlerType
        ));
    }

    @Test
    public void commandInfoKeepsMetadataForBuiltInAndExtraCommands() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(
                    db,
                    registration -> registration.register(CommandSpec.of(
                            new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                                    TransactionPolicy.QUEUEABLE),
                            CommandParsers.request(),
                            (cmd, ctx) -> ctx.out().simpleString("OK")
                    ))
            );
            try (FastTestClient client = new FastTestClient(processor)) {
                ReplyArray info = (ReplyArray) client.execute(Arrays.asList(
                        b("COMMAND"),
                        b("INFO"),
                        b("GET"),
                        b("AUTH"),
                        b("HELLO")
                ));

                Assert.assertNotNull(info.values());
                Assert.assertEquals(3, info.values().size());
                assertCommandInfo(info.values().get(0), "get", 2, 1, 1, 1);
                assertCommandInfo(info.values().get(1), "auth", -2, 0, 0, 0);
                assertCommandInfo(info.values().get(2), "hello", -1, 0, 0, 0);
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
