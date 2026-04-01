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

public class CommandDescriptorRegistryTest {
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
