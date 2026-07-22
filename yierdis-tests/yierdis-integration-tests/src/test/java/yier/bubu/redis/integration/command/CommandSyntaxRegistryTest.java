package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.testutil.FastTestClient;
import yier.bubu.redis.testutil.ReplyArray;
import yier.bubu.redis.testutil.ReplyBulkString;
import yier.bubu.redis.testutil.ReplyInteger;
import yier.bubu.redis.testutil.ReplyObject;

import java.lang.reflect.Field;
import java.util.Arrays;

import static yier.bubu.redis.testutil.TestBytes.b;
import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class CommandSyntaxRegistryTest {
    @Test
    public void registryExposesSyntaxAndMultiPolicyThroughUnifiedSpec() {
        CommandRegistry registry = new CommandRegistry();
        CommandSyntax syntax = new CommandSyntax(
                "HELLO",
                CommandArity.min(1),
                CommandKeySpec.NONE,
                TransactionPolicy.DISALLOWED_IN_MULTI
        );
        registry.register(CommandSpec.of(
                syntax,
                CommandParsers.request(),
                (cmd, ctx) -> ctx.out().simpleString("OK")
        ));

        CommandSpec<?> spec = registry.specByUpperName("HELLO");
        Assert.assertNotNull(spec);
        Assert.assertSame(syntax, spec.syntax());
        Assert.assertEquals(-1, spec.syntax().arity().redisMetadataArity());
        Assert.assertEquals(CommandKeySpec.NONE, spec.syntax().keys());
        Assert.assertEquals(TransactionPolicy.DISALLOWED_IN_MULTI, spec.syntax().transactionPolicy());
    }

    @Test
    public void commandInfoUsesSyntaxFromRegistryRegistration() {
        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(
                    db,
                    registration -> registration.register(CommandSpec.of(
                            new CommandSyntax(
                                    "HELLO",
                                    CommandArity.exact(7),
                                    new CommandKeySpec(2, 2, 1),
                                    TransactionPolicy.QUEUEABLE
                            ),
                            CommandParsers.request(),
                            (cmd, ctx) -> ctx.out().simpleString("OK")
                    ))
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
                        registration.register(CommandSpec.of(
                                new CommandSyntax("INFO", CommandArity.min(1), CommandKeySpec.NONE,
                                        TransactionPolicy.QUEUEABLE),
                                CommandParsers.request(),
                                (cmd, ctx) -> ctx.out().simpleString("OK")
                        ));
                        registration.register(CommandSpec.of(
                                new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                                        TransactionPolicy.DISALLOWED_IN_MULTI),
                                CommandParsers.request(),
                                (cmd, ctx) -> ctx.out().simpleString("OK")
                        ));
                    }
            );
            CommandRegistry registry = registryOf(processor);
            Assert.assertEquals(-1, registry.specByUpperName("INFO").syntax().arity().redisMetadataArity());
            Assert.assertEquals(
                    TransactionPolicy.DISALLOWED_IN_MULTI,
                    registry.specByUpperName("HELLO").syntax().transactionPolicy()
            );
            Assert.assertNotNull(registry.specByUpperName("SET").syntax());
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
}
