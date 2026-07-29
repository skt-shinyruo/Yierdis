package yier.bubu.redis.command.kernel;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;

public class CommandRegistryTest {
    @Test
    public void sealFreezesOneNormalizedSpecMap() {
        CommandRegistry registry = new CommandRegistry();
        CommandSpec ping = spec("ping", CommandArity.exact(1));
        registry.register(spec("ZADD"));
        registry.register(ping);
        registry.register(spec("APPEND"));
        registry.seal();

        Assert.assertSame(ping, registry.specByUpperName(" PiNg "));
        Assert.assertTrue(registry.containsUpperName(" ping "));
        Assert.assertArrayEquals(
                new String[]{"APPEND", "PING", "ZADD"},
                registry.upperNamesSorted()
        );
        Assert.assertThrows(IllegalStateException.class, () -> registry.register(spec("GET")));
    }

    @Test
    public void registrationRejectsNullAndDuplicateNormalizedMetadataNames() {
        CommandRegistry registry = new CommandRegistry();
        registry.register(spec(" ping "));

        Assert.assertThrows(NullPointerException.class, () -> registry.register((CommandSpec) null));
        IllegalArgumentException duplicate = Assert.assertThrows(
                IllegalArgumentException.class,
                () -> registry.register(spec("PING"))
        );
        Assert.assertEquals("duplicate command registration: PING", duplicate.getMessage());
    }

    @Test
    public void metadataLookupsRequireSealing() {
        CommandRegistry registry = new CommandRegistry();
        registry.register(spec("PING"));

        Assert.assertThrows(IllegalStateException.class, () -> registry.specByUpperName("PING"));
        Assert.assertThrows(IllegalStateException.class, () -> registry.containsUpperName("PING"));
        Assert.assertThrows(IllegalStateException.class, registry::upperNamesSorted);
    }

    @Test
    public void legacyDefinitionsKeepTheirMetadataIdentityAfterAdaptation() {
        CommandRegistry registry = new CommandRegistry();
        CommandDefinition<?> definition = new CommandDefinition<>(
                syntax("LEGACY", CommandArity.exact(1), TransactionPolicy.QUEUEABLE),
                CommandParsers.args(),
                (args, context) -> PreparedCommands.ready(
                        RedisReplies.simpleString("OK")
                )
        );
        registry.register(definition);
        registry.seal();

        Assert.assertSame(definition, registry.definitionByUpperName(" legacy "));
        Assert.assertSame(definition.syntax(), registry.specByUpperName("LEGACY").syntax());
    }

    private static CommandSpec spec(String name) {
        return spec(name, CommandArity.exact(1));
    }

    private static CommandSpec spec(String name, CommandArity arity) {
        return new CommandSpec(
                syntax(name, arity, TransactionPolicy.QUEUEABLE),
                args -> session -> PreparedCommands.ready(
                        RedisReplies.simpleString("PONG")
                )
        );
    }

    private static CommandSyntax syntax(
            String name,
            CommandArity arity,
            TransactionPolicy transactionPolicy
    ) {
        return new CommandSyntax(name, arity, CommandKeySpec.NONE, transactionPolicy);
    }
}
