package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;

import java.util.Arrays;

public final class CommandRegistries {
    private CommandRegistries() {
    }

    public static CommandRegistry from(CommandModule... modules) {
        CommandRegistry registry = new CommandRegistry();
        registerModules(registry, modules == null ? null : Arrays.asList(modules));
        registry.seal();
        return registry;
    }

    public static CommandRegistry from(Iterable<? extends CommandModule> modules) {
        CommandRegistry registry = new CommandRegistry();
        registerModules(registry, modules);
        registry.seal();
        return registry;
    }

    public static CommandDispatcher dispatcher(CommandModule... modules) {
        if (modules == null) {
            return dispatcher((Iterable<? extends CommandModule>) null);
        }
        return dispatcher(Arrays.asList(modules));
    }

    public static CommandDispatcher dispatcher(Iterable<? extends CommandModule> modules) {
        CommandRegistry registry = new CommandRegistry();
        CommandDispatcher dispatcher = new CommandDispatcher(registry);
        new TransactionCommands(dispatcher).register(registry);
        registerModules(registry, modules);
        registry.seal();
        return dispatcher;
    }

    private static void registerModules(CommandRegistry registry, Iterable<? extends CommandModule> modules) {
        if (modules == null) {
            return;
        }
        for (CommandModule module : modules) {
            if (module == null) {
                throw new IllegalArgumentException("modules must not contain null");
            }
            module.register(registry);
        }
    }
}
