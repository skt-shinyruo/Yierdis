package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;

public final class CommandRegistries {
    private CommandRegistries() {
    }

    public static CommandDispatcher dispatcher(CommandModule... modules) {
        CommandRegistry registry = new CommandRegistry();
        CommandDispatcher dispatcher = new CommandDispatcher(registry);
        new TransactionCommands(dispatcher).register(registry);
        registerModules(registry, modules);
        registry.seal();
        return dispatcher;
    }

    private static void registerModules(CommandRegistry registry, CommandModule[] modules) {
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
