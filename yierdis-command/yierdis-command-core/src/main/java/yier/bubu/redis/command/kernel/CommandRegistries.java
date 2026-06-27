package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;

import java.util.Arrays;

public final class CommandRegistries {
    private CommandRegistries() {
    }

    public static CommandRegistry from(CommandModule... modules) {
        CommandRegistry registry = new CommandRegistry();
        registerInto(registry, modules);
        return registry;
    }

    public static CommandRegistry from(Iterable<? extends CommandModule> modules) {
        CommandRegistry registry = new CommandRegistry();
        registerInto(registry, modules);
        return registry;
    }

    public static void registerInto(CommandRegistry registry, CommandModule... modules) {
        if (modules == null) {
            return;
        }
        registerInto(registry, Arrays.asList(modules));
    }

    public static void registerInto(CommandRegistry registry, Iterable<? extends CommandModule> modules) {
        if (registry == null) {
            throw new NullPointerException("registry");
        }
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
