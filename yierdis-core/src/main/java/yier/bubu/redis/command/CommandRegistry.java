package yier.bubu.redis.command;

import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Command name to handler registry (SSOT) for the server command processor.
 * <p>
 * This registry intentionally keeps the lookup contract simple and allocation-free at runtime:
 * matching uses {@link CommandSupport#asciiEqualsIgnoreCase(RespCommand, int, String)} on the request frame.
 */
final class CommandRegistry {
    @FunctionalInterface
    interface CommandHandler {
        void execute(RespCommand cmd, RespWriter out);
    }

    private static final class Entry {
        private final String nameUpper;
        private final CommandHandler handler;

        private Entry(String nameUpper, CommandHandler handler) {
            this.nameUpper = Objects.requireNonNull(nameUpper, "nameUpper");
            this.handler = Objects.requireNonNull(handler, "handler");
        }
    }

    private final List<Entry> entries = new ArrayList<>();
    private final Set<String> namesUpper = new HashSet<>();

    void register(String name, CommandHandler handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handler, "handler");
        String nameUpper = name.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty()) {
            throw new IllegalArgumentException("command name must not be empty");
        }
        if (!namesUpper.add(nameUpper)) {
            throw new IllegalArgumentException("duplicate command registration: " + nameUpper);
        }
        entries.add(new Entry(nameUpper, handler));
    }

    CommandHandler find(RespCommand cmd) {
        if (cmd == null || cmd.argc() <= 0) {
            return null;
        }
        for (int i = 0; i < entries.size(); i++) {
            Entry e = entries.get(i);
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, 0, e.nameUpper)) {
                return e.handler;
            }
        }
        return null;
    }
}

