package yier.bubu.redis.command;

import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;

/**
 * Extension point for registering additional commands into a processor.
 * <p>
 * Modules are applied at processor construction time and are intended for
 * composition-root concerns such as server-local command additions.
 */
@FunctionalInterface
public interface CommandModule {
    void register(Registration registration);

    interface Registration {
        void register(String name, Handler handler);

        default void registerDisallowedInMulti(String name, Handler handler, String errorMessage) {
            register(name, handler);
        }

        int commandCount();

        boolean containsUpperName(String nameUpper);

        String[] upperNamesSorted();
    }

    @FunctionalInterface
    interface Handler {
        void execute(Command cmd, CommandContext ctx);
    }
}
