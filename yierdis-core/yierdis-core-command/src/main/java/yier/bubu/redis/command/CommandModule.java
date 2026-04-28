package yier.bubu.redis.command;

import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ExecutionRequest;

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
        void register(String name, CommandSpec spec);

        default <T> void register(
                String name,
                CommandDescriptor descriptor,
                CommandParser<T> parser,
                CommandHandler<T> handler
        ) {
            register(name, CommandSpec.of(descriptor, parser, handler));
        }

        default void register(String name, Handler handler) {
            throw new UnsupportedOperationException(
                    "descriptor is required; use register(String, Handler, CommandDescriptor)"
            );
        }

        default void register(String name, Handler handler, CommandDescriptor descriptor) {
            register(name, new CommandSpec(handler, descriptor, null));
        }

        default void registerDisallowedInMulti(String name, Handler handler, String errorMessage) {
            throw new UnsupportedOperationException(
                    "descriptor is required; use registerDisallowedInMulti(String, Handler, CommandDescriptor, String)"
            );
        }

        default void registerDisallowedInMulti(
                String name,
                Handler handler,
                CommandDescriptor descriptor,
                String errorMessage
        ) {
            register(name, new CommandSpec(handler, descriptor, errorMessage));
        }

        default <T> void registerDisallowedInMulti(
                String name,
                CommandDescriptor descriptor,
                CommandParser<T> parser,
                CommandHandler<T> handler,
                String errorMessage
        ) {
            register(name, CommandSpec.disallowedInMulti(descriptor, parser, handler, errorMessage));
        }

        int commandCount();

        boolean containsUpperName(String nameUpper);

        String[] upperNamesSorted();
    }

    @FunctionalInterface
    interface Handler {
        void execute(ExecutionRequest request, CommandContext ctx);
    }
}
