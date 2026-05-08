package yier.bubu.redis.command.api;

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

        CommandDescriptor descriptorByUpperName(String nameUpper);

        String[] upperNamesSorted();
    }
}
