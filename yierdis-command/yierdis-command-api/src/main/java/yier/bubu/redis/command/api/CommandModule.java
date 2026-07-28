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
        void register(CommandSpec spec);

        void register(CommandDefinition<?> definition);

        int commandCount();

        boolean containsUpperName(String nameUpper);

        CommandDefinition<?> definitionByUpperName(String nameUpper);

        CommandSpec specByUpperName(String nameUpper);

        String[] upperNamesSorted();
    }
}
