package yier.bubu.redis.command.api;

/**
 * 在 composition root 构建命令表时注册一组 {@link CommandSpec}。
 *
 * <p>模块只参与启动期组合；registry sealed 后不能继续注册。</p>
 */
@FunctionalInterface
public interface CommandModule {
    void register(Registration registration);

    interface Registration {
        void register(CommandSpec spec);

        int commandCount();

        boolean containsUpperName(String nameUpper);

        CommandSpec specByUpperName(String nameUpper);

        String[] upperNamesSorted();
    }
}
