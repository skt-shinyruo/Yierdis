package yier.bubu.redis.app.server;

import yier.bubu.redis.app.server.args.YierdisServerRuntimeConfig;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;
import yier.bubu.redis.execution.engine.DefaultYierdisEngine;
import yier.bubu.redis.execution.engine.YierdisEngine;
import yier.bubu.redis.runtime.embedded.YierdisInstance;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;

final class TestYierdisEngines {
    private TestYierdisEngines() {
    }

    static YierdisEngine forInstance(YierdisInstance instance) {
        YierdisFastCommandProcessor processor = ServerCommandComposition.createProcessor(
                TestDbRouters.forInstance(instance),
                new NettyServerInfoProvider(runtimeConfig(0, 0, 1024, 0, 4, 5)),
                SlowCommandGovernor.DEFAULT
        );
        return new DefaultYierdisEngine(
                processor,
                () -> {
                }
        );
    }

    private static YierdisServerRuntimeConfig runtimeConfig(
            int transactionQueueMaxCommands,
            int transactionQueueMaxBytes,
            int protocolMaxBulkBytes,
            int protocolMaxArgs,
            int protocolMaxInlineBytes,
            int protocolMaxCommandBytes
    ) {
        return new YierdisServerRuntimeConfig(
                0,
                1,
                1000L,
                1,
                1024,
                1024L * 1024L,
                YierdisServerRuntimeConfig.ExecutorSchedulingPolicy.FAIR,
                256,
                128,
                0L,
                0L,
                128,
                10L,
                transactionQueueMaxCommands,
                transactionQueueMaxBytes,
                protocolMaxBulkBytes,
                protocolMaxArgs,
                protocolMaxInlineBytes,
                protocolMaxCommandBytes,
                300000L,
                67108864L,
                10000L,
                256L * 1024L * 1024L,
                128L * 1024L * 1024L,
                64L * 1024L * 1024L,
                64 * 1024,
                4L * 1024L,
                5_000L,
                0L,
                YierdisServerRuntimeConfig.MaxmemoryScope.GLOBAL,
                MaxmemoryPolicy.NOEVICTION,
                5,
                5L,
                5L,
                false,
                65536L,
                64L,
                1L,
                0,
                0L,
                0,
                128L * 1024L * 1024L
        );
    }
}
