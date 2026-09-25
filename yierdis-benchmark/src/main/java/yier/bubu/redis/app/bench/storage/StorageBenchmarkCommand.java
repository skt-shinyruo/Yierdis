package yier.bubu.redis.app.bench.storage;

import yier.bubu.redis.app.bench.BenchCommands;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.function.Function;

public final class StorageBenchmarkCommand {
    private final Function<StorageBenchmarkConfig, StorageBenchmarkResult> runner;
    private final StorageBenchmarkRenderer renderer;

    public StorageBenchmarkCommand() {
        this(new StorageBenchmarkRunner()::run, new StorageBenchmarkRenderer());
    }

    StorageBenchmarkCommand(
            Function<StorageBenchmarkConfig, StorageBenchmarkResult> runner,
            StorageBenchmarkRenderer renderer
    ) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.renderer = Objects.requireNonNull(renderer, "renderer");
    }

    /**
     * 解析并执行。返回退出码：0 = 成功；1 = 执行失败；2 = 用法错误（只打一行原因，不打 usage）。
     */
    public int run(String[] argv, PrintWriter out, PrintWriter err) {
        StorageBenchmarkConfig config;
        try {
            config = StorageBenchmarkOptions.parse(argv).toConfig();
        } catch (IllegalArgumentException failure) {
            err.println(failure.getMessage());
            err.flush();
            return BenchCommands.USAGE_ERROR;
        }

        try {
            StorageBenchmarkResult result = runner.apply(config);
            BenchCommands.writeOutput(out, renderer.render(config, result));
            return 0;
        } catch (RuntimeException failure) {
            err.println("storage benchmark failed: " + conciseMessage(failure));
            err.flush();
            return 1;
        }
    }

    private static String conciseMessage(RuntimeException failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }
}
