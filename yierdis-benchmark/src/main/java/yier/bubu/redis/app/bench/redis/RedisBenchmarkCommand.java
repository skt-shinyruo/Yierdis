package yier.bubu.redis.app.bench.redis;

import yier.bubu.redis.app.bench.BenchCommands;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.function.Function;

public final class RedisBenchmarkCommand {
    private final Function<BenchmarkConfig, BenchmarkRunResult> runner;
    private final BenchmarkOutputRenderer renderer;

    public RedisBenchmarkCommand() {
        this(new RedisBenchmark()::run, new BenchmarkOutputRenderer());
    }

    RedisBenchmarkCommand(
            Function<BenchmarkConfig, BenchmarkRunResult> runner,
            BenchmarkOutputRenderer renderer
    ) {
        this.runner = Objects.requireNonNull(runner, "runner");
        this.renderer = Objects.requireNonNull(renderer, "renderer");
    }

    /**
     * 解析并执行。返回退出码：0 = 成功；1 = 执行失败；2 = 用法错误（只打一行原因，不打 usage）。
     * {@link Error} 不降级：直接向外抛。
     */
    public int run(String[] argv, PrintWriter out, PrintWriter err) {
        BenchmarkConfig config;
        try {
            config = RedisBenchmarkOptions.parse(argv).toConfig(System::nanoTime);
        } catch (IllegalArgumentException failure) {
            err.println(failure.getMessage());
            err.flush();
            return BenchCommands.USAGE_ERROR;
        }

        BenchmarkRunResult result;
        try {
            result = runner.apply(config);
        } catch (RedisBenchmarkCatalog.SelectionException failure) {
            err.println(failure.getMessage());
            err.flush();
            return BenchCommands.USAGE_ERROR;
        } catch (IllegalArgumentException failure) {
            err.println(failure.getMessage());
            err.flush();
            return 1;
        }

        try {
            BenchCommands.writeOutput(out, renderer.render(config, result));
        } catch (RuntimeException failure) {
            err.println(failure.getMessage());
            err.flush();
            return 1;
        }
        return result.exitCode();
    }
}
