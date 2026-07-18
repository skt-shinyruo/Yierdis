package yier.bubu.redis.app.bench.redis;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

@Command(
        name = "yierdis-benchmark",
        description = "Connect-only Redis-compatible benchmark for Yierdis.",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class RedisBenchmarkCommand implements Callable<Integer> {
    private final Function<BenchmarkConfig, BenchmarkRunResult> runner;
    private final BenchmarkOutputRenderer renderer;

    @Mixin
    private RedisBenchmarkOptions options = new RedisBenchmarkOptions();

    @Spec
    private CommandSpec spec;

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

    @Override
    public Integer call() {
        BenchmarkConfig config;
        BenchmarkRunResult result;
        try {
            config = options.toConfig(System::nanoTime);
            result = runner.apply(config);
        } catch (IllegalArgumentException failure) {
            throw new ParameterException(spec.commandLine(), failure.getMessage(), failure);
        }

        PrintWriter out = spec.commandLine().getOut();
        out.print(renderer.render(config, result));
        out.flush();
        return result.exitCode();
    }
}
