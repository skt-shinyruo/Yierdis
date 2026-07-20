package yier.bubu.redis.app.bench.storage;

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
        name = "storage",
        description = "Measure in-process SET throughput and storage footprint.",
        mixinStandardHelpOptions = true,
        sortOptions = false,
        usageHelpAutoWidth = true
)
public final class StorageBenchmarkCommand implements Callable<Integer> {
    private final Function<StorageBenchmarkConfig, StorageBenchmarkResult> runner;
    private final StorageBenchmarkRenderer renderer;

    @Mixin
    private StorageBenchmarkOptions options = new StorageBenchmarkOptions();

    @Spec
    private CommandSpec spec;

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

    @Override
    public Integer call() {
        StorageBenchmarkConfig config;
        try {
            config = options.toConfig();
        } catch (IllegalArgumentException failure) {
            throw new ParameterException(spec.commandLine(), failure.getMessage(), failure);
        }

        StorageBenchmarkResult result;
        try {
            result = runner.apply(config);
        } catch (RuntimeException failure) {
            PrintWriter err = spec.commandLine().getErr();
            err.println("storage benchmark failed: " + conciseMessage(failure));
            err.flush();
            return 1;
        }

        PrintWriter out = spec.commandLine().getOut();
        out.print(renderer.render(config, result));
        out.flush();
        if (out.checkError()) {
            throw new IllegalStateException("failed to write benchmark output");
        }
        return 0;
    }

    private static String conciseMessage(RuntimeException failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }
}
