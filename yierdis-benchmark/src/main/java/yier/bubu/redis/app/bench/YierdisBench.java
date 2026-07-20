package yier.bubu.redis.app.bench;

import picocli.CommandLine;
import yier.bubu.redis.app.bench.redis.RedisBenchmarkCommand;
import yier.bubu.redis.app.bench.storage.StorageBenchmarkCommand;

public final class YierdisBench {
    public static void main(String[] args) {
        int exitCode = commandLine().execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static CommandLine commandLine() {
        return new CommandLine(new RedisBenchmarkCommand())
                .addSubcommand(new StorageBenchmarkCommand())
                .setCaseInsensitiveEnumValuesAllowed(true);
    }

    private YierdisBench() {
    }
}
