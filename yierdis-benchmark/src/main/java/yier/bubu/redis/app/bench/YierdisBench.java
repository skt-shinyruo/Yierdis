package yier.bubu.redis.app.bench;

import picocli.CommandLine;
import yier.bubu.redis.app.bench.redis.RedisBenchmarkCommand;

public final class YierdisBench {
    public static void main(String[] args) {
        int exitCode = commandLine().execute(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static CommandLine commandLine() {
        return new CommandLine(new RedisBenchmarkCommand())
                .setCaseInsensitiveEnumValuesAllowed(true);
    }

    private YierdisBench() {
    }
}
