package yier.bubu.redis.app.client;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.ArrayList;
import java.util.List;

@Command(
        name = "yierdis-client",
        description = "Netty-based client and CLI for Yierdis.",
        sortOptions = false,
        usageHelpAutoWidth = true
)
final class YierdisCliArgs {
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    boolean help;

    @Option(names = "--host", defaultValue = "127.0.0.1", description = "Server host.")
    String host = "127.0.0.1";

    @Option(names = "--port", defaultValue = "6378", description = "Server port.")
    int port = 6378;

    @Option(names = "--timeoutMillis", defaultValue = "5000", description = "Command timeout in milliseconds.")
    long timeoutMillis = 5000;

    @Option(names = "--hex", description = "Print the raw JSON reply line as hex bytes.")
    boolean hex;

    @Parameters(
            arity = "0..*",
            paramLabel = "COMMAND [ARG...]",
            description = "Execute a single command. If omitted, start interactive REPL."
    )
    List<String> command = new ArrayList<>();
}
