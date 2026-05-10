package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyWriter;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

final class ServerCommandModule implements CommandModule {
    private static final byte[] HELLO_SERVER_KEY = "server".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_SERVER_VALUE = "yierdis".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_KEY = "version".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_VERSION_VALUE = YierdisBuildInfo.versionAsciiBytes();
    private static final byte[] HELLO_PROTO_KEY = "proto".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_KEY = "mode".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_MODE_VALUE = "standalone".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_KEY = "role".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] HELLO_ROLE_VALUE = "master".getBytes(StandardCharsets.US_ASCII);

    private final ServerInfoProvider infoProvider;

    ServerCommandModule(ServerInfoProvider infoProvider) {
        this.infoProvider = Objects.requireNonNull(infoProvider, "infoProvider");
    }

    @Override
    public void register(Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(
                "HELLO",
                CommandSpec.disallowedInMulti(
                        CommandDescriptor.of(-1, 0, 0, 0),
                        CommandParsers.minRequest(1, "hello"),
                        this::hello,
                        "ERR HELLO is not allowed in MULTI"
                )
        );
        registration.register("INFO", CommandDescriptor.of(-1, 0, 0, 0), CommandParsers.oneOfRequest("info", 1, 2), this::info);
        registration.register("STATS", CommandDescriptor.of(1, 0, 0, 0), CommandParsers.exactRequest(1, "stats"), this::stats);
    }

    private void info(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (request.argc() != 1 && request.argc() != 2) {
            out.error("ERR wrong number of arguments for 'info' command");
            return;
        }
        infoProvider.info(request, ctx);
    }

    private void stats(ExecutionRequest request, CommandContext ctx) {
        infoProvider.stats(request, ctx);
    }

    private void hello(ExecutionRequest request, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        int requested = ctx.session().respVersion();
        int i = 1;
        if (request.argc() >= 2) {
            String version = CommandSupport.utf8(request, 1);
            if ("2".equals(version)) {
                requested = 2;
                i = 2;
            } else if ("3".equals(version)) {
                requested = 3;
                i = 2;
            } else {
                out.error("NOPROTO unsupported protocol version");
                return;
            }
        }
        while (i < request.argc()) {
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "SETNAME") && i + 1 < request.argc()) {
                ctx.session().setClientName(CommandSupport.utf8(request, i + 1));
                i += 2;
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(request, i, "AUTH")) {
                out.error("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?");
                return;
            }
            out.error("ERR syntax error");
            return;
        }
        ctx.session().setRespVersion(requested);
        out.mapHeader(5);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.integer(ctx.session().respVersion());
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }
}
