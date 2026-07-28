package yier.bubu.redis.app.server;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;

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
        registration.register(new CommandDefinition<>(
                new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                        TransactionPolicy.DISALLOWED_IN_MULTI),
                CommandParsers.args(),
                this::hello
        ));
        registration.register(new CommandDefinition<>(
                new CommandSyntax("INFO", CommandArity.oneOf(1, 2), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                CommandParsers.args(),
                this::info
        ));
        registration.register(new CommandDefinition<>(
                new CommandSyntax("STATS", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                CommandParsers.args(),
                this::stats
        ));
    }

    private PreparedCommand info(ArgReader args, CommandPreparationContext context) {
        return infoProvider.prepareInfo(args.request(), context);
    }

    private PreparedCommand stats(ArgReader args, CommandPreparationContext context) {
        return infoProvider.prepareStats(args.request(), context);
    }

    private PreparedCommand hello(ArgReader args, CommandPreparationContext context) {
        int requested = context.session().respVersion();
        int index = 1;
        String requestedClientName = null;
        boolean setClientName = false;
        if (args.argc() >= 2) {
            String version = CommandSupport.utf8(args.bytes(1));
            if ("2".equals(version)) {
                requested = 2;
                index = 2;
            } else if ("3".equals(version)) {
                requested = 3;
                index = 2;
            } else {
                return CommandSupport.error("NOPROTO unsupported protocol version");
            }
        }
        while (index < args.argc()) {
            if (args.is(index, "SETNAME") && index + 1 < args.argc()) {
                requestedClientName = CommandSupport.utf8(args.bytes(index + 1));
                setClientName = true;
                index += 2;
                continue;
            }
            if (args.is(index, "AUTH")) {
                return CommandSupport.error(
                        "ERR AUTH <password> called without any password configured for the default user. "
                                + "Are you sure your configuration is correct?"
                );
            }
            return CommandSupport.error("ERR syntax error");
        }

        int targetRespVersion = requested;
        String clientName = requestedClientName;
        boolean shouldSetClientName = setClientName;
        return CommandSupport.fixed(helloReplyShape(targetRespVersion), execution -> {
            execution.session().setRespVersion(targetRespVersion);
            if (shouldSetClientName) {
                execution.session().setClientName(clientName);
            }
            execution.reply().mapHeader(5);
            execution.reply().bulkString(HELLO_SERVER_KEY);
            execution.reply().bulkString(HELLO_SERVER_VALUE);
            execution.reply().bulkString(HELLO_VERSION_KEY);
            execution.reply().bulkString(HELLO_VERSION_VALUE);
            execution.reply().bulkString(HELLO_PROTO_KEY);
            execution.reply().integer(targetRespVersion);
            execution.reply().bulkString(HELLO_MODE_KEY);
            execution.reply().bulkString(HELLO_MODE_VALUE);
            execution.reply().bulkString(HELLO_ROLE_KEY);
            execution.reply().bulkString(HELLO_ROLE_VALUE);
        });
    }

    private static ReplyShape helloReplyShape(int targetRespVersion) {
        return ReplyShapes.map(List.of(
                ReplyShapes.bulkString(HELLO_SERVER_KEY.length, 0L),
                ReplyShapes.bulkString(HELLO_SERVER_VALUE.length, 0L),
                ReplyShapes.bulkString(HELLO_VERSION_KEY.length, 0L),
                ReplyShapes.bulkString(HELLO_VERSION_VALUE.length, 0L),
                ReplyShapes.bulkString(HELLO_PROTO_KEY.length, 0L),
                ReplyShapes.integer(targetRespVersion),
                ReplyShapes.bulkString(HELLO_MODE_KEY.length, 0L),
                ReplyShapes.bulkString(HELLO_MODE_VALUE.length, 0L),
                ReplyShapes.bulkString(HELLO_ROLE_KEY.length, 0L),
                ReplyShapes.bulkString(HELLO_ROLE_VALUE.length, 0L)
        ));
    }
}
