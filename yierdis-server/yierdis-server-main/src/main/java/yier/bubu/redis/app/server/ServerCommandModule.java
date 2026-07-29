package yier.bubu.redis.app.server;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;

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
        registration.register(new CommandSpec(
                new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                        TransactionPolicy.DISALLOWED_IN_MULTI),
                this::hello
        ));
        registration.register(new CommandSpec(
                new CommandSyntax("INFO", CommandArity.oneOf(1, 2), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                this::info
        ));
        registration.register(new CommandSpec(
                new CommandSyntax("STATS", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                this::stats
        ));
    }

    private CommandInvocation info(CommandArgs args) {
        return session -> PreparedCommands.ready(infoProvider.info(args, session));
    }

    private CommandInvocation stats(CommandArgs args) {
        return session -> PreparedCommands.ready(infoProvider.stats(session));
    }

    private CommandInvocation hello(CommandArgs args) throws CommandParseException {
        Integer requestedVersion = null;
        int index = 1;
        String requestedClientName = null;
        boolean setClientName = false;
        if (args.argc() >= 2) {
            String version = args.utf8(1);
            if ("2".equals(version)) {
                requestedVersion = 2;
                index = 2;
            } else if ("3".equals(version)) {
                requestedVersion = 3;
                index = 2;
            } else {
                throw new CommandParseException("NOPROTO unsupported protocol version");
            }
        }
        while (index < args.argc()) {
            if (args.is(index, "SETNAME") && index + 1 < args.argc()) {
                requestedClientName = args.utf8(index + 1);
                setClientName = true;
                index += 2;
                continue;
            }
            if (args.is(index, "AUTH")) {
                throw new CommandParseException(
                        "ERR AUTH <password> called without any password configured for the default user. "
                                + "Are you sure your configuration is correct?"
                );
            }
            throw new CommandParseException("ERR syntax error");
        }

        HelloArgs hello = new HelloArgs(requestedVersion, setClientName, requestedClientName);
        return session -> {
            int targetRespVersion = hello.requestedVersion() == null
                    ? session.respVersion()
                    : hello.requestedVersion();
            RedisReply reply = helloReply(targetRespVersion);
            return PreparedCommands.action(reply.shape(), execution -> {
                execution.session().setRespVersion(targetRespVersion);
                if (hello.setClientName()) {
                    execution.session().setClientName(hello.clientName());
                }
                return CommandResult.reply(reply);
            });
        };
    }

    private static RedisReply helloReply(int targetRespVersion) {
        return RedisReplies.map(List.of(
                RedisReplies.bulkString(HELLO_SERVER_KEY),
                RedisReplies.bulkString(HELLO_SERVER_VALUE),
                RedisReplies.bulkString(HELLO_VERSION_KEY),
                RedisReplies.bulkString(HELLO_VERSION_VALUE),
                RedisReplies.bulkString(HELLO_PROTO_KEY),
                RedisReplies.integer(targetRespVersion),
                RedisReplies.bulkString(HELLO_MODE_KEY),
                RedisReplies.bulkString(HELLO_MODE_VALUE),
                RedisReplies.bulkString(HELLO_ROLE_KEY),
                RedisReplies.bulkString(HELLO_ROLE_VALUE)
        ));
    }

    private record HelloArgs(Integer requestedVersion, boolean setClientName, String clientName) {
    }
}
