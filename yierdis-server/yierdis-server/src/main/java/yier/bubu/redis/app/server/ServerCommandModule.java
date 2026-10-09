package yier.bubu.redis.app.server;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import java.util.function.Function;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.connection.ConnectionHandshake;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.execution.api.PreparedCommand;
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
                        TransactionPolicy.QUEUEABLE),
                this::hello
        ));
        registration.register(new CommandSpec(
                new CommandSyntax("INFO", CommandArity.min(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                this::info
        ));
        registration.register(new CommandSpec(
                new CommandSyntax("STATS", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                this::stats
        ));
    }

    private Function<CommandSession, PreparedCommand> info(CommandArgs args) {
        return session -> PreparedCommands.ready(infoProvider.info(args, session));
    }

    private Function<CommandSession, PreparedCommand> stats(CommandArgs args) {
        return session -> PreparedCommands.ready(infoProvider.stats(session));
    }

    // 与 Redis helloCommand 同序：版本号 → 逐个选项（SETNAME 名字在这里校验）→ 认证 → 改名 → 切协议。
    // parse 失败（版本/选项/非法 SETNAME）立刻抛错；WRONGPASS 推迟到 execute，以便 SETNAME 校验先于认证。
    // 任一失败都不改连接名、不切协议。
    private Function<CommandSession, PreparedCommand> hello(CommandArgs args) {
        Integer requestedVersion = null;
        int index = 1;
        String requestedClientName = null;
        boolean setClientName = false;
        byte[] authUsername = null;
        if (args.argc() >= 2) {
            requestedVersion = helloProtocolVersion(args);
            index = 2;
        }
        while (index < args.argc()) {
            int moreArgs = args.argc() - 1 - index;
            if (args.is(index, "AUTH") && moreArgs >= 2) {
                authUsername = args.bytes(index + 1);
                index += 3;
                continue;
            }
            if (args.is(index, "SETNAME") && moreArgs >= 1) {
                byte[] rawName = args.bytes(index + 1);
                if (!ConnectionHandshake.validClientName(rawName)) {
                    throw new CommandParseException(ConnectionHandshake.INVALID_CLIENT_NAME);
                }
                requestedClientName = rawName.length == 0 ? "" : args.utf8(index + 1);
                setClientName = true;
                index += 2;
                continue;
            }
            throw new CommandParseException("ERR Syntax error in HELLO option '" + args.utf8(index) + "'");
        }
        boolean authRejected = authUsername != null && !ConnectionHandshake.acceptsCredentials(authUsername);

        HelloArgs hello = new HelloArgs(requestedVersion, setClientName, requestedClientName);
        return session -> {
            if (authRejected) {
                return PreparedCommands.ready(CommandResult.error(ConnectionHandshake.WRONGPASS));
            }
            int targetRespVersion = hello.requestedVersion() == null
                    ? session.respVersion()
                    : hello.requestedVersion();
            RedisReply reply = helloReply(targetRespVersion);
            // 协商版本在 prepare 时声明，使本回复的容量预留与写出都按协商后版本计算。
            return PreparedCommands.action(reply.shape(), targetRespVersion, execution -> {
                execution.setRespVersion(targetRespVersion);
                if (hello.setClientName()) {
                    execution.setClientName(hello.clientName());
                }
                return CommandResult.reply(reply);
            });
        };
    }

    private static int helloProtocolVersion(CommandArgs args) {
        long version;
        try {
            version = args.longAt(1);
        } catch (CommandParseException notAnInteger) {
            throw new CommandParseException("ERR Protocol version is not an integer or out of range");
        }
        if (version != 2 && version != 3) {
            throw new CommandParseException("NOPROTO unsupported protocol version");
        }
        return (int) version;
    }

    private static RedisReply helloReply(int targetRespVersion) {
        List<RedisReply> fields = List.of(
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
        );
        return targetRespVersion == 3 ? RedisReplies.map(fields) : RedisReplies.array(fields);
    }

    private record HelloArgs(Integer requestedVersion, boolean setClientName, String clientName) {
    }
}
