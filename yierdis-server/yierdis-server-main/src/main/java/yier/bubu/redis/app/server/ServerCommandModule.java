package yier.bubu.redis.app.server;

import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;

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
    private static final ReplyPlan HELLO_REPLY_PLAN = ReplyPlans.bulkStringArray(10, helloElementBytes(), 0L);

    private final ServerInfoProvider infoProvider;

    ServerCommandModule(ServerInfoProvider infoProvider) {
        this.infoProvider = Objects.requireNonNull(infoProvider, "infoProvider");
    }

    @Override
    public void register(Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(CommandSpec.of(
                new CommandSyntax("HELLO", CommandArity.min(1), CommandKeySpec.NONE,
                        TransactionPolicy.DISALLOWED_IN_MULTI),
                CommandParsers.request(),
                this::hello
        ));
        registration.register(CommandSpec.of(
                new CommandSyntax("INFO", CommandArity.oneOf(1, 2), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                CommandParsers.request(),
                this::info
        ));
        registration.register(CommandSpec.of(
                new CommandSyntax("STATS", CommandArity.exact(1), CommandKeySpec.NONE,
                        TransactionPolicy.QUEUEABLE),
                CommandParsers.request(),
                this::stats
        ));
    }

    private void info(ExecutionRequest request, CommandContext ctx) {
        infoProvider.info(request, ctx);
    }

    private void stats(ExecutionRequest request, CommandContext ctx) {
        infoProvider.stats(request, ctx);
    }

    private void hello(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        int requested = ctx.protocolNegotiationSession().respVersion();
        int i = 1;
        String requestedClientName = null;
        boolean setClientName = false;
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
                requestedClientName = CommandSupport.utf8(request, i + 1);
                setClientName = true;
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
        out.requireReply(HELLO_REPLY_PLAN);
        ctx.protocolNegotiationSession().setRespVersion(requested);
        if (setClientName) {
            ctx.clientMetadataSession().setClientName(requestedClientName);
        }
        out.mapHeader(5);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.integer(ctx.protocolNegotiationSession().respVersion());
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }

    private static long helloElementBytes() {
        long encodedElementBytes = 0L;
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_SERVER_KEY);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_SERVER_VALUE);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_VERSION_KEY);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_VERSION_VALUE);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_PROTO_KEY);
        encodedElementBytes = saturatingAdd(encodedElementBytes, 4L);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_MODE_KEY);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_MODE_VALUE);
        encodedElementBytes = addBulkString(encodedElementBytes, HELLO_ROLE_KEY);
        return addBulkString(encodedElementBytes, HELLO_ROLE_VALUE);
    }

    private static long addBulkString(long current, byte[] value) {
        return saturatingAdd(current, ReplyPlans.bulkString(value.length, 0L).encodedUpperBoundBytes());
    }

    private static long saturatingAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }
}
