package yier.bubu.redis;

import yier.bubu.redis.command.CommandModule;
import yier.bubu.redis.command.CommandDescriptor;
import yier.bubu.redis.command.ServerInfoProvider;
import yier.bubu.redis.contract.Command;
import yier.bubu.redis.contract.CommandContext;
import yier.bubu.redis.contract.ReplyWriter;
import yier.bubu.redis.protocol.YierdisBuildInfo;

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
        registration.registerDisallowedInMulti(
                "HELLO",
                this::hello,
                CommandDescriptor.of(-1, 0, 0, 0),
                "ERR HELLO is not allowed in MULTI"
        );
        registration.register("INFO", this::info, CommandDescriptor.of(-1, 0, 0, 0));
        registration.register("STATS", this::stats, CommandDescriptor.of(1, 0, 0, 0));
    }

    private void info(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            out.error("ERR wrong number of arguments for 'info' command");
            return;
        }
        infoProvider.info(cmd, ctx);
    }

    private void stats(Command cmd, CommandContext ctx) {
        infoProvider.stats(cmd, ctx);
    }

    private void hello(Command cmd, CommandContext ctx) {
        ReplyWriter out = ctx.out();
        if (cmd.argc() != 1 && cmd.argc() != 2) {
            out.error("ERR wrong number of arguments for 'hello' command");
            return;
        }
        out.mapHeader(5);
        out.bulkString(HELLO_SERVER_KEY);
        out.bulkString(HELLO_SERVER_VALUE);
        out.bulkString(HELLO_VERSION_KEY);
        out.bulkString(HELLO_VERSION_VALUE);
        out.bulkString(HELLO_PROTO_KEY);
        out.integer(1);
        out.bulkString(HELLO_MODE_KEY);
        out.bulkString(HELLO_MODE_VALUE);
        out.bulkString(HELLO_ROLE_KEY);
        out.bulkString(HELLO_ROLE_VALUE);
    }
}
