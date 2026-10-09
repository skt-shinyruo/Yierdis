package yier.bubu.redis.command.defaults;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public final class CollectionScanCommandSupport {
    private CollectionScanCommandSupport() {
    }

    public static Arguments parse(CommandArgs args) {
        return parse(args, ScanArguments.Target.MEMBERS);
    }

    public static Arguments parseWithOptionalNoValues(CommandArgs args) {
        return parse(args, ScanArguments.Target.HASH);
    }

    private static Arguments parse(CommandArgs args, ScanArguments.Target target) {
        // cursor 是不透明整数，非法时 Redis 回 invalid cursor；能解析的任意值（含 phase 位超出内部约定的值）
        // 都交给存储层，无法映射到当前表拓扑时按重启迭代处理。
        ScanArguments parsed = ScanArguments.parse(args, 2, target);
        return new Arguments(args.bytes(1), parsed.cursor(), parsed.match(), parsed.count(), parsed.noValues());
    }

    public static PreparedCommand prepareReply(CollectionScanWindow window) {
        Objects.requireNonNull(window, "window");
        RedisReply reply = RedisReplies.array(List.of(
                RedisReplies.bulkString(window.nextCursor().toAsciiBytes()),
                DbReplies.sequence(window)
        ));
        return PreparedCommands.owned(CommandResult.reply(reply), window);
    }

    public record Arguments(
            byte[] key,
            ScanCursorV2 cursor,
            byte[] match,
            int count,
            boolean noValues
    ) {
    }
}
