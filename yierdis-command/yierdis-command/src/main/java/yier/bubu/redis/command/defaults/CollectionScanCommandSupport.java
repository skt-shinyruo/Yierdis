package yier.bubu.redis.command.defaults;

import java.util.List;
import java.util.Objects;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

public final class CollectionScanCommandSupport {
    private static final int DEFAULT_COUNT = 10;
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INTEGER_ERROR = "ERR value is not an integer or out of range";

    private CollectionScanCommandSupport() {
    }

    public static Arguments parse(CommandArgs args) {
        return parse(args, NoValuesPolicy.REJECT);
    }

    public static Arguments parseWithOptionalNoValues(CommandArgs args) {
        return parse(args, NoValuesPolicy.ALLOW);
    }

    private static Arguments parse(CommandArgs args, NoValuesPolicy noValuesPolicy) {
        // cursor 是不透明非负整数：负数与溢出由 nonNegativeLongAt 拒绝；其余任意值（含 phase 位
        // 超出内部约定的值）都可解析，无法映射到当前表拓扑时由存储层按重启迭代处理。
        ScanCursorV2 cursor = ScanCursorV2.of(args.nonNegativeLongAt(2));

        byte[] match = null;
        int count = DEFAULT_COUNT;
        boolean noValues = false;
        for (int index = 3; index < args.argc(); index++) {
            if (args.is(index, "MATCH")) {
                if (++index >= args.argc()) {
                    throw syntaxFailure();
                }
                match = args.bytes(index);
                continue;
            }
            if (args.is(index, "COUNT")) {
                if (++index >= args.argc()) {
                    throw syntaxFailure();
                }
                long parsed = args.positiveLongAt(index);
                if (parsed > Integer.MAX_VALUE) {
                    throw integerFailure();
                }
                count = (int) parsed;
                continue;
            }
            if (noValuesPolicy == NoValuesPolicy.ALLOW && args.is(index, "NOVALUES")) {
                noValues = true;
                continue;
            }
            throw syntaxFailure();
        }
        return new Arguments(args.bytes(1), cursor, match, count, noValues);
    }

    public static PreparedCommand prepareReply(CollectionScanWindow window) {
        Objects.requireNonNull(window, "window");
        RedisReply reply = RedisReplies.array(List.of(
                RedisReplies.bulkString(window.nextCursor().toAsciiBytes()),
                DbReplies.sequence(window)
        ));
        return PreparedCommands.owned(CommandResult.reply(reply), window);
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }

    private static CommandParseException integerFailure() {
        return new CommandParseException(INTEGER_ERROR);
    }

    private enum NoValuesPolicy {
        ALLOW,
        REJECT
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
