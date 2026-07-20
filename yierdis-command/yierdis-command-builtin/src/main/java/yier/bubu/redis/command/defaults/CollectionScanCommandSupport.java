package yier.bubu.redis.command.defaults;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;

/** HSCAN、SSCAN 与 ZSCAN 共用的参数和嵌套回复处理。 */
public final class CollectionScanCommandSupport {
    private static final int DEFAULT_COUNT = 10;

    private CollectionScanCommandSupport() {
    }

    public static CommandParseResult<Arguments> parse(
            ArgReader args,
            String commandLower,
            boolean allowNoValues
    ) {
        CommandParseError arity = CommandArity.min(3, commandLower).validate(args);
        if (arity != null) {
            return CommandParseResult.error(arity);
        }

        ScanCursorV2 cursor;
        try {
            cursor = ScanCursorV2.of(args.nonNegativeLongAt(2));
        } catch (IllegalArgumentException e) {
            return CommandParseResult.error(CommandParseError.integerOutOfRange());
        }

        byte[] match = null;
        int count = DEFAULT_COUNT;
        boolean noValues = false;
        for (int index = 3; index < args.argc(); index++) {
            if (args.is(index, "MATCH")) {
                if (index + 1 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                match = args.bytes(++index);
                continue;
            }
            if (args.is(index, "COUNT")) {
                if (index + 1 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                long parsed;
                try {
                    parsed = args.positiveLongAt(++index);
                } catch (IllegalArgumentException e) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                if (parsed > Integer.MAX_VALUE) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                count = (int) parsed;
                continue;
            }
            if (allowNoValues && args.is(index, "NOVALUES")) {
                noValues = true;
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new Arguments(args.bytes(1), cursor, match, count, noValues));
    }

    public static void writeReply(RedisReplyWriter out, CollectionScanWindow window) {
        Objects.requireNonNull(out, "out");
        Objects.requireNonNull(window, "window");
        try {
            byte[] nextCursor = window.nextCursor().toBulkStringAscii();
            ReplyPlan cursorPlan = ReplyPlans.bulkString(nextCursor.length, 0L);
            ReplyPlan elementsPlan = ReplyPlans.bulkStringArray(
                    window.count(),
                    window.encodedElementBytes(),
                    window.retainedMemoryBytes()
            );
            out.requireReply(ReplyPlans.array(
                    2,
                    addSaturating(cursorPlan.encodedUpperBoundBytes(), elementsPlan.encodedUpperBoundBytes()),
                    window.retainedMemoryBytes()
            ));
            out.arrayHeader(2);
            out.bulkString(nextCursor);
            out.arrayHeader(window.count());
            window.emitTo(new BulkStringReplyAdapter(out));
        } finally {
            // BulkStringSink 要求同步消费输入；写入完成后立即释放 scan pin，不能让 reply slot 长期保留 DB 来源。
            window.close();
        }
    }

    private static long addSaturating(long left, long right) {
        return left < 0L || right < 0L || left > Long.MAX_VALUE - right
                ? Long.MAX_VALUE
                : left + right;
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
