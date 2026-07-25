package yier.bubu.redis.command.defaults;

import java.util.Objects;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.CollectionScanWindow;
import java.util.List;

/** HSCAN、SSCAN 与 ZSCAN 共用的参数和嵌套回复处理。 */
public final class CollectionScanCommandSupport {
    private static final int DEFAULT_COUNT = 10;

    private CollectionScanCommandSupport() {
    }

    public static CommandParseResult<Arguments> parse(
            ArgReader args,
            boolean allowNoValues
    ) {
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

    public static PreparedCommand prepareReply(CollectionScanWindow window) {
        Objects.requireNonNull(window, "window");
        byte[] nextCursor = window.nextCursor().toAsciiBytes();
        ReplyShape elements = ReplyShapes.sequence(
                window.elementCount(),
                window.retainedMemoryBytes(),
                consumer -> window.visitElementLengths(consumer::accept)
        );
        ReplyShape shape = ReplyShapes.array(List.of(
                ReplyShapes.bulkString(nextCursor.length, 0L),
                elements
        ));
        return CommandSupport.owned(shape, window, context -> {
            context.reply().arrayHeader(2);
            context.reply().bulkString(nextCursor);
            context.reply().arrayHeader(window.elementCount());
            window.emitTo(new BulkStringReplyAdapter(context.reply()));
        });
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
