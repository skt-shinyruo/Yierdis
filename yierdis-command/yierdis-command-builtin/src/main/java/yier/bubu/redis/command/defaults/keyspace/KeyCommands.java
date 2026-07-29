package yier.bubu.redis.command.defaults.keyspace;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.ToLongFunction;
import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.execution.api.CommandPreparationContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.ReplyShape;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

public final class KeyCommands implements CommandModule {
    private static final int KEY_WINDOW_DISCOVERY_ATTEMPTS = 2;
    private static final CommandKeySpec KEY = new CommandKeySpec(1, 1, 1);
    private static final CommandKeySpec MULTI_KEYS = new CommandKeySpec(1, -1, 1);
    private static final MemoryStatField[] MEMORY_STATS_FIELDS = {
            memoryStat("maxmemory_bytes", YierdisMemoryStats::maxmemoryBytes),
            memoryStat("used_bytes_for_maxmemory", YierdisMemoryStats::usedBytesForMaxmemory),
            memoryStat("effective_used_bytes_for_maxmemory", YierdisMemoryStats::effectiveUsedBytesForMaxmemory),
            memoryStat("ledger_used_bytes", YierdisMemoryStats::heapDataBytesEstimate),
            memoryStat("offheap_used_bytes", YierdisMemoryStats::offHeapUsedBytes),
            memoryStat("ledger_reserved_bytes", YierdisMemoryStats::reservedBytes),
            memoryStat("offheap_included_in_maxmemory", stats -> stats.offHeapIncludedInMaxmemory() ? 1L : 0L),
            memoryStat("keyspace_table_overhead_bytes_estimate", YierdisMemoryStats::keyspaceTableOverheadBytesEstimate),
            memoryStat("expire_table_overhead_bytes_estimate", YierdisMemoryStats::expireTableOverheadBytesEstimate),
            memoryStat("expire_value_objects_bytes_estimate", YierdisMemoryStats::expireValueObjectsBytesEstimate),
            memoryStat("total_estimated_bytes", YierdisMemoryStats::totalEstimatedBytes),
            memoryStat("keys_stored_offheap", stats -> stats.keysStoredOffHeap() ? 1L : 0L),
            memoryStat("key_count", YierdisMemoryStats::keyCount),
            memoryStat("expire_count", YierdisMemoryStats::expireCount),
            memoryStat(
                    "expired_entries_awaiting_physical_deletion",
                    YierdisMemoryStats::expiredEntriesAwaitingPhysicalDeletion
            ),
            memoryStat("keyspace_rehashing", stats -> stats.keyspaceRehashing() ? 1L : 0L),
            memoryStat("keyspace_table0_capacity", YierdisMemoryStats::keyspaceTable0Capacity),
            memoryStat("keyspace_table1_capacity", YierdisMemoryStats::keyspaceTable1Capacity),
            memoryStat("expire_rehashing", stats -> stats.expireRehashing() ? 1L : 0L),
            memoryStat("expire_table0_capacity", YierdisMemoryStats::expireTable0Capacity),
            memoryStat("expire_table1_capacity", YierdisMemoryStats::expireTable1Capacity)
    };

    private final CommandSupport support;

    public KeyCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register(new CommandDefinition<>(syntax("TYPE", CommandArity.exact(2), KEY),
                CommandParsers.args(), this::type));
        registration.register(new CommandDefinition<>(syntax("MEMORY", CommandArity.min(2), CommandKeySpec.NONE),
                CommandParsers.args(), this::memory));
        registration.register(new CommandDefinition<>(syntax("OBJECT", CommandArity.min(2), CommandKeySpec.NONE),
                CommandParsers.args(), this::object));
        registration.register(new CommandDefinition<>(syntax("KEYS", CommandArity.exact(2), CommandKeySpec.NONE),
                CommandParsers.args(), this::keys));
        registration.register(new CommandDefinition<>(syntax("SCAN", CommandArity.min(2), CommandKeySpec.NONE),
                this::parseScan, this::scan));
        registration.register(new CommandDefinition<>(syntax("DEL", CommandArity.min(2), MULTI_KEYS),
                CommandParsers.args(), this::del));
        registration.register(new CommandDefinition<>(syntax("EXISTS", CommandArity.min(2), MULTI_KEYS),
                CommandParsers.args(), this::exists));
        registration.register(new CommandDefinition<>(syntax("EXPIRE", CommandArity.exact(3), KEY),
                CommandParsers.args(), this::expire));
        registration.register(new CommandDefinition<>(syntax("PEXPIRE", CommandArity.exact(3), KEY),
                CommandParsers.args(), this::pexpire));
        registration.register(new CommandDefinition<>(syntax("EXPIREAT", CommandArity.exact(3), KEY),
                CommandParsers.args(), this::expireat));
        registration.register(new CommandDefinition<>(syntax("PEXPIREAT", CommandArity.exact(3), KEY),
                CommandParsers.args(), this::pexpireat));
        registration.register(new CommandDefinition<>(syntax("PERSIST", CommandArity.exact(2), KEY),
                CommandParsers.args(), this::persist));
        registration.register(new CommandDefinition<>(syntax("TTL", CommandArity.exact(2), KEY),
                CommandParsers.args(), this::ttl));
        registration.register(new CommandDefinition<>(syntax("PTTL", CommandArity.exact(2), KEY),
                CommandParsers.args(), this::pttl));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private PreparedCommand type(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        ValueType valueType = support.commandDb(context).reads().keyspace().typeOf(support.argView(request, 1));
        String value = valueType == null ? "none" : valueType.name().toLowerCase(Locale.ROOT);
        return CommandSupport.fixed(ReplyShapes.simpleString(value), execution -> execution.reply().simpleString(value));
    }

    private PreparedCommand memory(ArgReader args, CommandPreparationContext context) {
        if (args.is(1, "USAGE")) {
            if (args.argc() != 3) {
                return CommandSupport.error("ERR wrong number of arguments for 'memory' command");
            }
            ExecutionRequest request = args.request();
            long bytes = support.commandDb(context).memory().memoryUsage(support.argView(request, 2));
            return bytes < 0L
                    ? CommandSupport.fixed(ReplyShapes.nullValue(), execution -> execution.reply().nullValue())
                    : CommandSupport.fixed(ReplyShapes.integer(bytes), execution -> execution.reply().integer(bytes));
        }

        if (args.is(1, "STATS")) {
            if (args.argc() != 2) {
                return CommandSupport.error("ERR wrong number of arguments for 'memory' command");
            }
            ServerInfoProvider infoProvider = support.infoProvider();
            YierdisMemoryStats stats = infoProvider == null ? null : infoProvider.memoryStats(context.session());
            if (stats == null) {
                stats = support.commandDb(context).memory().memoryStats();
            }
            return memoryStats(stats);
        }

        return CommandSupport.error("ERR syntax error");
    }

    private static PreparedCommand memoryStats(YierdisMemoryStats stats) {
        ArrayList<MemoryStatValue> values = new ArrayList<>(MEMORY_STATS_FIELDS.length);
        ArrayList<ReplyShape> shapes = new ArrayList<>(MEMORY_STATS_FIELDS.length * 2);
        for (MemoryStatField field : MEMORY_STATS_FIELDS) {
            long value = field.value(stats);
            values.add(new MemoryStatValue(field.key(), value));
            shapes.add(ReplyShapes.bulkString(field.key().length, 0L));
            shapes.add(ReplyShapes.integer(value));
        }
        ReplyShape shape = ReplyShapes.map(shapes);
        return CommandSupport.fixed(shape, execution -> {
            execution.reply().mapHeader(values.size());
            for (MemoryStatValue value : values) {
                execution.reply().bulkString(value.key());
                execution.reply().integer(value.value());
            }
        });
    }

    private static MemoryStatField memoryStat(
            String key,
            ToLongFunction<YierdisMemoryStats> valueExtractor
    ) {
        return new MemoryStatField(key.getBytes(StandardCharsets.US_ASCII), valueExtractor);
    }

    private record MemoryStatField(
            byte[] key,
            ToLongFunction<YierdisMemoryStats> valueExtractor
    ) {
        private long value(YierdisMemoryStats stats) {
            return valueExtractor.applyAsLong(stats);
        }
    }

    private record MemoryStatValue(byte[] key, long value) {
    }

    private PreparedCommand object(ArgReader args, CommandPreparationContext context) {
        if (args.argc() != 3) {
            return CommandSupport.error("ERR wrong number of arguments for 'object' command");
        }
        if (!args.is(1, "ENCODING")) {
            return CommandSupport.error("ERR syntax error");
        }
        ExecutionRequest request = args.request();
        String encoding = support.commandDb(context).memory().objectEncoding(support.argView(request, 2));
        if (encoding == null) {
            return CommandSupport.fixed(ReplyShapes.nullValue(), execution -> execution.reply().nullValue());
        }
        byte[] value = encoding.getBytes(StandardCharsets.US_ASCII);
        return CommandSupport.fixed(ReplyShapes.bulkString(value.length, 0L),
                execution -> execution.reply().bulkString(value));
    }

    private PreparedCommand keys(ArgReader args, CommandPreparationContext context) {
        SlowCommandGovernor governor = support.slowGovernor();
        long timeBudgetNanos = governor.keysTimeBudgetNanos(context.session());
        long deadlineNanos = deadlineNanos(timeBudgetNanos);
        for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
            if (attempt > 0 && deadlineExpired(deadlineNanos)) {
                break;
            }
            long remainingBudgetNanos = remainingBudgetNanos(timeBudgetNanos, deadlineNanos);
            KeyScanWindow window = support.commandDb(context).reads().keyspace().keys(
                    args.bytes(1),
                    governor.keysMaxResults(context.session()),
                    remainingBudgetNanos
            );
            if (!window.current()) {
                window.close();
                continue;
            }
            return keyWindowReply(window);
        }
        return CommandSupport.error("ERR key discovery window changed before reply preflight");
    }

    private record ScanArgs(long cursor, byte[] match, int count) {
    }

    private CommandParseResult<ScanArgs> parseScan(ArgReader args) {
        long cursor;
        try {
            cursor = args.nonNegativeLongAt(1);
        } catch (IllegalArgumentException e) {
            return CommandParseResult.error(CommandParseError.integerOutOfRange());
        }
        byte[] match = null;
        int count = 10;
        for (int i = 2; i < args.argc(); i++) {
            if (args.is(i, "MATCH")) {
                if (i + 1 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                match = args.bytes(++i);
                continue;
            }
            if (args.is(i, "COUNT")) {
                if (i + 1 >= args.argc()) {
                    return CommandParseResult.error(CommandParseError.syntax());
                }
                long value;
                try {
                    value = args.nonNegativeLongAt(++i);
                } catch (IllegalArgumentException e) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                if (value <= 0L || value > Integer.MAX_VALUE) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                count = (int) value;
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new ScanArgs(cursor, match, count));
    }

    private PreparedCommand scan(ScanArgs args, CommandPreparationContext context) {
        for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
            KeyScanWindow window = support.commandDb(context).reads().keyspace().scan(
                    ScanCursorV2.of(args.cursor()),
                    args.match(),
                    args.count()
            );
            if (!window.current()) {
                window.close();
                continue;
            }
            return scanWindowReply(window);
        }
        return CommandSupport.error("ERR scan window changed before reply preflight");
    }

    private static PreparedCommand keyWindowReply(KeyScanWindow window) {
        ReplyShape shape = ReplyShapes.sequence(
                window.elementCount(),
                window.retainedMemoryBytes(),
                consumer -> window.visitElementLengths(consumer::accept)
        );
        return CommandSupport.owned(
                shape,
                window,
                () -> window.current() ? ValidationResult.VALID : ValidationResult.STALE,
                execution -> {
                    execution.reply().arrayHeader(window.elementCount());
                    window.emitTo(new yier.bubu.redis.command.defaults.BulkStringReplyAdapter(execution.reply()));
                }
        );
    }

    private static PreparedCommand scanWindowReply(KeyScanWindow window) {
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
        return CommandSupport.owned(
                shape,
                window,
                () -> window.current() ? ValidationResult.VALID : ValidationResult.STALE,
                execution -> {
                    execution.reply().arrayHeader(2);
                    execution.reply().bulkString(nextCursor);
                    execution.reply().arrayHeader(window.elementCount());
                    window.emitTo(new yier.bubu.redis.command.defaults.BulkStringReplyAdapter(execution.reply()));
                }
        );
    }

    private static long deadlineNanos(long budgetNanos) {
        if (budgetNanos <= 0L || budgetNanos >= Long.MAX_VALUE - System.nanoTime()) {
            return Long.MAX_VALUE;
        }
        return System.nanoTime() + budgetNanos;
    }

    private static boolean deadlineExpired(long deadlineNanos) {
        return deadlineNanos != Long.MAX_VALUE && System.nanoTime() >= deadlineNanos;
    }

    private static long remainingBudgetNanos(long configuredBudgetNanos, long deadlineNanos) {
        if (configuredBudgetNanos <= 0L) {
            return 0L;
        }
        return Math.max(1L, deadlineNanos - System.nanoTime());
    }

    private PreparedCommand del(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            int len = request.argc() - 1;
            support.sliceResetFromRequest(request, 1, len);
            try {
                long deleted = support.commandDb(execution).writes().keyspace().del(support.slice()).value();
                execution.reply().integer(deleted);
            } finally {
                support.clearScratch(len);
            }
        });
    }

    private PreparedCommand exists(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long count = 0L;
        for (int i = 1; i < args.argc(); i++) {
            if (support.commandDb(context).reads().keyspace().existsKey(support.argView(request, i))) {
                count++;
            }
        }
        long value = count;
        return CommandSupport.fixed(ReplyShapes.integer(value), execution -> execution.reply().integer(value));
    }

    private PreparedCommand expire(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        final long seconds;
        try {
            seconds = args.longAt(2);
        } catch (IllegalArgumentException e) {
            return CommandSupport.error("ERR value is not an integer or out of range");
        }
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            boolean applied = support.commandDb(execution).writes().ttl()
                    .expire(support.argView(request, 1), seconds)
                    .value();
            execution.reply().integer(applied ? 1L : 0L);
        });
    }

    private PreparedCommand pexpire(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        final long millis;
        try {
            millis = args.longAt(2);
        } catch (IllegalArgumentException e) {
            return CommandSupport.error("ERR value is not an integer or out of range");
        }
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            boolean applied = support.commandDb(execution).writes().ttl()
                    .pexpire(support.argView(request, 1), millis)
                    .value();
            execution.reply().integer(applied ? 1L : 0L);
        });
    }

    private PreparedCommand expireat(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        final long seconds;
        try {
            seconds = args.longAt(2);
        } catch (IllegalArgumentException e) {
            return CommandSupport.error("ERR value is not an integer or out of range");
        }
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            boolean applied = support.commandDb(execution).writes().ttl()
                    .expireAtSeconds(support.argView(request, 1), seconds)
                    .value();
            execution.reply().integer(applied ? 1L : 0L);
        });
    }

    private PreparedCommand pexpireat(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        final long millis;
        try {
            millis = args.longAt(2);
        } catch (IllegalArgumentException e) {
            return CommandSupport.error("ERR value is not an integer or out of range");
        }
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            boolean applied = support.commandDb(execution).writes().ttl()
                    .expireAtMillis(support.argView(request, 1), millis)
                    .value();
            execution.reply().integer(applied ? 1L : 0L);
        });
    }

    private PreparedCommand persist(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        return CommandSupport.fixed(ReplyShapes.integerUpperBound(), execution -> {
            boolean applied = support.commandDb(execution).writes().ttl()
                    .persist(support.argView(request, 1))
                    .value();
            execution.reply().integer(applied ? 1L : 0L);
        });
    }

    private PreparedCommand ttl(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long value = support.commandDb(context).reads().ttl().ttlSeconds(support.argView(request, 1));
        return CommandSupport.fixed(ReplyShapes.integer(value), execution -> execution.reply().integer(value));
    }

    private PreparedCommand pttl(ArgReader args, CommandPreparationContext context) {
        ExecutionRequest request = args.request();
        long value = support.commandDb(context).reads().ttl().ttlMillis(support.argView(request, 1));
        return CommandSupport.fixed(ReplyShapes.integer(value), execution -> execution.reply().integer(value));
    }
}
