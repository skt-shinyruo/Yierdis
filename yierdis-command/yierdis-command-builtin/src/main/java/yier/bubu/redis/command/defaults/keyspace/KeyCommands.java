package yier.bubu.redis.command.defaults.keyspace;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;

import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.result.KeyScanWindow;
import yier.bubu.redis.execution.api.CommandContext;
import yier.bubu.redis.execution.api.ExecutionRequest;
import yier.bubu.redis.execution.api.ReplyPlan;
import yier.bubu.redis.execution.api.ReplyPlans;
import yier.bubu.redis.execution.api.RedisReplyWriter;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;
import java.util.function.ToLongFunction;

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
        registration.register(CommandSpec.of(syntax("TYPE", CommandArity.exact(2), KEY), CommandParsers.request(), this::type));
        registration.register(CommandSpec.of(syntax("MEMORY", CommandArity.min(2), CommandKeySpec.NONE), CommandParsers.request(), this::memory));
        registration.register(CommandSpec.of(syntax("OBJECT", CommandArity.min(2), CommandKeySpec.NONE), CommandParsers.request(), this::object));
        registration.register(CommandSpec.of(syntax("KEYS", CommandArity.exact(2), CommandKeySpec.NONE), CommandParsers.request(), this::keys));
        registration.register(CommandSpec.of(syntax("SCAN", CommandArity.min(2), CommandKeySpec.NONE), this::parseScan, this::scan));
        registration.register(CommandSpec.of(syntax("DEL", CommandArity.min(2), MULTI_KEYS), CommandParsers.request(), this::del));
        registration.register(CommandSpec.of(syntax("EXISTS", CommandArity.min(2), MULTI_KEYS), CommandParsers.request(), this::exists));
        registration.register(CommandSpec.of(syntax("EXPIRE", CommandArity.exact(3), KEY), CommandParsers.request(), this::expire));
        registration.register(CommandSpec.of(syntax("PEXPIRE", CommandArity.exact(3), KEY), CommandParsers.request(), this::pexpire));
        registration.register(CommandSpec.of(syntax("EXPIREAT", CommandArity.exact(3), KEY), CommandParsers.request(), this::expireat));
        registration.register(CommandSpec.of(syntax("PEXPIREAT", CommandArity.exact(3), KEY), CommandParsers.request(), this::pexpireat));
        registration.register(CommandSpec.of(syntax("PERSIST", CommandArity.exact(2), KEY), CommandParsers.request(), this::persist));
        registration.register(CommandSpec.of(syntax("TTL", CommandArity.exact(2), KEY), CommandParsers.request(), this::ttl));
        registration.register(CommandSpec.of(syntax("PTTL", CommandArity.exact(2), KEY), CommandParsers.request(), this::pttl));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private void type(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        ValueType t = support.commandDb(ctx).reads().keyspace().typeOf(support.argView(request, 1));
        if (t == null) {
            out.simpleString("none");
            return;
        }
        out.simpleString(t.name().toLowerCase(Locale.ROOT));
    }

    private void memory(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "USAGE")) {
            if (request.argc() != 3) {
                CommandSupport.wrongArity(out, "memory");
                return;
            }
            long bytes = support.commandDb(ctx).memory().memoryUsage(support.argView(request, 2));
            if (bytes < 0) {
                out.bulkString((byte[]) null);
                return;
            }
            out.integer(bytes);
            return;
        }

        if (CommandSupport.asciiEqualsIgnoreCase(request, 1, "STATS")) {
            if (request.argc() != 2) {
                CommandSupport.wrongArity(out, "memory");
                return;
            }

            YierdisMemoryStats s = null;
            ServerInfoProvider infoProvider = support.infoProvider();
            if (infoProvider != null) {
                s = infoProvider.memoryStats(ctx);
            }
            if (s == null) {
                s = support.commandDb(ctx).memory().memoryStats();
            }
            // Flat key/value pairs map naturally to RESP3 maps and RESP2 key/value arrays.
            out.requireReply(memoryStatsReplyPlan(s));
            out.mapHeader(MEMORY_STATS_FIELDS.length);
            for (MemoryStatField field : MEMORY_STATS_FIELDS) {
                out.bulkString(field.key());
                out.integer(field.value(s));
            }
            return;
        }

        out.error("ERR syntax error");
    }

    private static ReplyPlan memoryStatsReplyPlan(YierdisMemoryStats stats) {
        long encodedElementBytes = 0L;
        for (MemoryStatField field : MEMORY_STATS_FIELDS) {
            encodedElementBytes = addMemoryStatsPair(encodedElementBytes, field.key(), field.value(stats));
        }
        return ReplyPlans.bulkStringArray(MEMORY_STATS_FIELDS.length * 2, encodedElementBytes, 0L);
    }

    private static MemoryStatField memoryStat(
            String key,
            ToLongFunction<YierdisMemoryStats> valueExtractor
    ) {
        return new MemoryStatField(key.getBytes(StandardCharsets.US_ASCII), valueExtractor);
    }

    private static long addMemoryStatsPair(long current, byte[] key, long value) {
        long keyBytes = ReplyPlans.bulkString(key.length, 0L).encodedUpperBoundBytes();
        long valueBytes = 3L + Long.toString(value).length();
        return saturatingAdd(current, saturatingAdd(keyBytes, valueBytes));
    }

    private static long saturatingAdd(long left, long right) {
        if (left < 0L || right < 0L || left > Long.MAX_VALUE - right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private record MemoryStatField(
            byte[] key,
            ToLongFunction<YierdisMemoryStats> valueExtractor
    ) {
        private long value(YierdisMemoryStats stats) {
            return valueExtractor.applyAsLong(stats);
        }
    }

    private void object(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "object");
            return;
        }
        if (!CommandSupport.asciiEqualsIgnoreCase(request, 1, "ENCODING")) {
            out.error("ERR syntax error");
            return;
        }
        String enc = support.commandDb(ctx).memory().objectEncoding(support.argView(request, 2));
        if (enc == null) {
            out.bulkString((byte[]) null);
            return;
        }
        out.bulkString(enc.getBytes(StandardCharsets.US_ASCII));
    }

    private void keys(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        SlowCommandGovernor governor = support.slowGovernor();
        long timeBudgetNanos = governor.keysTimeBudgetNanos(ctx);
        long deadlineNanos = deadlineNanos(timeBudgetNanos);
        for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
            if (attempt > 0 && deadlineExpired(deadlineNanos)) {
                break;
            }
            long remainingBudgetNanos = remainingBudgetNanos(timeBudgetNanos, deadlineNanos);
            KeyScanWindow window = support.commandDb(ctx).reads().keyspace().keys(
                    request.readOnlyByteArray(1),
                    governor.keysMaxResults(ctx),
                    remainingBudgetNanos
            );
            boolean ownershipTransferred = false;
            try {
                if (!window.current()) {
                    continue;
                }
                CommandSupport.writeMeasuredBulkStringArray(out, window);
                ownershipTransferred = true;
                return;
            } finally {
                if (!ownershipTransferred) {
                    window.close();
                }
            }
        }
        throw new IllegalStateException("key discovery window changed before reply preflight");
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
                long v;
                try {
                    v = args.nonNegativeLongAt(++i);
                } catch (IllegalArgumentException e) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                if (v <= 0 || v > Integer.MAX_VALUE) {
                    return CommandParseResult.error(CommandParseError.integerOutOfRange());
                }
                count = (int) v;
                continue;
            }
            return CommandParseResult.error(CommandParseError.syntax());
        }
        return CommandParseResult.ok(new ScanArgs(cursor, match, count));
    }

    private void scan(ScanArgs args, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
            KeyScanWindow window = support.commandDb(ctx).reads().keyspace().scan(
                    ScanCursorV2.of(args.cursor()),
                    args.match(),
                    args.count()
            );
            boolean ownershipTransferred = false;
            try {
                if (!window.current()) {
                    continue;
                }
                byte[] nextCursor = window.nextCursor().toBulkStringAscii();
                out.requireReply(scanReplyPlan(window, nextCursor));
                out.arrayHeader(2);
                out.bulkString(nextCursor);
                out.arrayHeader(window.count());
                window.emitTo(new yier.bubu.redis.command.defaults.BulkStringReplyAdapter(out));
                out.transferReplyOwnership(window);
                ownershipTransferred = true;
                return;
            } finally {
                if (!ownershipTransferred) {
                    window.close();
                }
            }
        }
        throw new IllegalStateException("scan discovery window changed before reply preflight");
    }

    private static ReplyPlan scanReplyPlan(KeyScanWindow window, byte[] nextCursor) {
        ReplyPlan cursor = ReplyPlans.bulkString(nextCursor.length, 0L);
        ReplyPlan keys = ReplyPlans.bulkStringArray(window.count(), window.encodedElementBytes(), 0L);
        return ReplyPlans.bulkStringArray(
                2,
                saturatedAdd(cursor.encodedUpperBoundBytes(), keys.encodedUpperBoundBytes()),
                window.retainedMemoryBytes()
        );
    }

    private static long deadlineNanos(long budgetNanos) {
        if (budgetNanos <= 0L) {
            return Long.MAX_VALUE;
        }
        if (budgetNanos >= Long.MAX_VALUE - System.nanoTime()) {
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

    private static long saturatedAdd(long left, long right) {
        if (left < 0L || right < 0L || Long.MAX_VALUE - left < right) {
            return Long.MAX_VALUE;
        }
        return left + right;
    }

    private void del(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        int len = request.argc() - 1;
        support.sliceResetFromRequest(request, 1, len);
        try {
            long deleted = support.commandDb(ctx).writes().keyspace().del(support.slice()).value();
            out.integer(deleted);
        } finally {
            support.clearScratch(len);
        }
    }

    private void exists(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        long count = 0;
        for (int i = 1; i < request.argc(); i++) {
            if (support.commandDb(ctx).reads().keyspace().existsKey(support.argView(request, i))) {
                count++;
            }
        }
        out.integer(count);
    }

    private void expire(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        long seconds = CommandSupport.parseLong(request, 2, "seconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expire(support.argView(request, 1), seconds)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void pexpire(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        long millis = CommandSupport.parseLong(request, 2, "milliseconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .pexpire(support.argView(request, 1), millis)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void expireat(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        long seconds = CommandSupport.parseLong(request, 2, "seconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expireAtSeconds(support.argView(request, 1), seconds)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void pexpireat(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        long millis = CommandSupport.parseLong(request, 2, "milliseconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expireAtMillis(support.argView(request, 1), millis)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void persist(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        boolean applied = support.commandDb(ctx).writes().ttl()
                .persist(support.argView(request, 1))
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void ttl(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        out.integer(support.commandDb(ctx).reads().ttl().ttlSeconds(support.argView(request, 1)));
    }

    private void pttl(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        out.integer(support.commandDb(ctx).reads().ttl().ttlMillis(support.argView(request, 1)));
    }
}
