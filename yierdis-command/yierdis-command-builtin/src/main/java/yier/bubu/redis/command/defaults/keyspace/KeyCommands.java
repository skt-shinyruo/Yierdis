package yier.bubu.redis.command.defaults.keyspace;

import yier.bubu.redis.command.api.ArgReader;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandDescriptor;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseError;
import yier.bubu.redis.command.api.CommandParseResult;
import yier.bubu.redis.command.api.CommandParsers;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
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

public final class KeyCommands implements CommandModule {
    private static final int KEY_WINDOW_DISCOVERY_ATTEMPTS = 2;
    private static final byte[] MEMORY_STATS_MAXMEMORY_BYTES = "maxmemory_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_USED_BYTES_FOR_MAXMEMORY = "used_bytes_for_maxmemory".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EFFECTIVE_USED_BYTES_FOR_MAXMEMORY = "effective_used_bytes_for_maxmemory".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_LEDGER_USED_BYTES = "ledger_used_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_LEDGER_RESERVED_BYTES = "ledger_reserved_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_OFFHEAP_USED_BYTES = "offheap_used_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_OFFHEAP_INCLUDED_IN_MAXMEMORY = "offheap_included_in_maxmemory".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_TABLE_OVERHEAD_BYTES_ESTIMATE = "keyspace_table_overhead_bytes_estimate".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_TABLE_OVERHEAD_BYTES_ESTIMATE = "expire_table_overhead_bytes_estimate".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_VALUE_OBJECTS_BYTES_ESTIMATE = "expire_value_objects_bytes_estimate".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_TOTAL_ESTIMATED_BYTES = "total_estimated_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYS_STORED_OFFHEAP = "keys_stored_offheap".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEY_COUNT = "key_count".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_COUNT = "expire_count".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRED_ENTRIES_AWAITING_PHYSICAL_DELETION =
            "expired_entries_awaiting_physical_deletion".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_REHASHING = "keyspace_rehashing".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_TABLE0_CAPACITY = "keyspace_table0_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_TABLE1_CAPACITY = "keyspace_table1_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_REHASHING = "expire_rehashing".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_TABLE0_CAPACITY = "expire_table0_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_TABLE1_CAPACITY = "expire_table1_capacity".getBytes(StandardCharsets.US_ASCII);

    private final CommandSupport support;

    public KeyCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    @Override
    public void register(CommandModule.Registration registration) {
        Objects.requireNonNull(registration, "registration");
        registration.register("TYPE", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "type"), this::type);
        registration.register("MEMORY", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(2, "memory"), this::memory);
        registration.register("OBJECT", CommandDescriptor.of(-2, 0, 0, 0), CommandParsers.minRequest(2, "object"), this::object);
        registration.register("KEYS", CommandDescriptor.of(2, 0, 0, 0), CommandParsers.exactRequest(2, "keys"), this::keys);
        registration.register("SCAN", CommandDescriptor.of(-2, 0, 0, 0), this::parseScan, this::scan);
        registration.register("DEL", CommandDescriptor.of(-2, 1, -1, 1), CommandParsers.minRequest(2, "del"), this::del);
        registration.register("EXISTS", CommandDescriptor.of(-2, 1, -1, 1), CommandParsers.minRequest(2, "exists"), this::exists);
        registration.register("EXPIRE", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "expire"), this::expire);
        registration.register("PEXPIRE", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "pexpire"), this::pexpire);
        registration.register("EXPIREAT", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "expireat"), this::expireat);
        registration.register("PEXPIREAT", CommandDescriptor.of(3, 1, 1, 1), CommandParsers.exactRequest(3, "pexpireat"), this::pexpireat);
        registration.register("PERSIST", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "persist"), this::persist);
        registration.register("TTL", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "ttl"), this::ttl);
        registration.register("PTTL", CommandDescriptor.of(2, 1, 1, 1), CommandParsers.exactRequest(2, "pttl"), this::pttl);
    }

    private void type(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "type");
            return;
        }
        ValueType t = support.commandDb(ctx).reads().keyspace().typeOf(support.argView(request, 1));
        if (t == null) {
            out.simpleString("none");
            return;
        }
        out.simpleString(t.name().toLowerCase(Locale.ROOT));
    }

    private void memory(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() < 2) {
            CommandSupport.wrongArity(out, "memory");
            return;
        }

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
            out.mapHeader(21);

            out.bulkString(MEMORY_STATS_MAXMEMORY_BYTES);
            out.integer(s.maxmemoryBytes());

            out.bulkString(MEMORY_STATS_USED_BYTES_FOR_MAXMEMORY);
            out.integer(s.usedBytesForMaxmemory());

            out.bulkString(MEMORY_STATS_EFFECTIVE_USED_BYTES_FOR_MAXMEMORY);
            out.integer(s.effectiveUsedBytesForMaxmemory());

            out.bulkString(MEMORY_STATS_LEDGER_USED_BYTES);
            out.integer(s.heapDataBytesEstimate());

            out.bulkString(MEMORY_STATS_OFFHEAP_USED_BYTES);
            out.integer(s.offHeapUsedBytes());

            out.bulkString(MEMORY_STATS_LEDGER_RESERVED_BYTES);
            out.integer(s.reservedBytes());

            out.bulkString(MEMORY_STATS_OFFHEAP_INCLUDED_IN_MAXMEMORY);
            out.integer(s.offHeapIncludedInMaxmemory() ? 1 : 0);

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE_OVERHEAD_BYTES_ESTIMATE);
            out.integer(s.keyspaceTableOverheadBytesEstimate());

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE_OVERHEAD_BYTES_ESTIMATE);
            out.integer(s.expireTableOverheadBytesEstimate());

            out.bulkString(MEMORY_STATS_EXPIRE_VALUE_OBJECTS_BYTES_ESTIMATE);
            out.integer(s.expireValueObjectsBytesEstimate());

            out.bulkString(MEMORY_STATS_TOTAL_ESTIMATED_BYTES);
            out.integer(s.totalEstimatedBytes());

            out.bulkString(MEMORY_STATS_KEYS_STORED_OFFHEAP);
            out.integer(s.keysStoredOffHeap() ? 1 : 0);

            out.bulkString(MEMORY_STATS_KEY_COUNT);
            out.integer(s.keyCount());

            out.bulkString(MEMORY_STATS_EXPIRE_COUNT);
            out.integer(s.expireCount());

            out.bulkString(MEMORY_STATS_EXPIRED_ENTRIES_AWAITING_PHYSICAL_DELETION);
            out.integer(s.expiredEntriesAwaitingPhysicalDeletion());

            out.bulkString(MEMORY_STATS_KEYSPACE_REHASHING);
            out.integer(s.keyspaceRehashing() ? 1 : 0);

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE0_CAPACITY);
            out.integer(s.keyspaceTable0Capacity());

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE1_CAPACITY);
            out.integer(s.keyspaceTable1Capacity());

            out.bulkString(MEMORY_STATS_EXPIRE_REHASHING);
            out.integer(s.expireRehashing() ? 1 : 0);

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE0_CAPACITY);
            out.integer(s.expireTable0Capacity());

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE1_CAPACITY);
            out.integer(s.expireTable1Capacity());
            return;
        }

        out.error("ERR syntax error");
    }

    private static ReplyPlan memoryStatsReplyPlan(YierdisMemoryStats stats) {
        long encodedElementBytes = 0L;
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_MAXMEMORY_BYTES, stats.maxmemoryBytes());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_USED_BYTES_FOR_MAXMEMORY, stats.usedBytesForMaxmemory());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_EFFECTIVE_USED_BYTES_FOR_MAXMEMORY, stats.effectiveUsedBytesForMaxmemory());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_LEDGER_USED_BYTES, stats.heapDataBytesEstimate());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_OFFHEAP_USED_BYTES, stats.offHeapUsedBytes());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_LEDGER_RESERVED_BYTES, stats.reservedBytes());
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_OFFHEAP_INCLUDED_IN_MAXMEMORY,
                stats.offHeapIncludedInMaxmemory() ? 1L : 0L
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_KEYSPACE_TABLE_OVERHEAD_BYTES_ESTIMATE,
                stats.keyspaceTableOverheadBytesEstimate()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRE_TABLE_OVERHEAD_BYTES_ESTIMATE,
                stats.expireTableOverheadBytesEstimate()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRE_VALUE_OBJECTS_BYTES_ESTIMATE,
                stats.expireValueObjectsBytesEstimate()
        );
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_TOTAL_ESTIMATED_BYTES, stats.totalEstimatedBytes());
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_KEYS_STORED_OFFHEAP,
                stats.keysStoredOffHeap() ? 1L : 0L
        );
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_KEY_COUNT, stats.keyCount());
        encodedElementBytes = addMemoryStatsPair(encodedElementBytes, MEMORY_STATS_EXPIRE_COUNT, stats.expireCount());
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRED_ENTRIES_AWAITING_PHYSICAL_DELETION,
                stats.expiredEntriesAwaitingPhysicalDeletion()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_KEYSPACE_REHASHING,
                stats.keyspaceRehashing() ? 1L : 0L
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_KEYSPACE_TABLE0_CAPACITY,
                stats.keyspaceTable0Capacity()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_KEYSPACE_TABLE1_CAPACITY,
                stats.keyspaceTable1Capacity()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRE_REHASHING,
                stats.expireRehashing() ? 1L : 0L
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRE_TABLE0_CAPACITY,
                stats.expireTable0Capacity()
        );
        encodedElementBytes = addMemoryStatsPair(
                encodedElementBytes,
                MEMORY_STATS_EXPIRE_TABLE1_CAPACITY,
                stats.expireTable1Capacity()
        );
        return ReplyPlans.bulkStringArray(42, encodedElementBytes, 0L);
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
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "keys");
            return;
        }
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
        CommandParseError arity = CommandArity.min(2, "scan").validate(args);
        if (arity != null) {
            return CommandParseResult.error(arity);
        }
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
        if (request.argc() < 2) {
            CommandSupport.wrongArity(out, "del");
            return;
        }
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
        if (request.argc() < 2) {
            CommandSupport.wrongArity(out, "exists");
            return;
        }

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
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "expire");
            return;
        }
        long seconds = CommandSupport.parseLong(request, 2, "seconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expire(support.argView(request, 1), seconds)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void pexpire(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "pexpire");
            return;
        }
        long millis = CommandSupport.parseLong(request, 2, "milliseconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .pexpire(support.argView(request, 1), millis)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void expireat(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "expireat");
            return;
        }
        long seconds = CommandSupport.parseLong(request, 2, "seconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expireAtSeconds(support.argView(request, 1), seconds)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void pexpireat(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 3) {
            CommandSupport.wrongArity(out, "pexpireat");
            return;
        }
        long millis = CommandSupport.parseLong(request, 2, "milliseconds");
        boolean applied = support.commandDb(ctx).writes().ttl()
                .expireAtMillis(support.argView(request, 1), millis)
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void persist(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "persist");
            return;
        }
        boolean applied = support.commandDb(ctx).writes().ttl()
                .persist(support.argView(request, 1))
                .value();
        out.integer(applied ? 1 : 0);
    }

    private void ttl(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "ttl");
            return;
        }
        out.integer(support.commandDb(ctx).reads().ttl().ttlSeconds(support.argView(request, 1)));
    }

    private void pttl(ExecutionRequest request, CommandContext ctx) {
        RedisReplyWriter out = ctx.out();
        if (request.argc() != 2) {
            CommandSupport.wrongArity(out, "pttl");
            return;
        }
        out.integer(support.commandDb(ctx).reads().ttl().ttlMillis(support.argView(request, 1)));
    }
}
