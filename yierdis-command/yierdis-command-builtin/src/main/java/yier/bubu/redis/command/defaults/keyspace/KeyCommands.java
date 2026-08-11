package yier.bubu.redis.command.defaults.keyspace;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.ToLongFunction;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.command.api.CommandArgs;
import yier.bubu.redis.command.api.CommandArity;
import yier.bubu.redis.command.api.CommandInvocation;
import yier.bubu.redis.command.api.CommandKeySpec;
import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandParseException;
import yier.bubu.redis.command.api.CommandSpec;
import yier.bubu.redis.command.api.CommandSyntax;
import yier.bubu.redis.command.api.ServerInfoProvider;
import yier.bubu.redis.command.api.SlowCommandGovernor;
import yier.bubu.redis.command.api.TransactionPolicy;
import yier.bubu.redis.command.defaults.CommandSupport;
import yier.bubu.redis.command.defaults.DbReplies;
import yier.bubu.redis.execution.api.CommandResult;
import yier.bubu.redis.execution.api.PreparedCommand;
import yier.bubu.redis.execution.api.PreparedCommands;
import yier.bubu.redis.execution.api.RedisReplies;
import yier.bubu.redis.execution.api.RedisReply;
import yier.bubu.redis.execution.api.ReplyShapes;
import yier.bubu.redis.execution.api.ValidationResult;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.api.WrongTypeException;
import yier.bubu.redis.storage.api.YierdisCommandException;
import yier.bubu.redis.storage.api.YierdisMemoryStats;
import yier.bubu.redis.storage.api.result.KeyScanWindow;

public final class KeyCommands implements CommandModule {
    private static final int KEY_WINDOW_DISCOVERY_ATTEMPTS = 2;
    private static final String KEYS_INCOMPLETE_ERROR = "ERR KEYS scan incomplete; use SCAN";
    private static final String SYNTAX_ERROR = "ERR syntax error";
    private static final String INTEGER_ERROR = "ERR value is not an integer or out of range";
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
        registration.register(new CommandSpec(syntax("TYPE", CommandArity.exact(2), KEY), this::type));
        registration.register(new CommandSpec(syntax("MEMORY", CommandArity.min(2), CommandKeySpec.NONE), this::memory));
        registration.register(new CommandSpec(syntax("OBJECT", CommandArity.min(2), CommandKeySpec.NONE), this::object));
        registration.register(new CommandSpec(syntax("KEYS", CommandArity.exact(2), CommandKeySpec.NONE), this::keys));
        registration.register(new CommandSpec(syntax("SCAN", CommandArity.min(2), CommandKeySpec.NONE), this::scan));
        registration.register(new CommandSpec(syntax("DEL", CommandArity.min(2), MULTI_KEYS), this::del));
        registration.register(new CommandSpec(syntax("EXISTS", CommandArity.min(2), MULTI_KEYS), this::exists));
        registration.register(new CommandSpec(syntax("EXPIRE", CommandArity.exact(3), KEY), this::expire));
        registration.register(new CommandSpec(syntax("PEXPIRE", CommandArity.exact(3), KEY), this::pexpire));
        registration.register(new CommandSpec(syntax("EXPIREAT", CommandArity.exact(3), KEY), this::expireat));
        registration.register(new CommandSpec(syntax("PEXPIREAT", CommandArity.exact(3), KEY), this::pexpireat));
        registration.register(new CommandSpec(syntax("PERSIST", CommandArity.exact(2), KEY), this::persist));
        registration.register(new CommandSpec(syntax("TTL", CommandArity.exact(2), KEY), this::ttl));
        registration.register(new CommandSpec(syntax("PTTL", CommandArity.exact(2), KEY), this::pttl));
    }

    private static CommandSyntax syntax(String nameUpper, CommandArity arity, CommandKeySpec keys) {
        return new CommandSyntax(nameUpper, arity, keys, TransactionPolicy.QUEUEABLE);
    }

    private CommandInvocation type(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> {
            ValueType valueType = support.commandDb(session).reads().keyspace().typeOf(key);
            String value = valueType == null ? "none" : valueType.name().toLowerCase(Locale.ROOT);
            return PreparedCommands.ready(RedisReplies.simpleString(value));
        };
    }

    private CommandInvocation memory(CommandArgs args) throws CommandParseException {
        if (args.is(1, "USAGE")) {
            if (args.argc() != 3) {
                throw new CommandParseException("ERR wrong number of arguments for 'memory' command");
            }
            BytesSlice key = args.slice(2);
            return session -> {
                long bytes = support.commandDb(session).memory().memoryUsage(key);
                RedisReply reply = bytes < 0L ? RedisReplies.nullValue() : RedisReplies.integer(bytes);
                return PreparedCommands.ready(reply);
            };
        }
        if (args.is(1, "STATS")) {
            if (args.argc() != 2) {
                throw new CommandParseException("ERR wrong number of arguments for 'memory' command");
            }
            return session -> {
                ServerInfoProvider infoProvider = support.infoProvider();
                YierdisMemoryStats stats = infoProvider == null ? null : infoProvider.memoryStats(session);
                if (stats == null) {
                    stats = support.commandDb(session).memory().memoryStats();
                }
                return PreparedCommands.ready(memoryStats(stats));
            };
        }
        throw syntaxFailure();
    }

    private static RedisReply memoryStats(YierdisMemoryStats stats) {
        ArrayList<RedisReply> fields = new ArrayList<>(MEMORY_STATS_FIELDS.length * 2);
        for (MemoryStatField field : MEMORY_STATS_FIELDS) {
            fields.add(RedisReplies.bulkString(field.key()));
            fields.add(RedisReplies.integer(field.value(stats)));
        }
        return RedisReplies.map(fields);
    }

    private static MemoryStatField memoryStat(String key, ToLongFunction<YierdisMemoryStats> valueExtractor) {
        return new MemoryStatField(key.getBytes(StandardCharsets.US_ASCII), valueExtractor);
    }

    private record MemoryStatField(byte[] key, ToLongFunction<YierdisMemoryStats> valueExtractor) {
        private long value(YierdisMemoryStats stats) {
            return valueExtractor.applyAsLong(stats);
        }
    }

    private CommandInvocation object(CommandArgs args) throws CommandParseException {
        if (args.argc() != 3) {
            throw new CommandParseException("ERR wrong number of arguments for 'object' command");
        }
        if (!args.is(1, "ENCODING")) {
            throw syntaxFailure();
        }
        BytesSlice key = args.slice(2);
        return session -> {
            String encoding = support.commandDb(session).memory().objectEncoding(key);
            return PreparedCommands.ready(encoding == null
                    ? RedisReplies.nullValue()
                    : RedisReplies.bulkString(encoding.getBytes(StandardCharsets.US_ASCII)));
        };
    }

    private CommandInvocation keys(CommandArgs args) {
        byte[] pattern = args.bytes(1);
        return session -> {
            SlowCommandGovernor governor = support.slowGovernor();
            long timeBudgetNanos = governor.keysTimeBudgetNanos(session);
            long deadlineNanos = deadlineNanos(timeBudgetNanos);
            for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
                if (attempt > 0 && deadlineExpired(deadlineNanos)) {
                    break;
                }
                long remainingBudgetNanos = remainingBudgetNanos(timeBudgetNanos, deadlineNanos);
                KeyScanWindow window = support.commandDb(session).reads().keyspace().keys(
                        pattern,
                        governor.keysMaxResults(session),
                        remainingBudgetNanos
                );
                if (!window.current()) {
                    window.close();
                    continue;
                }
                if (window.nextCursor().value() != 0L) {
                    window.close();
                    return PreparedCommands.ready(RedisReplies.error(KEYS_INCOMPLETE_ERROR));
                }
                return keyWindowReply(window);
            }
            return PreparedCommands.ready(RedisReplies.error(
                    "ERR key discovery window changed before reply preflight"));
        };
    }

    private record ScanArgs(long cursor, byte[] match, int count) {
    }

    private CommandInvocation scan(CommandArgs args) throws CommandParseException {
        ScanArgs parsed = scanArgs(args);
        return session -> {
            for (int attempt = 0; attempt < KEY_WINDOW_DISCOVERY_ATTEMPTS; attempt++) {
                KeyScanWindow window = support.commandDb(session).reads().keyspace().scan(
                        ScanCursorV2.of(parsed.cursor()), parsed.match(), parsed.count());
                if (!window.current()) {
                    window.close();
                    continue;
                }
                return scanWindowReply(window);
            }
            return PreparedCommands.ready(RedisReplies.error(
                    "ERR scan window changed before reply preflight"));
        };
    }

    private static ScanArgs scanArgs(CommandArgs args) throws CommandParseException {
        long cursor = args.nonNegativeLongAt(1);
        byte[] match = null;
        int count = 10;
        for (int index = 2; index < args.argc(); index++) {
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
                long value = args.positiveLongAt(index);
                if (value > Integer.MAX_VALUE) {
                    throw new CommandParseException(INTEGER_ERROR);
                }
                count = (int) value;
                continue;
            }
            throw syntaxFailure();
        }
        return new ScanArgs(cursor, match, count);
    }

    private static PreparedCommand keyWindowReply(KeyScanWindow window) {
        RedisReply reply = DbReplies.sequence(window);
        return PreparedCommands.ownedAction(
                reply.shape(),
                window,
                () -> window.current() ? ValidationResult.VALID : ValidationResult.STALE,
                context -> CommandResult.reply(reply)
        );
    }

    private static PreparedCommand scanWindowReply(KeyScanWindow window) {
        RedisReply elements = DbReplies.sequence(window);
        RedisReply reply = RedisReplies.array(List.of(
                RedisReplies.bulkString(window.nextCursor().toAsciiBytes()),
                elements
        ));
        return PreparedCommands.ownedAction(
                reply.shape(),
                window,
                () -> window.current() ? ValidationResult.VALID : ValidationResult.STALE,
                context -> CommandResult.reply(reply)
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

    private CommandInvocation del(CommandArgs args) {
        List<byte[]> keys = args.byteArraysFrom(1);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                long deleted = support.commandDb(execution).writes().keyspace().del(keys).value();
                return CommandResult.reply(RedisReplies.integer(deleted));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation exists(CommandArgs args) {
        List<BytesSlice> keys = new ArrayList<>(args.argc() - 1);
        for (int index = 1; index < args.argc(); index++) {
            keys.add(args.slice(index));
        }
        return session -> {
            long count = 0L;
            for (BytesSlice key : keys) {
                if (support.commandDb(session).reads().keyspace().existsKey(key)) {
                    count++;
                }
            }
            return PreparedCommands.ready(RedisReplies.integer(count));
        };
    }

    private record TtlArgs(BytesSlice key, long value) {
    }

    private CommandInvocation expire(CommandArgs args) throws CommandParseException {
        TtlArgs parsed = ttlArgs(args);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                boolean applied = support.commandDb(execution).writes().ttl().expire(parsed.key(), parsed.value()).value();
                return CommandResult.reply(RedisReplies.integer(applied ? 1L : 0L));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation pexpire(CommandArgs args) throws CommandParseException {
        TtlArgs parsed = ttlArgs(args);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                boolean applied = support.commandDb(execution).writes().ttl().pexpire(parsed.key(), parsed.value()).value();
                return CommandResult.reply(RedisReplies.integer(applied ? 1L : 0L));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation expireat(CommandArgs args) throws CommandParseException {
        TtlArgs parsed = ttlArgs(args);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                boolean applied = support.commandDb(execution).writes().ttl()
                        .expireAtSeconds(parsed.key(), parsed.value()).value();
                return CommandResult.reply(RedisReplies.integer(applied ? 1L : 0L));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation pexpireat(CommandArgs args) throws CommandParseException {
        TtlArgs parsed = ttlArgs(args);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                boolean applied = support.commandDb(execution).writes().ttl()
                        .expireAtMillis(parsed.key(), parsed.value()).value();
                return CommandResult.reply(RedisReplies.integer(applied ? 1L : 0L));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private static TtlArgs ttlArgs(CommandArgs args) throws CommandParseException {
        return new TtlArgs(args.slice(1), args.longAt(2));
    }

    private CommandInvocation persist(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.action(ReplyShapes.integerUpperBound(), execution -> {
            try {
                boolean applied = support.commandDb(execution).writes().ttl().persist(key).value();
                return CommandResult.reply(RedisReplies.integer(applied ? 1L : 0L));
            } catch (WrongTypeException | YierdisCommandException failure) {
                return CommandResult.controlError(failure.getMessage());
            }
        });
    }

    private CommandInvocation ttl(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().ttl().ttlSeconds(key)));
    }

    private CommandInvocation pttl(CommandArgs args) {
        BytesSlice key = args.slice(1);
        return session -> PreparedCommands.ready(RedisReplies.integer(
                support.commandDb(session).reads().ttl().ttlMillis(key)));
    }

    private static CommandParseException syntaxFailure() {
        return new CommandParseException(SYNTAX_ERROR);
    }
}
