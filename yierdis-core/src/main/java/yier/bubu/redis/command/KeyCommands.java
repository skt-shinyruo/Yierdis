package yier.bubu.redis.command;

import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.db.YierdisMemoryStats;
import yier.bubu.redis.db.ScanCursorV2;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

final class KeyCommands {
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
    private static final byte[] MEMORY_STATS_KEYSPACE_REHASHING = "keyspace_rehashing".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_TABLE0_CAPACITY = "keyspace_table0_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_KEYSPACE_TABLE1_CAPACITY = "keyspace_table1_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_REHASHING = "expire_rehashing".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_TABLE0_CAPACITY = "expire_table0_capacity".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_EXPIRE_TABLE1_CAPACITY = "expire_table1_capacity".getBytes(StandardCharsets.US_ASCII);

    private final CommandSupport support;

    KeyCommands(CommandSupport support) {
        this.support = Objects.requireNonNull(support, "support");
    }

    void register(CommandRegistry registry) {
        Objects.requireNonNull(registry, "registry");
        registry.register("TYPE", this::type);
        registry.register("MEMORY", this::memory);
        registry.register("OBJECT", this::object);
        registry.register("KEYS", this::keys);
        registry.register("SCAN", this::scan);
        registry.register("DEL", this::del);
        registry.register("EXISTS", this::exists);
        registry.register("EXPIRE", this::expire);
        registry.register("PEXPIRE", this::pexpire);
        registry.register("EXPIREAT", this::expireat);
        registry.register("PEXPIREAT", this::pexpireat);
        registry.register("PERSIST", this::persist);
        registry.register("TTL", this::ttl);
        registry.register("PTTL", this::pttl);
    }

    private void type(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "type");
            return;
        }
        ValueType t = support.db(out).typeOf(support.argView(cmd, 1));
        if (t == null) {
            out.simpleString("none");
            return;
        }
        out.simpleString(t.name().toLowerCase(Locale.ROOT));
    }

    private void memory(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "memory");
            return;
        }

        if (CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "USAGE")) {
            if (cmd.argc() != 3) {
                CommandSupport.wrongArity(out, "memory");
                return;
            }
            long bytes = support.db(out).memoryUsage(support.argView(cmd, 2));
            if (bytes < 0) {
                out.bulkString((byte[]) null);
                return;
            }
            out.integer(bytes);
            return;
        }

        if (CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "STATS")) {
            if (cmd.argc() != 2) {
                CommandSupport.wrongArity(out, "memory");
                return;
            }

            YierdisMemoryStats s = null;
            ServerInfoProvider infoProvider = support.infoProvider();
            if (infoProvider != null) {
                s = infoProvider.memoryStats(out);
            }
            if (s == null) {
                s = support.db(out).memoryStats();
            }
            // RESP2-compatible flat array of key/value pairs; in RESP3 emit a map for friendlier clients.
            if (out.protocol() == RespProtocol.RESP3) {
                out.mapHeader(20);
            } else {
                out.arrayHeader(40);
            }

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

    private void object(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "object");
            return;
        }
        if (!CommandSupport.asciiEqualsIgnoreCase(cmd, 1, "ENCODING")) {
            out.error("ERR syntax error");
            return;
        }
        String enc = support.db(out).objectEncoding(support.argView(cmd, 2));
        if (enc == null) {
            out.bulkString((byte[]) null);
            return;
        }
        out.bulkString(enc.getBytes(StandardCharsets.US_ASCII));
    }

    private void keys(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "keys");
            return;
        }
        SlowCommandGovernor governor = support.slowGovernor();
        out.bulkStringArray(support.db(out).keys(
                cmd.toByteArray(1),
                governor.keysMaxResults(out),
                governor.keysTimeBudgetNanos(out)
        ));
    }

    private void scan(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "scan");
            return;
        }
        long cursor = CommandSupport.parseNonNegativeLong(cmd, 1, "cursor");

        byte[] match = null;
        int count = 10;
        for (int i = 2; i < cmd.argc(); i++) {
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "MATCH")) {
                if (i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                match = cmd.toByteArray(++i);
                continue;
            }
            if (CommandSupport.asciiEqualsIgnoreCase(cmd, i, "COUNT")) {
                if (i + 1 >= cmd.argc()) {
                    out.error("ERR syntax error");
                    return;
                }
                long v = CommandSupport.parseNonNegativeLong(cmd, ++i, "count");
                if (v <= 0 || v > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("value is not an integer or out of range");
                }
                count = (int) v;
                continue;
            }
            out.error("ERR syntax error");
            return;
        }

        List<byte[]> keys = new ArrayList<>();
        ScanCursorV2 next = support.db(out).scan(ScanCursorV2.of(cursor), match, count, keys);

        // Redis-compatible: reply is [cursor, keys].
        out.arrayHeader(2);
        out.bulkString(next.toBulkStringAscii());
        out.bulkStringArray(keys);
    }

    private void del(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "del");
            return;
        }
        int len = cmd.argc() - 1;
        support.sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(support.db(out).del(support.slice()));
        } finally {
            support.clearScratch(len);
        }
    }

    private void exists(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "exists");
            return;
        }

        long count = 0;
        for (int i = 1; i < cmd.argc(); i++) {
            if (support.db(out).existsKey(support.argView(cmd, i))) {
                count++;
            }
        }
        out.integer(count);
    }

    private void expire(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "expire");
            return;
        }
        long seconds = CommandSupport.parseLong(cmd, 2, "seconds");
        out.integer(support.db(out).expire(support.argView(cmd, 1), seconds) ? 1 : 0);
    }

    private void pexpire(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "pexpire");
            return;
        }
        long millis = CommandSupport.parseLong(cmd, 2, "milliseconds");
        out.integer(support.db(out).pexpire(support.argView(cmd, 1), millis) ? 1 : 0);
    }

    private void expireat(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "expireat");
            return;
        }
        long seconds = CommandSupport.parseLong(cmd, 2, "seconds");
        out.integer(support.db(out).expireAtSeconds(support.argView(cmd, 1), seconds) ? 1 : 0);
    }

    private void pexpireat(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 3) {
            CommandSupport.wrongArity(out, "pexpireat");
            return;
        }
        long millis = CommandSupport.parseLong(cmd, 2, "milliseconds");
        out.integer(support.db(out).expireAtMillis(support.argView(cmd, 1), millis) ? 1 : 0);
    }

    private void persist(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "persist");
            return;
        }
        out.integer(support.db(out).persist(support.argView(cmd, 1)) ? 1 : 0);
    }

    private void ttl(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "ttl");
            return;
        }
        out.integer(support.db(out).ttlSeconds(support.argView(cmd, 1)));
    }

    private void pttl(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "pttl");
            return;
        }
        out.integer(support.db(out).ttlMillis(support.argView(cmd, 1)));
    }
}
