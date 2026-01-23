package yier.bubu.redis.command;

import yier.bubu.redis.db.ValueType;
import yier.bubu.redis.db.YierdisMemoryStats;
import yier.bubu.redis.protocol.RespCommand;
import yier.bubu.redis.protocol.RespProtocol;
import yier.bubu.redis.protocol.RespWriter;

import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Objects;

final class KeyCommands {
    private static final byte[] MEMORY_STATS_MAXMEMORY_BYTES = "maxmemory_bytes".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_USED_BYTES_FOR_MAXMEMORY = "used_bytes_for_maxmemory".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_HEAP_DATA_BYTES_ESTIMATE = "heap_data_bytes_estimate".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MEMORY_STATS_OFFHEAP_USED_BYTES = "offheap_used_bytes".getBytes(StandardCharsets.US_ASCII);
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
        registry.register("DEL", this::del);
        registry.register("EXISTS", this::exists);
        registry.register("EXPIRE", this::expire);
        registry.register("TTL", this::ttl);
    }

    private void type(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "type");
            return;
        }
        ValueType t = support.db().typeOf(support.argView(cmd, 1));
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
            long bytes = support.db().memoryUsage(support.argView(cmd, 2));
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

            YierdisMemoryStats s = support.db().memoryStats();
            // RESP2-compatible flat array of key/value pairs; in RESP3 emit a map for friendlier clients.
            if (out.protocol() == RespProtocol.RESP3) {
                out.mapHeader(17);
            } else {
                out.arrayHeader(34);
            }

            out.bulkString(MEMORY_STATS_MAXMEMORY_BYTES);
            out.bulkStringLongAscii(s.maxmemoryBytes());

            out.bulkString(MEMORY_STATS_USED_BYTES_FOR_MAXMEMORY);
            out.bulkStringLongAscii(s.usedBytesForMaxmemory());

            out.bulkString(MEMORY_STATS_HEAP_DATA_BYTES_ESTIMATE);
            out.bulkStringLongAscii(s.heapDataBytesEstimate());

            out.bulkString(MEMORY_STATS_OFFHEAP_USED_BYTES);
            out.bulkStringLongAscii(s.offHeapUsedBytes());

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE_OVERHEAD_BYTES_ESTIMATE);
            out.bulkStringLongAscii(s.keyspaceTableOverheadBytesEstimate());

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE_OVERHEAD_BYTES_ESTIMATE);
            out.bulkStringLongAscii(s.expireTableOverheadBytesEstimate());

            out.bulkString(MEMORY_STATS_EXPIRE_VALUE_OBJECTS_BYTES_ESTIMATE);
            out.bulkStringLongAscii(s.expireValueObjectsBytesEstimate());

            out.bulkString(MEMORY_STATS_TOTAL_ESTIMATED_BYTES);
            out.bulkStringLongAscii(s.totalEstimatedBytes());

            out.bulkString(MEMORY_STATS_KEYS_STORED_OFFHEAP);
            out.bulkStringLongAscii(s.keysStoredOffHeap() ? 1 : 0);

            out.bulkString(MEMORY_STATS_KEY_COUNT);
            out.bulkStringLongAscii(s.keyCount());

            out.bulkString(MEMORY_STATS_EXPIRE_COUNT);
            out.bulkStringLongAscii(s.expireCount());

            out.bulkString(MEMORY_STATS_KEYSPACE_REHASHING);
            out.bulkStringLongAscii(s.keyspaceRehashing() ? 1 : 0);

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE0_CAPACITY);
            out.bulkStringLongAscii(s.keyspaceTable0Capacity());

            out.bulkString(MEMORY_STATS_KEYSPACE_TABLE1_CAPACITY);
            out.bulkStringLongAscii(s.keyspaceTable1Capacity());

            out.bulkString(MEMORY_STATS_EXPIRE_REHASHING);
            out.bulkStringLongAscii(s.expireRehashing() ? 1 : 0);

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE0_CAPACITY);
            out.bulkStringLongAscii(s.expireTable0Capacity());

            out.bulkString(MEMORY_STATS_EXPIRE_TABLE1_CAPACITY);
            out.bulkStringLongAscii(s.expireTable1Capacity());
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
        String enc = support.db().objectEncoding(support.argView(cmd, 2));
        if (enc == null) {
            out.bulkString((byte[]) null);
            return;
        }
        out.simpleString(enc);
    }

    private void keys(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "keys");
            return;
        }
        out.bulkStringArray(support.db().keys(cmd.toByteArray(1)));
    }

    private void del(RespCommand cmd, RespWriter out) {
        if (cmd.argc() < 2) {
            CommandSupport.wrongArity(out, "del");
            return;
        }
        int len = cmd.argc() - 1;
        support.sliceResetFromCommand(cmd, 1, len);
        try {
            out.integer(support.db().del(support.slice()));
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
            if (support.db().existsKey(support.argView(cmd, i))) {
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
        out.integer(support.db().expire(support.argView(cmd, 1), seconds) ? 1 : 0);
    }

    private void ttl(RespCommand cmd, RespWriter out) {
        if (cmd.argc() != 2) {
            CommandSupport.wrongArity(out, "ttl");
            return;
        }
        out.integer(support.db().ttlSeconds(support.argView(cmd, 1)));
    }
}
