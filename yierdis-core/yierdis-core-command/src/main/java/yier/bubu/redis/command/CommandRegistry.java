package yier.bubu.redis.command;

import yier.bubu.redis.contract.Command;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Command name to handler registry (SSOT) for the server command processor.
 * <p>
 * Runtime lookup is allocation-free: the registry builds an open-addressed hash table at registration time,
 * then matches commands directly against the request {@link Command} bytes (ASCII case-insensitive).
 */
final class CommandRegistry implements CommandModule.Registration {
    private static final int MIN_TABLE_SIZE = 16;
    private static final int LOAD_FACTOR_NUM = 2; // ~= 0.66
    private static final int LOAD_FACTOR_DEN = 3;

    private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV64_PRIME = 0x100000001b3L;

    private static final class Entry {
        private final byte[] nameUpperAscii;
        private final long hash;
        private final CommandSpec spec;

        private Entry(
                byte[] nameUpperAscii,
                long hash,
                CommandSpec spec
        ) {
            this.nameUpperAscii = Objects.requireNonNull(nameUpperAscii, "nameUpperAscii");
            this.hash = hash;
            this.spec = Objects.requireNonNull(spec, "spec");
        }
    }

    private final Set<String> namesUpper = new HashSet<>();
    private Entry[] table = new Entry[MIN_TABLE_SIZE];
    private int mask = table.length - 1;
    private int resizeThreshold = threshold(table.length);
    private int size = 0;

    @Override
    public void register(String name, CommandSpec spec) {
        registerInternal(name, spec);
    }

    private void registerInternal(String name, CommandSpec spec) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(spec, "spec");
        String nameUpper = name.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty()) {
            throw new IllegalArgumentException("command name must not be empty");
        }
        String disallowedInMultiError = spec.disallowedInMultiError();
        if (disallowedInMultiError != null && disallowedInMultiError.isBlank()) {
            throw new IllegalArgumentException("disallowedInMultiError must not be blank");
        }
        if (!namesUpper.add(nameUpper)) {
            throw new IllegalArgumentException("duplicate command registration: " + nameUpper);
        }

        if (size + 1 > resizeThreshold) {
            resize(table.length << 1);
        }

        byte[] ascii = asciiUpperBytes(nameUpper);
        long hash = hashUpperAscii(ascii, 0, ascii.length);
        CommandDescriptor effectiveDescriptor = spec.descriptor() == null
                ? defaultDescriptorForNameUpper(nameUpper)
                : spec.descriptor();
        insert(new Entry(
                ascii,
                hash,
                new CommandSpec(spec.handler(), effectiveDescriptor, disallowedInMultiError)
        ));
    }

    CommandModule.Handler find(Command cmd) {
        CommandSpec spec = spec(cmd);
        return spec == null ? null : spec.handler();
    }

    String disallowedInMultiError(Command cmd) {
        CommandSpec spec = spec(cmd);
        return spec == null ? null : spec.disallowedInMultiError();
    }

    CommandDescriptor descriptor(Command cmd) {
        CommandSpec spec = spec(cmd);
        return spec == null ? null : spec.descriptor();
    }

    CommandSpec spec(Command cmd) {
        Entry entry = findEntry(cmd);
        return entry == null ? null : entry.spec;
    }

    CommandSpec specByUpperName(String nameUpper) {
        if (nameUpper == null || nameUpper.isBlank()) {
            return null;
        }
        String normalized = nameUpper.trim().toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return null;
        }
        byte[] ascii;
        try {
            ascii = asciiUpperBytes(normalized);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
        Entry entry = findEntry(ascii);
        return entry == null ? null : entry.spec;
    }

    private Entry findEntry(Command cmd) {
        if (cmd == null || cmd.argc() <= 0) {
            return null;
        }
        if (cmd.isNull(0)) {
            return null;
        }
        int len = cmd.len(0);
        if (len <= 0) {
            return null;
        }

        long hash = hashUpperAscii(cmd, 0, len);
        int idx = index(hash);
        for (; ; ) {
            Entry e = table[idx];
            if (e == null) {
                return null;
            }
            if (e.hash == hash && e.nameUpperAscii.length == len && asciiEqualsIgnoreCase(cmd, 0, e.nameUpperAscii)) {
                return e;
            }
            idx = (idx + 1) & mask;
        }
    }

    private Entry findEntry(byte[] nameUpperAscii) {
        if (nameUpperAscii == null || nameUpperAscii.length == 0) {
            return null;
        }
        long hash = hashUpperAscii(nameUpperAscii, 0, nameUpperAscii.length);
        int idx = index(hash);
        for (; ; ) {
            Entry e = table[idx];
            if (e == null) {
                return null;
            }
            if (e.hash == hash && Arrays.equals(e.nameUpperAscii, nameUpperAscii)) {
                return e;
            }
            idx = (idx + 1) & mask;
        }
    }

    @Override
    public int commandCount() {
        return namesUpper.size();
    }

    @Override
    public boolean containsUpperName(String nameUpper) {
        if (nameUpper == null || nameUpper.isBlank()) {
            return false;
        }
        return namesUpper.contains(nameUpper.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String[] upperNamesSorted() {
        String[] out = namesUpper.toArray(new String[0]);
        Arrays.sort(out);
        return out;
    }

    private void insert(Entry entry) {
        int idx = index(entry.hash);
        for (; ; ) {
            if (table[idx] == null) {
                table[idx] = entry;
                size++;
                return;
            }
            idx = (idx + 1) & mask;
        }
    }

    private void resize(int newCapacity) {
        int capacity = Math.max(MIN_TABLE_SIZE, nextPowerOfTwo(newCapacity));
        Entry[] old = table;
        table = new Entry[capacity];
        mask = capacity - 1;
        resizeThreshold = threshold(capacity);
        size = 0;
        for (int i = 0; i < old.length; i++) {
            Entry e = old[i];
            if (e == null) {
                continue;
            }
            insert(e);
        }
    }

    private int index(long hash) {
        int h = (int) (hash ^ (hash >>> 32));
        h ^= (h >>> 16);
        return h & mask;
    }

    private static int threshold(int capacity) {
        return (capacity * LOAD_FACTOR_NUM) / LOAD_FACTOR_DEN;
    }

    private static int nextPowerOfTwo(int x) {
        int v = Math.max(MIN_TABLE_SIZE, x);
        int highest = Integer.highestOneBit(v);
        return v == highest ? v : highest << 1;
    }

    private static CommandDescriptor defaultDescriptorForNameUpper(String nameUpper) {
        return CommandDescriptor.of(
                defaultArity(nameUpper),
                defaultFirstKeyIndex(nameUpper),
                defaultLastKeyIndex(nameUpper),
                defaultKeyStep(nameUpper)
        );
    }

    private static int defaultArity(String nameUpper) {
        if (nameUpper == null) {
            return -1;
        }
        return switch (nameUpper) {
            case "PING", "COMMAND" -> -1;
            case "ECHO", "SELECT" -> 2;
            case "QUIT", "FLUSHDB" -> 1;
            case "TYPE", "KEYS", "TTL", "GET", "STRLEN", "INCR", "DECR", "SMEMBERS", "SCARD", "HGETALL", "HLEN" -> 2;
            case "EXPIRE", "APPEND", "HGET", "SISMEMBER", "GETBIT" -> 3;
            case "SETBIT", "LRANGE", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE" -> 4;
            case "DEL", "EXISTS", "MEMORY", "OBJECT", "BITCOUNT", "LPOP", "RPOP", "PFCOUNT" -> -2;
            case "SET", "LPUSH", "RPUSH", "SADD", "SREM", "HDEL", "ZREM", "PFADD", "PFMERGE" -> -3;
            case "HSET", "ZADD", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE" -> -4;
            default -> -1;
        };
    }

    private static int defaultFirstKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        return switch (nameUpper) {
            case "PING", "ECHO", "COMMAND", "QUIT", "FLUSHDB", "SELECT", "KEYS", "MEMORY", "OBJECT" -> 0;
            default -> 1;
        };
    }

    private static int defaultLastKeyIndex(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        return switch (nameUpper) {
            case "DEL", "EXISTS", "PFCOUNT", "PFMERGE" -> -1;
            case "PING", "ECHO", "COMMAND", "SELECT", "QUIT", "FLUSHDB", "KEYS", "MEMORY", "OBJECT" -> 0;
            default -> 1;
        };
    }

    private static int defaultKeyStep(String nameUpper) {
        if (nameUpper == null) {
            return 0;
        }
        return switch (nameUpper) {
            case "PING", "ECHO", "COMMAND", "QUIT", "FLUSHDB", "SELECT", "KEYS", "MEMORY", "OBJECT" -> 0;
            default -> 1;
        };
    }

    private static byte[] asciiUpperBytes(String nameUpper) {
        for (int i = 0; i < nameUpper.length(); i++) {
            if (nameUpper.charAt(i) > 0x7F) {
                throw new IllegalArgumentException("command name must be ASCII: " + nameUpper);
            }
        }
        byte[] bytes = nameUpper.getBytes(StandardCharsets.US_ASCII);
        return bytes;
    }

    private static boolean asciiEqualsIgnoreCase(Command cmd, int argIndex, byte[] upperAscii) {
        if (cmd.len(argIndex) != upperAscii.length) {
            return false;
        }
        for (int i = 0; i < upperAscii.length; i++) {
            int b = cmd.byteAt(argIndex, i) & 0xFF;
            if (b >= 'a' && b <= 'z') {
                b -= 32;
            }
            if ((byte) b != upperAscii[i]) {
                return false;
            }
        }
        return true;
    }

    private static long hashUpperAscii(Command cmd, int argIndex, int len) {
        long h = FNV64_OFFSET_BASIS;
        for (int i = 0; i < len; i++) {
            int b = cmd.byteAt(argIndex, i) & 0xFF;
            if (b >= 'a' && b <= 'z') {
                b -= 32;
            }
            h ^= b;
            h *= FNV64_PRIME;
        }
        return h;
    }

    private static long hashUpperAscii(byte[] asciiUpper, int off, int len) {
        long h = FNV64_OFFSET_BASIS;
        for (int i = 0; i < len; i++) {
            h ^= (asciiUpper[off + i] & 0xFF);
            h *= FNV64_PRIME;
        }
        return h;
    }
}
