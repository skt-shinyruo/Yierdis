package yier.bubu.redis.command;

import yier.bubu.redis.protocol.Command;
import yier.bubu.redis.protocol.CommandContext;
import yier.bubu.redis.protocol.ReplyWriter;

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
final class CommandRegistry {
    private static final int MIN_TABLE_SIZE = 16;
    private static final int LOAD_FACTOR_NUM = 2; // ~= 0.66
    private static final int LOAD_FACTOR_DEN = 3;

    private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV64_PRIME = 0x100000001b3L;

    @FunctionalInterface
    interface CommandHandler {
        void execute(Command cmd, CommandContext ctx);
    }

    private static final class Entry {
        private final byte[] nameUpperAscii;
        private final long hash;
        private final CommandHandler handler;

        private Entry(byte[] nameUpperAscii, long hash, CommandHandler handler) {
            this.nameUpperAscii = Objects.requireNonNull(nameUpperAscii, "nameUpperAscii");
            this.hash = hash;
            this.handler = Objects.requireNonNull(handler, "handler");
        }
    }

    private final Set<String> namesUpper = new HashSet<>();
    private Entry[] table = new Entry[MIN_TABLE_SIZE];
    private int mask = table.length - 1;
    private int resizeThreshold = threshold(table.length);
    private int size = 0;

    void register(String name, CommandHandler handler) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(handler, "handler");
        String nameUpper = name.trim().toUpperCase(Locale.ROOT);
        if (nameUpper.isEmpty()) {
            throw new IllegalArgumentException("command name must not be empty");
        }
        if (!namesUpper.add(nameUpper)) {
            throw new IllegalArgumentException("duplicate command registration: " + nameUpper);
        }

        if (size + 1 > resizeThreshold) {
            resize(table.length << 1);
        }

        byte[] ascii = asciiUpperBytes(nameUpper);
        long hash = hashUpperAscii(ascii, 0, ascii.length);
        insert(new Entry(ascii, hash, handler));
    }

    CommandHandler find(Command cmd) {
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
                return e.handler;
            }
            idx = (idx + 1) & mask;
        }
    }

    int commandCount() {
        return namesUpper.size();
    }

    boolean containsUpperName(String nameUpper) {
        if (nameUpper == null || nameUpper.isBlank()) {
            return false;
        }
        return namesUpper.contains(nameUpper.trim().toUpperCase(Locale.ROOT));
    }

    String[] upperNamesSorted() {
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
