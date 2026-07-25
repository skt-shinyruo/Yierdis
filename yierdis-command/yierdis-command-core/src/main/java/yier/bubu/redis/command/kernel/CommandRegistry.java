package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandDefinition;
import yier.bubu.redis.execution.api.ExecutionRequest;

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
 * then matches commands directly against the request {@link ExecutionRequest} bytes (ASCII case-insensitive).
 */
public final class CommandRegistry implements CommandModule.Registration {
    private static final int MIN_TABLE_SIZE = 16;
    private static final int LOAD_FACTOR_NUM = 2; // ~= 0.66
    private static final int LOAD_FACTOR_DEN = 3;

    private static final long FNV64_OFFSET_BASIS = 0xcbf29ce484222325L;
    private static final long FNV64_PRIME = 0x100000001b3L;

    private static final class Entry {
        private final byte[] nameUpperAscii;
        private final long hash;
        private final CommandDefinition<?> definition;

        private Entry(
                byte[] nameUpperAscii,
                long hash,
                CommandDefinition<?> definition
        ) {
            this.nameUpperAscii = Objects.requireNonNull(nameUpperAscii, "nameUpperAscii");
            this.hash = hash;
            this.definition = Objects.requireNonNull(definition, "definition");
        }
    }

    private final Set<String> namesUpper = new HashSet<>();
    private Entry[] table = new Entry[MIN_TABLE_SIZE];
    private int mask = table.length - 1;
    private int resizeThreshold = threshold(table.length);
    private int size = 0;

    @Override
    public void register(CommandDefinition<?> definition) {
        registerInternal(definition);
    }

    private void registerInternal(CommandDefinition<?> definition) {
        Objects.requireNonNull(definition, "definition");
        String nameUpper = definition.syntax().nameUpper();
        if (!namesUpper.add(nameUpper)) {
            throw new IllegalArgumentException("duplicate command registration: " + nameUpper);
        }

        if (size + 1 > resizeThreshold) {
            resize(table.length << 1);
        }

        byte[] ascii = asciiUpperBytes(nameUpper);
        long hash = hashUpperAscii(ascii, 0, ascii.length);
        insert(new Entry(
                ascii,
                hash,
                definition
        ));
    }

    CommandDefinition<?> definition(ExecutionRequest request) {
        Entry entry = findEntry(request);
        return entry == null ? null : entry.definition;
    }

    @Override
    public CommandDefinition<?> definitionByUpperName(String nameUpper) {
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
        return entry == null ? null : entry.definition;
    }

    private Entry findEntry(ExecutionRequest request) {
        if (request == null || request.argc() <= 0) {
            return null;
        }
        if (request.isNull(0)) {
            return null;
        }
        int len = request.len(0);
        if (len <= 0) {
            return null;
        }

        long hash = hashUpperAscii(request, 0, len);
        int idx = index(hash);
        for (; ; ) {
            Entry e = table[idx];
            if (e == null) {
                return null;
            }
            if (e.hash == hash && e.nameUpperAscii.length == len && asciiEqualsIgnoreCase(request, 0, e.nameUpperAscii)) {
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

    private static byte[] asciiUpperBytes(String nameUpper) {
        for (int i = 0; i < nameUpper.length(); i++) {
            if (nameUpper.charAt(i) > 0x7F) {
                throw new IllegalArgumentException("command name must be ASCII: " + nameUpper);
            }
        }
        byte[] bytes = nameUpper.getBytes(StandardCharsets.US_ASCII);
        return bytes;
    }

    private static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, byte[] upperAscii) {
        if (request.len(argIndex) != upperAscii.length) {
            return false;
        }
        for (int i = 0; i < upperAscii.length; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
            if (b >= 'a' && b <= 'z') {
                b -= 32;
            }
            if ((byte) b != upperAscii[i]) {
                return false;
            }
        }
        return true;
    }

    private static long hashUpperAscii(ExecutionRequest request, int argIndex, int len) {
        long h = FNV64_OFFSET_BASIS;
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
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
