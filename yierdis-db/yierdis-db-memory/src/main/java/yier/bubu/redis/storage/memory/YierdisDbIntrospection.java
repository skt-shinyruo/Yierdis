package yier.bubu.redis.storage.memory;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.ValueType;
import yier.bubu.redis.storage.memory.internal.entry.EntryRecord;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

import java.util.List;
import java.util.Objects;

public final class YierdisDbIntrospection implements YierdisSnapshot {
    private final Runnable threadChecker;
    private final YierdisDbKeyLifecycle keyLifecycle;

    YierdisDbIntrospection(Runnable threadChecker, YierdisDbKeyLifecycle keyLifecycle) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
    }

    String objectEncoding(BytesView keyView) {
        threadChecker.run();
        EntryRecord record = keyLifecycle.liveEntryRecord(keyView);
        if (record == null) {
            return null;
        }
        return encodingName(record.encoding());
    }

    String objectEncoding(byte[] keyBytes) {
        threadChecker.run();
        if (keyBytes == null) {
            return null;
        }
        EntryRecord record = keyLifecycle.liveEntryRecord(keyBytes);
        if (record == null) {
            return null;
        }
        return encodingName(record.encoding());
    }

    @Override
    public ScanCursorV2 snapshot(ScanCursorV2 cursor, int count, List<YierdisSnapshotEntry> out) {
        threadChecker.run();
        Objects.requireNonNull(out, "out");
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }

        long now = System.currentTimeMillis();
        int maxSteps = Math.max(64, count * 10);
        final int[] remaining = new int[]{count};

        return keyLifecycle.scan(cursor == null ? ScanCursorV2.start() : cursor, maxSteps, (k, record) -> {
            if (k == null || record == null) {
                return true;
            }
            if (keyLifecycle.isKeyExpired(k, now)) {
                return true;
            }

            byte[] keyBytes = YierdisDb.toByteArray(k);
            ValueType type = record.type();
            byte[] stringValue = null;
            if (type == ValueType.STRING) {
                ValueHandle handle = record.valueHandle();
                stringValue = handle == null ? null : keyLifecycle.stringRoot().copy(handle);
            }
            Long expireAtMillis;
            if (record.expireAtMillis() < 0) {
                expireAtMillis = keyLifecycle.expireAtMillis(k);
            } else {
                expireAtMillis = record.expireAtMillis();
            }
            out.add(new YierdisSnapshotEntry(keyBytes, type, stringValue, expireAtMillis));

            remaining[0]--;
            return remaining[0] > 0;
        });
    }

    private static String encodingName(ValueEncoding encoding) {
        if (encoding == null) {
            return "unknown";
        }
        switch (encoding) {
            case STRING_INT:
                return "int";
            case STRING_EMBSTR:
                return "embstr";
            case STRING_RAW:
                return "raw";
            case HASH_PACKED:
            case LIST_PACKED:
            case ZSET_PACKED:
                return "listpack";
            case HASH_HT:
            case SET_HT:
                return "hashtable";
            case SET_INTSET:
                return "intset";
            case LIST_QUICKLIST:
                return "quicklist";
            case ZSET_SKIPLIST:
                return "skiplist";
            default:
                return encoding.name().toLowerCase(java.util.Locale.ROOT);
        }
    }
}
