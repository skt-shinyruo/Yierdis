package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public record EntryHandle(long raw) {
    public EntryHandle {
        requireEntryHandle(raw);
    }

    public static EntryHandle fromNativeHandle(NativeHandle handle) {
        if (handle == null) {
            throw new NullPointerException("handle");
        }
        requireEntryHandle(handle.raw());
        return new EntryHandle(handle.raw());
    }

    public static EntryHandle fromRaw(long raw) {
        return new EntryHandle(raw);
    }

    public NativeHandle nativeHandle() {
        return NativeHandle.fromRaw(raw);
    }

    private static void requireEntryHandle(long raw) {
        NativeHandle.requireValidRaw(raw);
        if (NativeHandle.isNull(raw)
                || NativeHandle.domainCode(raw) != NativeObjectKind.ENTRY_RECORD.domain().code()
                || NativeHandle.kindCode(raw) != NativeObjectKind.ENTRY_RECORD.code()) {
            throw new IllegalArgumentException("entry handle must wrap ENTRY_RECORD native handle");
        }
    }
}
