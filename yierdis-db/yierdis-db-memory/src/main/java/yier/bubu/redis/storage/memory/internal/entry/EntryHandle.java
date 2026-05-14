package yier.bubu.redis.storage.memory.internal.entry;

import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectKind;

public record EntryHandle(long raw) {
    public EntryHandle {
        requireEntryHandle(NativeHandle.fromRaw(raw));
    }

    public static EntryHandle fromNativeHandle(NativeHandle handle) {
        requireEntryHandle(handle);
        return new EntryHandle(handle.raw());
    }

    public static EntryHandle fromRaw(long raw) {
        return fromNativeHandle(NativeHandle.fromRaw(raw));
    }

    public NativeHandle nativeHandle() {
        return NativeHandle.fromRaw(raw);
    }

    private static void requireEntryHandle(NativeHandle handle) {
        if (handle == null) {
            throw new NullPointerException("handle");
        }
        if (handle.isNull()
                || handle.domain() != NativeObjectKind.ENTRY_RECORD.domain()
                || handle.kindCode() != NativeObjectKind.ENTRY_RECORD.code()) {
            throw new IllegalArgumentException("entry handle must wrap ENTRY_RECORD native handle");
        }
    }
}
