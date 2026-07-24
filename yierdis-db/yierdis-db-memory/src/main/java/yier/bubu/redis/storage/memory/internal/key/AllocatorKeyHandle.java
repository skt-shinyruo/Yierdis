package yier.bubu.redis.storage.memory.internal.key;

import yier.bubu.redis.memory.api.NativeAccessMode;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.NativeObjectView;

import java.util.Objects;

public final class AllocatorKeyHandle implements KeyHandle {
    private final StableMemoryBackend allocator;
    private final NativeHandle handle;
    private final int dictHash;
    private final int contentHash;

    public AllocatorKeyHandle(StableMemoryBackend allocator, NativeHandle handle, int dictHash) {
        this.allocator = Objects.requireNonNull(allocator, "allocator");
        this.handle = Objects.requireNonNull(handle, "handle");
        this.dictHash = dictHash;
        this.contentHash = computeContentHash();
    }

    @Override
    public int dictHash() {
        return dictHash;
    }

    @Override
    public int length() {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            return view.size();
        }
    }

    @Override
    public byte getByte(int index) {
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            if (index < 0 || index >= view.size()) {
                throw new IndexOutOfBoundsException("index=" + index + ", len=" + view.size());
            }
            return view.getByte(index);
        }
    }

    @Override
    public int hashCode() {
        return contentHash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof KeyHandle other)) {
            return false;
        }
        if (other instanceof AllocatorKeyHandle allocatorOther) {
            if (handle.equals(allocatorOther.handle)) {
                return true;
            }
            if (contentHash != allocatorOther.contentHash) {
                return false;
            }
        }
        int len = length();
        if (other.length() != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (getByte(i) != other.getByte(i)) {
                return false;
            }
        }
        return true;
    }

    NativeHandle nativeHandle() {
        return handle;
    }

    private int computeContentHash() {
        int h = 1;
        try (NativeObjectView view = allocator.resolve(handle, NativeAccessMode.READ_ONLY)) {
            for (int i = 0; i < view.size(); i++) {
                h = 31 * h + view.getByte(i);
            }
        }
        return h;
    }
}
