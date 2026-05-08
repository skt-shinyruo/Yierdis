package yier.bubu.redis.storage.memory.internal.ffm;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.memory.foreign.YierdisFfmAccess;
import yier.bubu.redis.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.memory.foreign.YierdisFfmRegion;
import yier.bubu.redis.memory.foreign.YierdisFfmSpan;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;

public final class YierdisFfmBlobStore {
    private final YierdisFfmMemoryRuntime runtime;
    private final String ownerPrefix;
    private final Map<YierdisFfmBytesRef, Integer> refCounts = new IdentityHashMap<>();

    private long liveBytes;

    public YierdisFfmBlobStore(YierdisFfmMemoryRuntime runtime, String ownerPrefix) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.ownerPrefix = Objects.requireNonNull(ownerPrefix, "ownerPrefix");
    }

    public YierdisFfmMemoryRuntime memoryRuntime() {
        return runtime;
    }

    public static YierdisFfmBytesRef fromBytes(YierdisFfmMemoryRuntime runtime, byte[] bytes) {
        Objects.requireNonNull(runtime, "runtime");
        Objects.requireNonNull(bytes, "bytes");
        int len = bytes.length;
        YierdisFfmRegion region = runtime.allocateRegion("blob", Math.max(1, len));
        if (len > 0) {
            YierdisFfmAccess.setBytes(region.span(0, len), 0, bytes, 0, len);
        }
        return new YierdisFfmBytesRef(region, 0, len);
    }

    public YierdisFfmBytesRef store(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes");
        int len = bytes.length;
        YierdisFfmRegion region = runtime.allocateRegion(ownerPrefix, Math.max(1, len));
        if (len > 0) {
            YierdisFfmAccess.setBytes(region.span(0, len), 0, bytes, 0, len);
        }
        YierdisFfmBytesRef ref = new YierdisFfmBytesRef(region, 0, len);
        refCounts.put(ref, 1);
        liveBytes += ref.region().size();
        return ref;
    }

    public void retain(YierdisFfmBytesRef ref) {
        Objects.requireNonNull(ref, "ref");
        Integer count = refCounts.get(ref);
        if (count == null) {
            refCounts.put(ref, 1);
            liveBytes += ref.region().size();
            return;
        }
        refCounts.put(ref, count + 1);
    }

    public void release(YierdisFfmBytesRef ref) {
        Objects.requireNonNull(ref, "ref");
        Integer count = refCounts.get(ref);
        if (count == null) {
            throw new IllegalStateException("unknown blob ref");
        }
        if (count > 1) {
            refCounts.put(ref, count - 1);
            return;
        }
        refCounts.remove(ref);
        liveBytes -= ref.region().size();
        ref.region().close();
    }

    public long liveBytes() {
        return liveBytes;
    }

    public byte[] toByteArray(YierdisFfmBytesRef ref) {
        Objects.requireNonNull(ref, "ref");
        int len = ref.length();
        if (len == 0) {
            return new byte[0];
        }
        byte[] out = new byte[len];
        YierdisFfmAccess.getBytes(ref.span(), 0, out, 0, len);
        return out;
    }

    public boolean equalsBytes(YierdisFfmBytesRef ref, byte[] keyBytes) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(keyBytes, "keyBytes");
        int len = ref.length();
        if (len != keyBytes.length) {
            return false;
        }
        YierdisFfmSpan span = ref.span();
        for (int i = 0; i < len; i++) {
            if (YierdisFfmAccess.getByte(span, i) != keyBytes[i]) {
                return false;
            }
        }
        return true;
    }

    public boolean equalsBytes(YierdisFfmBytesRef ref, BytesView keyView) {
        Objects.requireNonNull(ref, "ref");
        Objects.requireNonNull(keyView, "keyView");
        int len = keyView.length();
        if (ref.length() != len) {
            return false;
        }
        YierdisFfmSpan span = ref.span();
        for (int i = 0; i < len; i++) {
            if (YierdisFfmAccess.getByte(span, i) != keyView.getByte(i)) {
                return false;
            }
        }
        return true;
    }
}
