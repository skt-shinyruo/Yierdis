package yier.bubu.redis.storage.memory.internal.key;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.storage.memory.internal.ffm.YierdisFfmBytesRef;

import java.util.Objects;

public final class FfmKeyHandle implements KeyHandle {
    private final YierdisFfmBytesRef ref;
    private final int dictHash;
    private final int contentHash;

    public FfmKeyHandle(YierdisFfmBytesRef ref, int dictHash) {
        this.ref = Objects.requireNonNull(ref, "ref");
        this.dictHash = dictHash;
        this.contentHash = KeyHandle.hashBytesView(this, ref.length());
    }

    @Override
    public int dictHash() {
        return dictHash;
    }

    @Override
    public int len() {
        return ref.length();
    }

    @Override
    public byte byteAt(int index) {
        if (index < 0 || index >= ref.length()) {
            throw new IndexOutOfBoundsException("index=" + index + ", len=" + ref.length());
        }
        return ref.byteAt(index);
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
        int len = ref.length();
        if (other.len() != len) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            if (byteAt(i) != other.byteAt(i)) {
                return false;
            }
        }
        return true;
    }

    public YierdisFfmBytesRef refUnsafe() {
        return ref;
    }
}
