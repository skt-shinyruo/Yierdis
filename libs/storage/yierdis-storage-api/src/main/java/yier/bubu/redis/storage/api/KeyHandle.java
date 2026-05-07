package yier.bubu.redis.storage.api;

import yier.bubu.redis.bytes.BytesView;

/**
 * Opaque storage key identity used by maxmemory and storage-pressure APIs.
 * <p>
 * The handle is intentionally only a read-only byte view plus a storage-local dictionary hash. Implementations may
 * point to heap or off-heap storage, so callers must treat handles as short-lived identities owned by the participant
 * that produced them.
 */
public interface KeyHandle extends BytesView {
    /**
     * Length of the key in bytes.
     */
    int len();

    /**
     * Byte at {@code index}.
     */
    byte byteAt(int index);

    @Override
    default int length() {
        return len();
    }

    @Override
    default byte getByte(int index) {
        return byteAt(index);
    }

    /**
     * Storage-local dictionary hash. It is stable only inside the owning keyspace.
     */
    int dictHash();
}
