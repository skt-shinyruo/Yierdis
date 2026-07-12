package yier.bubu.redis.storage.memory.internal.value;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import yier.bubu.redis.storage.api.result.BulkStringSink;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.ValueHandle;

public final class PreparedPoppedValueSequence implements PoppedValueSequence {
    private static final PreparedPoppedValueSequence NULL_VALUE = new PreparedPoppedValueSequence(
            null,
            null,
            0,
            true,
            true,
            0L,
            0L
    );
    private static final PreparedPoppedValueSequence EMPTY_VALUE = new PreparedPoppedValueSequence(
            null,
            null,
            0,
            true,
            false,
            0L,
            0L
    );

    private final ListRoot listRoot;
    private final ValueHandle ownerHandle;
    private final int count;
    private final boolean left;
    private final boolean nullValue;
    private final long encodedElementBytes;
    private final long retainedMemoryBytes;
    private final AtomicBoolean closed = new AtomicBoolean();

    private PreparedPoppedValueSequence(
            ListRoot listRoot,
            ValueHandle ownerHandle,
            int count,
            boolean left,
            boolean nullValue,
            long encodedElementBytes,
            long retainedMemoryBytes
    ) {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        if (encodedElementBytes < 0L) {
            throw new IllegalArgumentException("encodedElementBytes must be >= 0");
        }
        if (retainedMemoryBytes < 0L) {
            throw new IllegalArgumentException("retainedMemoryBytes must be >= 0");
        }
        this.listRoot = listRoot;
        this.ownerHandle = ownerHandle;
        this.count = count;
        this.left = left;
        this.nullValue = nullValue;
        this.encodedElementBytes = encodedElementBytes;
        this.retainedMemoryBytes = retainedMemoryBytes;
    }

    public static PreparedPoppedValueSequence nullValue() {
        return NULL_VALUE;
    }

    public static PreparedPoppedValueSequence empty() {
        return EMPTY_VALUE;
    }

    public static PreparedPoppedValueSequence owned(
            ListRoot listRoot,
            ValueHandle ownerHandle,
            int count,
            boolean left
    ) {
        Objects.requireNonNull(listRoot, "listRoot");
        Objects.requireNonNull(ownerHandle, "ownerHandle");
        return new PreparedPoppedValueSequence(
                listRoot,
                ownerHandle,
                count,
                left,
                false,
                listRoot.encodedPopElementBytes(ownerHandle, count, left),
                listRoot.retainedBytes(ownerHandle)
        );
    }

    @Override
    public boolean isNull() {
        return nullValue;
    }

    @Override
    public int count() {
        return count;
    }

    @Override
    public long encodedElementBytes() {
        return encodedElementBytes;
    }

    @Override
    public long retainedMemoryBytes() {
        return retainedMemoryBytes;
    }

    @Override
    public void emitTo(BulkStringSink out) {
        Objects.requireNonNull(out, "out");
        if (ownerHandle == null || count == 0) {
            return;
        }
        listRoot.emitPopRange(ownerHandle, count, left, out);
    }

    @Override
    public void close() {
        if (ownerHandle == null || !closed.compareAndSet(false, true)) {
            return;
        }
        listRoot.release(ownerHandle);
    }
}
