package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesSink;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.StringReadOps;
import yier.bubu.redis.ops.StringWriteOps;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.result.BulkStringValue;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.Arrays;
import java.util.Objects;

final class YierdisStringOps implements StringReadOps, StringWriteOps {
    private static final long TTL_ENTRY_BYTES_ESTIMATE = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;

    private final YierdisDbInternals internals;

    YierdisStringOps(YierdisDbInternals internals) {
        this.internals = Objects.requireNonNull(internals, "internals");
    }

    @Override
    public SetStringResult set(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption, boolean returnOldValue) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
        Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, value == null ? 0 : value.len());
        if (expireAtMillis != null) {
            upperBound += TTL_ENTRY_BYTES_ESTIMATE;
        }
        final long finalUpperBound = upperBound;

        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return finalUpperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<SetStringResult> apply() {
                final boolean[] didSet = new boolean[]{false};
                final boolean[] existed = new boolean[]{false};
                final KeyHandle[] handleRef = new KeyHandle[]{null};
                final long[] deltaBytes = new long[]{0};
                final byte[][] oldValue = new byte[1][];

                internals.store().computeWithHandle(keyBytes, (k, old) -> {
                    handleRef[0] = k;
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    existed[0] = old != null;
                    if (mode == SetMode.NX && old != null) {
                        internals.touch(old);
                        return old;
                    }
                    if (mode == SetMode.XX && old == null) {
                        return null;
                    }
                    if (returnOldValue && old != null && old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    if (returnOldValue && old != null) {
                        byte[] raw = old.stringBytesView();
                        oldValue[0] = raw == null ? null : Arrays.copyOf(raw, raw.length);
                    }
                    if (old == null) {
                        didSet[0] = true;
                        YierdisObject next = YierdisObject.newString(internals.offHeapAllocator(), value);
                        internals.touch(next);
                        internals.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    old.overwriteWithString(internals.offHeapAllocator(), value);
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    didSet[0] = true;
                    return old;
                });

                if (didSet[0]) {
                    YierdisChangeTracking.markValueChanged();
                    if (keepTtl && existed[0]) {
                        return YierdisDbMutationExecutor.MutationResult.of(
                                SetStringResult.of(true, oldValue[0]),
                                deltaBytes[0]
                        );
                    }
                    if (expireAtMillis != null) {
                        internals.setExpireAtMillis(handleRef[0], expireAtMillis);
                        YierdisChangeTracking.markTtlChanged();
                        return YierdisDbMutationExecutor.MutationResult.of(
                                SetStringResult.of(true, oldValue[0]),
                                deltaBytes[0]
                        );
                    }
                    Long beforeTtl = internals.expires().get(handleRef[0]);
                    internals.removeExpire(handleRef[0]);
                    if (beforeTtl != null) {
                        YierdisChangeTracking.markTtlChanged();
                    }
                }
                return YierdisDbMutationExecutor.MutationResult.of(
                        SetStringResult.of(didSet[0], oldValue[0]),
                        deltaBytes[0]
                );
            }
        });
    }

    @Override
    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        return set(keyBytes, sliceOf(value), mode, expireOption, false).applied();
    }

    @Override
    public boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
        return set(keyBytes, value, mode, expireOption, false).applied();
    }

    @Override
    public long append(byte[] keyBytes, BytesSlice value) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, value == null ? 0 : value.len());
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Long> apply() {
                final int[] newLen = new int[]{0};
                final boolean[] changed = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                internals.store().computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    if (old == null) {
                        YierdisObject created = YierdisObject.newString(internals.offHeapAllocator(), value);
                        newLen[0] = created.stringByteLength();
                        changed[0] = true;
                        internals.touch(created);
                        internals.refreshEstimatedBytes(k, created);
                        deltaBytes[0] += created.estimatedBytes;
                        return created;
                    }

                    if (old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    internals.touch(old);
                    int beforeLen = old.stringByteLength();
                    newLen[0] = old.stringAppend(internals.offHeapAllocator(), value);
                    if (newLen[0] != beforeLen) {
                        changed[0] = true;
                    }
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changed[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of((long) newLen[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public int setBit(byte[] keyBytes, long offset, int value) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long currentLen = stringLength(keyBytes);
        long requiredBytes = (offset >>> 3) + 1;
        long growth = Math.max(0L, requiredBytes - currentLen);
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, (int) growth);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Integer> apply() {
                final int[] oldBit = new int[]{0};
                final boolean[] changed = new boolean[]{false};
                final long[] deltaBytes = new long[]{0};
                internals.store().computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        old = YierdisObject.newString(internals.offHeapAllocator(), (byte[]) null);
                        internals.touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        internals.touch(old);
                    }

                    int beforeLen = old.stringByteLength();
                    boolean existed = oldEstimate > 0;
                    oldBit[0] = old.stringSetBit(internals.offHeapAllocator(), offset, value);
                    int afterLen = old.stringByteLength();
                    if (!existed || oldBit[0] != value || afterLen != beforeLen) {
                        changed[0] = true;
                    }
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
                if (changed[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(oldBit[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public long incrBy(byte[] keyBytes, long delta) {
        internals.checkThread();
        long now = System.currentTimeMillis();
        long upperBound = estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, 32);
        return internals.executeMutation(new YierdisDbMutationExecutor.MutationPlan<>() {
            @Override
            public long upperBoundBytes() {
                return upperBound;
            }

            @Override
            public YierdisDbMutationExecutor.MutationResult<Long> apply() {
                final long[] result = new long[]{0L};
                final long[] deltaBytes = new long[]{0L};
                final boolean[] changed = new boolean[]{false};
                internals.store().computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && internals.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        internals.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    if (old == null) {
                        result[0] = delta;
                        YierdisObject created = YierdisObject.newStringInt(delta);
                        internals.touch(created);
                        internals.refreshEstimatedBytes(k, created);
                        deltaBytes[0] += created.estimatedBytes;
                        changed[0] = true;
                        return created;
                    }

                    if (old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    result[0] = old.stringIncrBy(internals.offHeapAllocator(), delta);
                    internals.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    internals.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    changed[0] = true;
                    return old;
                });
                if (changed[0]) {
                    YierdisChangeTracking.markValueChanged();
                }
                return YierdisDbMutationExecutor.MutationResult.of(result[0], deltaBytes[0]);
            }
        });
    }

    @Override
    public byte[] getStringBytes(byte[] keyBytes) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return null;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return object.stringBytesView();
    }

    @Override
    public BulkStringValue getStringValue(BytesView keyView) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyView);
        if (object == null) {
            return BulkStringValue.nullValue();
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        if (object.encoding == ValueEncoding.STRING_INT) {
            return BulkStringValue.longAscii(object.intValue);
        }
        OffHeapSlice slice = object.stringOffHeapSlice();
        if (slice != null) {
            return BulkStringValue.slice(slice);
        }
        return BulkStringValue.bytes((byte[]) object.payload, 0, object.rawLen);
    }

    @Override
    public long strlen(BytesView keyView) {
        return stringLength(keyView);
    }

    @Override
    public int getBit(BytesView keyView, long offset) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyView);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return object.stringGetBit(offset);
    }

    @Override
    public long bitcount(BytesView keyView) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyView);
        if (object == null) {
            return 0L;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        int len = object.stringByteLength();
        if (len <= 0) {
            return 0L;
        }
        return bitcountRange(object, 0, len - 1);
    }

    @Override
    public long bitcount(BytesView keyView, long start, long end) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyView);
        if (object == null) {
            return 0L;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        int len = object.stringByteLength();
        if (len <= 0) {
            return 0L;
        }

        long s = start;
        long ed = end;
        if (s < 0) {
            s = len + s;
        }
        if (ed < 0) {
            ed = len + ed;
        }
        if (s < 0) {
            s = 0;
        }
        if (ed < 0) {
            return 0L;
        }
        if (s >= len) {
            return 0L;
        }
        if (ed >= len) {
            ed = len - 1L;
        }
        if (s > ed) {
            return 0L;
        }
        return bitcountRange(object, (int) s, (int) ed);
    }

    private int stringLength(byte[] keyBytes) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyBytes);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return object.stringByteLength();
    }

    private int stringLength(BytesView keyView) {
        internals.checkThread();
        YierdisObject object = internals.getObjectIfNotExpired(keyView);
        if (object == null) {
            return 0;
        }
        if (object.type != ValueType.STRING) {
            throw new WrongTypeException();
        }
        return object.stringByteLength();
    }

    private static long bitcountRange(YierdisObject object, int start, int end) {
        if (start < 0 || end < start) {
            return 0L;
        }
        long count = 0L;
        if (object.encoding == ValueEncoding.STRING_INT) {
            byte[] view = object.stringBytesView();
            int to = Math.min(end, view.length - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(view[i] & 0xFF);
            }
            return count;
        }

        if (object.payload instanceof byte[] buf) {
            int to = Math.min(end, object.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(buf[i] & 0xFF);
            }
            return count;
        }
        if (object.payload instanceof OffHeapBuf buf) {
            int to = Math.min(end, object.rawLen - 1);
            for (int i = start; i <= to; i++) {
                count += Integer.bitCount(buf.getByte(i) & 0xFF);
            }
            return count;
        }
        return 0L;
    }

    private static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength) + Math.max(0, valueLength) + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    private static BytesSlice sliceOf(byte[] value) {
        if (value == null) {
            return null;
        }
        return new BytesSlice() {
            @Override
            public void writeTo(BytesSink out) {
                out.writeBytes(value, 0, value.length);
            }

            @Override
            public int length() {
                return value.length;
            }

            @Override
            public byte getByte(int index) {
                return value[index];
            }
        };
    }
}
