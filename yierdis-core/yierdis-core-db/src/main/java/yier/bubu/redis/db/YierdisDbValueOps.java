package yier.bubu.redis.db;

// YierdisDbValueOps：将 YierdisDb 的 value 操作按类型拆分为 ops 边界（String/Hash/List/Set/ZSet/HLL）。

import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.offheap.api.OffHeapAddressAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.offheap.api.OffHeapOutOfMemoryException;
import yier.bubu.redis.offheap.api.OffHeapSlice;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.HashOps;
import yier.bubu.redis.ops.HllOps;
import yier.bubu.redis.ops.ListOps;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.SetMode;
import yier.bubu.redis.ops.SetOps;
import yier.bubu.redis.ops.StringOps;
import yier.bubu.redis.ops.ValueOps;
import yier.bubu.redis.ops.WrongTypeException;
import yier.bubu.redis.ops.YierdisCommandException;
import yier.bubu.redis.ops.ZSetOps;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.bytes.BytesSlice;
import yier.bubu.redis.ops.result.BulkStringMapPairs;
import yier.bubu.redis.ops.result.BulkStringMapPairsSupport;
import yier.bubu.redis.ops.result.BulkStringSequence;
import yier.bubu.redis.ops.result.BulkStringSequences;
import yier.bubu.redis.ops.result.BulkStringSink;
import yier.bubu.redis.ops.result.BulkStringValue;
import yier.bubu.redis.runtime.api.YierdisChangeTracking;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class YierdisDbValueOps implements ValueOps {
    private final YierdisDb db;
    private final StringOps strings;
    private final HashOps hashes;
    private final ListOps lists;
    private final SetOps sets;
    private final ZSetOps zsets;
    private final HllOps hll;

    YierdisDbValueOps(YierdisDb db) {
        this.db = Objects.requireNonNull(db, "db");
        this.strings = new Strings(db);
        this.hashes = new Hashes(db);
        this.lists = new Lists(db);
        this.sets = new Sets(db);
        this.zsets = new ZSets(db);
        this.hll = new Hll(db);
    }

    @Override
    public StringOps strings() {
        return strings;
    }

    @Override
    public HashOps hashes() {
        return hashes;
    }

    @Override
    public ListOps lists() {
        return lists;
    }

    @Override
    public SetOps sets() {
        return sets;
    }

    @Override
    public ZSetOps zsets() {
        return zsets;
    }

    @Override
    public HllOps hll() {
        return hll;
    }

    private static final class Strings implements StringOps {
        private final YierdisDb db;

        private Strings(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
            db.checkThread();
            long now = System.currentTimeMillis();
            boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
            Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);

            final boolean[] didSet = new boolean[]{false};
            final boolean[] existed = new boolean[]{false};
            final KeyHandle[] handleRef = new KeyHandle[]{null};
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(keyBytes, (k, old) -> {
                    handleRef[0] = k;
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    existed[0] = old != null;
                    if (mode == SetMode.NX && old != null) {
                        db.touch(old);
                        return old;
                    }
                    if (mode == SetMode.XX && old == null) {
                        return null;
                    }
                    if (old == null) {
                        didSet[0] = true;
                        YierdisObject next = YierdisObject.newString(db.offHeapAllocator, value);
                        db.touch(next);
                        db.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    old.overwriteWithString(db.offHeapAllocator, value);
                    db.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    didSet[0] = true;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            if (didSet[0]) {
                YierdisChangeTracking.markValueChanged();
                if (keepTtl && existed[0]) {
                    // KEEPTTL：覆盖写入但保留原有过期时间（仅当 key 原先存在时有意义）。
                    return true;
                }
                if (expireAtMillis != null) {
                    db.setExpireAtMillis(handleRef[0], expireAtMillis);
                    YierdisChangeTracking.markTtlChanged();
                    return true;
                }
                Long beforeTtl = db.expires.get(handleRef[0]);
                db.removeExpire(handleRef[0]);
                if (beforeTtl != null) {
                    YierdisChangeTracking.markTtlChanged();
                }
            }
            return didSet[0];
        }

        @Override
        public boolean setString(byte[] keyBytes, BytesSlice value, SetMode mode, ExpireOption expireOption) {
            db.checkThread();
            long now = System.currentTimeMillis();
            boolean keepTtl = expireOption != null && expireOption.isKeepTtl();
            Long expireAtMillis = (expireOption == null || keepTtl) ? null : expireOption.toExpireAtMillis(now);

            final boolean[] didSet = new boolean[]{false};
            final boolean[] existed = new boolean[]{false};
            final KeyHandle[] handleRef = new KeyHandle[]{null};
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(keyBytes, (k, old) -> {
                    handleRef[0] = k;
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    existed[0] = old != null;
                    if (mode == SetMode.NX && old != null) {
                        db.touch(old);
                        return old;
                    }
                    if (mode == SetMode.XX && old == null) {
                        return null;
                    }
                    if (old == null) {
                        didSet[0] = true;
                        YierdisObject next = YierdisObject.newString(db.offHeapAllocator, value);
                        db.touch(next);
                        db.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }
                    old.overwriteWithString(db.offHeapAllocator, value);
                    db.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    didSet[0] = true;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            if (didSet[0]) {
                YierdisChangeTracking.markValueChanged();
                if (keepTtl && existed[0]) {
                    // KEEPTTL：覆盖写入但保留原有过期时间（仅当 key 原先存在时有意义）。
                    return true;
                }
                if (expireAtMillis != null) {
                    db.setExpireAtMillis(handleRef[0], expireAtMillis);
                    YierdisChangeTracking.markTtlChanged();
                    return true;
                }
                Long beforeTtl = db.expires.get(handleRef[0]);
                db.removeExpire(handleRef[0]);
                if (beforeTtl != null) {
                    YierdisChangeTracking.markTtlChanged();
                }
            }
            return didSet[0];
        }

        @Override
        public byte[] getStringBytes(byte[] keyBytes) {
            return db.getStringBytes(keyBytes);
        }

        @Override
        public BulkStringValue getStringValue(BytesView keyView) {
            db.checkThread();

            YierdisObject e = db.getObjectIfNotExpired(keyView);
            if (e == null) {
                return BulkStringValue.nullValue();
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }

            if (e.encoding == ValueEncoding.STRING_INT) {
                return BulkStringValue.longAscii(e.intValue);
            }
            OffHeapSlice slice = e.stringOffHeapSlice();
            if (slice != null) {
                return BulkStringValue.slice(slice);
            }
            byte[] buf = (byte[]) e.payload;
            return BulkStringValue.bytes(buf, 0, e.rawLen);
        }

        @Override
        public long strlen(BytesView keyView) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyView);
            if (e == null) {
                return 0;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            return e.stringByteLength();
        }

        @Override
        public long append(byte[] keyBytes, BytesSlice value) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] newLen = new int[]{0};
            final boolean[] changed = new boolean[]{false};
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }
                    if (old == null) {
                        YierdisObject o = YierdisObject.newString(db.offHeapAllocator, value);
                        newLen[0] = o.stringByteLength();
                        changed[0] = true;
                        db.touch(o);
                        db.refreshEstimatedBytes(k, o);
                        deltaBytes[0] += o.estimatedBytes;
                        return o;
                    }

                    if (old.type != ValueType.STRING) {
                        throw new WrongTypeException();
                    }
                    db.touch(old);
                    int beforeLen = old.stringByteLength();
                    newLen[0] = old.stringAppend(db.offHeapAllocator, value);
                    if (newLen[0] != beforeLen) {
                        changed[0] = true;
                    }
                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            if (changed[0]) {
                YierdisChangeTracking.markValueChanged();
            }
            return newLen[0];
        }

        @Override
        public int setBit(byte[] keyBytes, long offset, int value) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] oldBit = new int[]{0};
            final boolean[] changed = new boolean[]{false};
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    boolean existed = old != null;
                    if (old == null) {
                        old = YierdisObject.newString(db.offHeapAllocator, (byte[]) null);
                        db.touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        db.touch(old);
                    }

                    int beforeLen = old.stringByteLength();
                    oldBit[0] = old.stringSetBit(db.offHeapAllocator, offset, value);
                    int afterLen = old.stringByteLength();
                    if (!existed || oldBit[0] != value || afterLen != beforeLen) {
                        changed[0] = true;
                    }
                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            if (changed[0]) {
                YierdisChangeTracking.markValueChanged();
            }
            return oldBit[0];
        }

        @Override
        public int getBit(BytesView keyView, long offset) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyView);
            if (e == null) {
                return 0;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            return e.stringGetBit(offset);
        }

        @Override
        public long bitcount(BytesView keyView) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyView);
            if (e == null) {
                return 0L;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }

            int len = e.stringByteLength();
            if (len <= 0) {
                return 0L;
            }
            return bitcountRange(e, 0, len - 1);
        }

        @Override
        public long bitcount(BytesView keyView, long start, long end) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyView);
            if (e == null) {
                return 0L;
            }
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            int len = e.stringByteLength();
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
            return bitcountRange(e, (int) s, (int) ed);
        }

        @Override
        public long incrBy(byte[] keyBytes, long delta) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final long[] result = new long[]{0L};
            final long[] deltaBytes = new long[]{0};
            db.store.computeWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    long next = delta;
                    result[0] = next;
                    YierdisObject o = YierdisObject.newStringInt(next);
                    db.touch(o);
                    db.refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }

                if (old.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                db.touch(old);
                result[0] = old.stringIncrBy(db.offHeapAllocator, delta);
                deltaBytes[0] -= oldEstimate;
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (delta != 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return result[0];
        }

        private static long bitcountRange(YierdisObject e, int start, int end) {
            if (start < 0 || end < start) {
                return 0L;
            }
            long count = 0L;
            if (e.encoding == ValueEncoding.STRING_INT) {
                byte[] view = e.stringBytesView();
                int to = Math.min(end, view.length - 1);
                for (int i = start; i <= to; i++) {
                    count += Integer.bitCount(view[i] & 0xFF);
                }
                return count;
            }

            if (e.payload instanceof byte[] buf) {
                int to = Math.min(end, e.rawLen - 1);
                for (int i = start; i <= to; i++) {
                    count += Integer.bitCount(buf[i] & 0xFF);
                }
                return count;
            }
            if (e.payload instanceof OffHeapBuf buf) {
                int to = Math.min(end, e.rawLen - 1);
                for (int i = start; i <= to; i++) {
                    count += Integer.bitCount(buf.getByte(i) & 0xFF);
                }
                return count;
            }
            if (e.payload instanceof yier.bubu.redis.db.offheap.YierdisUnsafeOffHeapString s) {
                int to = Math.min(end, e.rawLen - 1);
                for (int i = start; i <= to; i++) {
                    count += Integer.bitCount(s.getByte(i) & 0xFF);
                }
                return count;
            }
            return 0L;
        }
    }

    private static final class Hashes implements HashOps {
        private final YierdisDb db;

        private Hashes(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
            db.checkThread();
            if (fieldValuePairs.size() % 2 != 0) {
                throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
            }
            long now = System.currentTimeMillis();
            OffHeapAddressAllocator addressAllocator =
                    db.offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
            final int[] added = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    HashValue hv = addressAllocator != null ? new HashValue(addressAllocator) : new HashValue();
                    added[0] = hv.hsetMany(fieldValuePairs);
                    YierdisObject o = YierdisObject.newHash(hv);
                    db.touch(o);
                    db.refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }
                if (old.type != ValueType.HASH) {
                    throw new WrongTypeException();
                }
                added[0] = ((HashValue) old.payload).hsetMany(fieldValuePairs);
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                deltaBytes[0] -= oldEstimate;
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            YierdisChangeTracking.markValueChanged();
            return added[0];
        }

        @Override
        public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return null;
            }
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.payload).hget(fieldBytes);
        }

        @Override
        public BulkStringMapPairs hgetall(byte[] keyBytes) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringMapPairsSupport.empty();
            }
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            HashValue hv = (HashValue) e.payload;
            int pairs = hv.size();
            if (pairs == 0) {
                return BulkStringMapPairsSupport.empty();
            }
            return new BulkStringMapPairs() {
                @Override
                public int pairCount() {
                    return pairs;
                }

                @Override
                public void emitPairsTo(BulkStringSink out) {
                    hv.hgetallPairsInto(out);
                }
            };
        }

        @Override
        public long hlen(byte[] keyBytes) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return 0;
            }
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.payload).size();
        }

        @Override
        public long hdel(byte[] keyBytes, List<byte[]> fields) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] removed = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.HASH) {
                    throw new WrongTypeException();
                }
                HashValue hv = (HashValue) old.payload;
                removed[0] = hv.hdel(fields);
                if (hv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (removed[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return removed[0];
        }
    }

    private static final class Lists implements ListOps {
        private final YierdisDb db;

        private Lists(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long lpush(byte[] keyBytes, List<byte[]> values) {
            db.checkThread();
            return pushInternal(keyBytes, values, true);
        }

        @Override
        public long rpush(byte[] keyBytes, List<byte[]> values) {
            db.checkThread();
            return pushInternal(keyBytes, values, false);
        }

        @Override
        public BulkStringSequence lrange(byte[] keyBytes, int start, int stop) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.LIST) {
                throw new WrongTypeException();
            }
            ListValue lv = (ListValue) e.payload;
            int count = lv.rangeCount(start, stop);
            if (count == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return count;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    lv.rangeInto(start, stop, out);
                }
            };
        }

        @Override
        public List<byte[]> lpop(byte[] keyBytes, int count) {
            db.checkThread();
            return popInternal(keyBytes, count, true);
        }

        @Override
        public List<byte[]> rpop(byte[] keyBytes, int count) {
            db.checkThread();
            return popInternal(keyBytes, count, false);
        }

        private long pushInternal(byte[] keyBytes, List<byte[]> values, boolean left) {
            long now = System.currentTimeMillis();
            OffHeapAddressAllocator addressAllocator =
                    db.offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
            final int[] len = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    ListValue lv = addressAllocator != null ? new ListValue(addressAllocator) : new ListValue();
                    if (left) {
                        lv.lpushAll(values);
                    } else {
                        lv.rpushAll(values);
                    }
                    len[0] = lv.size();
                    YierdisObject o = YierdisObject.newList(lv);
                    db.touch(o);
                    db.refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }

                if (old.type != ValueType.LIST) {
                    throw new WrongTypeException();
                }
                ListValue lv = (ListValue) old.payload;
                if (left) {
                    lv.lpushAll(values);
                } else {
                    lv.rpushAll(values);
                }
                len[0] = lv.size();
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                deltaBytes[0] -= oldEstimate;
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            YierdisChangeTracking.markValueChanged();
            return len[0];
        }

        private List<byte[]> popInternal(byte[] keyBytes, int count, boolean left) {
            if (count == 0) {
                return Collections.emptyList();
            }
            if (count < 0) {
                throw new IllegalArgumentException("count must be >= 0");
            }
            long now = System.currentTimeMillis();
            final List<byte[]>[] popped = new List[]{null};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.LIST) {
                    throw new WrongTypeException();
                }
                ListValue lv = (ListValue) old.payload;
                popped[0] = left ? lv.lpop(count) : lv.rpop(count);
                if (lv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (popped[0] != null && !popped[0].isEmpty()) {
                YierdisChangeTracking.markValueChanged();
            }
            return popped[0];
        }
    }

    private static final class Sets implements SetOps {
        private final YierdisDb db;

        private Sets(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long sadd(byte[] keyBytes, List<byte[]> members) {
            db.checkThread();
            long now = System.currentTimeMillis();
            OffHeapAddressAllocator addressAllocator =
                    db.offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
            final int[] added = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    SetValue sv = addressAllocator != null ? new SetValue(addressAllocator) : new SetValue();
                    added[0] = sv.addAll(members);
                    YierdisObject o = YierdisObject.newSet(sv);
                    db.touch(o);
                    db.refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }
                if (old.type != ValueType.SET) {
                    throw new WrongTypeException();
                }
                added[0] = ((SetValue) old.payload).addAll(members);
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                deltaBytes[0] -= oldEstimate;
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (added[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return added[0];
        }

        @Override
        public long srem(byte[] keyBytes, List<byte[]> members) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] removed = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.SET) {
                    throw new WrongTypeException();
                }
                SetValue sv = (SetValue) old.payload;
                removed[0] = sv.removeAll(members);
                if (sv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (removed[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return removed[0];
        }

        @Override
        public BulkStringSequence smembers(byte[] keyBytes) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            SetValue sv = (SetValue) e.payload;
            int count = sv.size();
            if (count == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return count;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    sv.membersInto(out);
                }
            };
        }

        @Override
        public boolean sismember(byte[] keyBytes, byte[] member) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return false;
            }
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.payload).contains(member);
        }

        @Override
        public long scard(byte[] keyBytes) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return 0;
            }
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.payload).size();
        }
    }

    private static final class ZSets implements ZSetOps {
        private final YierdisDb db;

        private ZSets(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public long zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
            db.checkThread();
            if (scoreMemberPairs.size() % 2 != 0) {
                throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
            }
            long now = System.currentTimeMillis();
            OffHeapAddressAllocator addressAllocator =
                    db.offHeapAllocator instanceof OffHeapAddressAllocator a ? a : null;
            final int[] added = new int[]{0};
            final boolean[] changedAny = new boolean[]{false};
            final long[] deltaBytes = new long[]{0};
            db.store.computeWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old == null ? 0 : old.estimatedBytes;
                if (old != null && db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    old = null;
                    oldEstimate = 0;
                }
                if (old == null) {
                    ZSetValue zv = addressAllocator != null ? new ZSetValue(addressAllocator) : new ZSetValue();
                    try {
                        added[0] = zv.zaddMany(scoreMemberPairs, changedAny);
                    } catch (RuntimeException e) {
                        zv.close();
                        throw e;
                    }
                    YierdisObject o = YierdisObject.newZSet(zv);
                    db.touch(o);
                    db.refreshEstimatedBytes(k, o);
                    deltaBytes[0] += o.estimatedBytes;
                    return o;
                }
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs, changedAny);
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                deltaBytes[0] -= oldEstimate;
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (changedAny[0]) {
                YierdisChangeTracking.markValueChanged();
            }
            return added[0];
        }

        @Override
        public BulkStringSequence zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) e.payload;
            int count = zv.zrangeCount(start, stop, withScores);
            if (count == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return count;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    zv.zrangeWriteTo(start, stop, withScores, out);
                }
            };
        }

        @Override
        public BulkStringSequence zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) e.payload;
            int count = zv.zrevrangeCount(start, stop, withScores);
            if (count == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return count;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    zv.zrevrangeWriteTo(start, stop, withScores, out);
                }
            };
        }

        @Override
        public BulkStringSequence zrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive,
                boolean withScores,
                long offset,
                long count
        ) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) e.payload;
            int replyCount = zv.zrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
            if (replyCount == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return replyCount;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    zv.zrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
                }
            };
        }

        @Override
        public BulkStringSequence zrevrangeByScore(
                byte[] keyBytes,
                double min,
                boolean minExclusive,
                double max,
                boolean maxExclusive,
                boolean withScores,
                long offset,
                long count
        ) {
            db.checkThread();
            YierdisObject e = db.getObjectIfNotExpired(keyBytes);
            if (e == null) {
                return BulkStringSequences.empty();
            }
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            ZSetValue zv = (ZSetValue) e.payload;
            int replyCount = zv.zrevrangeByScoreCount(min, minExclusive, max, maxExclusive, withScores, offset, count);
            if (replyCount == 0) {
                return BulkStringSequences.empty();
            }
            return new BulkStringSequence() {
                @Override
                public int count() {
                    return replyCount;
                }

                @Override
                public void emitTo(BulkStringSink out) {
                    zv.zrevrangeByScoreWriteTo(min, minExclusive, max, maxExclusive, withScores, offset, count, out);
                }
            };
        }

        @Override
        public long zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] removed = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zremrangeByScore(min, minExclusive, max, maxExclusive);
                if (zv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (removed[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return removed[0];
        }

        @Override
        public long zremrangeByRank(byte[] keyBytes, long start, long stop) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] removed = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zremrangeByRank(start, stop);
                if (zv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (removed[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return removed[0];
        }

        @Override
        public long zrem(byte[] keyBytes, List<byte[]> members) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final int[] removed = new int[]{0};
            final long[] deltaBytes = new long[]{0};
            db.store.computeIfPresentWithHandle(keyBytes, (k, old) -> {
                long oldEstimate = old.estimatedBytes;
                if (db.isKeyExpired(k, now)) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zrem(members);
                if (zv.size() == 0) {
                    old.releasePayloadIfAny();
                    db.removeExpire(k);
                    deltaBytes[0] -= oldEstimate;
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                db.touch(old);
                db.refreshEstimatedBytes(k, old);
                deltaBytes[0] += old.estimatedBytes - oldEstimate;
                return old;
            });
            db.commitWrite(deltaBytes[0]);
            if (removed[0] > 0) {
                YierdisChangeTracking.markValueChanged();
            }
            return removed[0];
        }
    }

    private static final class Hll implements HllOps {
        private final YierdisDb db;

        private Hll(YierdisDb db) {
            this.db = Objects.requireNonNull(db, "db");
        }

        @Override
        public int pfadd(byte[] keyBytes, List<byte[]> elements) {
            db.checkThread();
            long now = System.currentTimeMillis();
            final boolean[] changed = new boolean[]{false};
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(keyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        old = YierdisObject.newString(db.offHeapAllocator, YierdisHyperLogLog.newSparse());
                        db.touch(old);
                    } else {
                        if (old.type != ValueType.STRING) {
                            throw new WrongTypeException();
                        }
                        db.touch(old);
                    }

                    changed[0] = YierdisHyperLogLog.pfAdd(old, db.offHeapAllocator, elements);

                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            if (changed[0]) {
                YierdisChangeTracking.markValueChanged();
            }
            return changed[0] ? 1 : 0;
        }

        @Override
        public long pfcount(List<byte[]> keys) {
            db.checkThread();
            if (keys == null || keys.isEmpty()) {
                return 0L;
            }

            int[] registers = new int[YierdisHyperLogLog.REGISTERS];
            for (byte[] keyBytes : keys) {
                YierdisObject e = db.getObjectIfNotExpired(keyBytes);
                if (e == null) {
                    continue;
                }
                if (e.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                if (!YierdisHyperLogLog.isHllString(e)) {
                    throw new WrongTypeException();
                }
                YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
            }
            return YierdisHyperLogLog.estimateCardinality(registers);
        }

        @Override
        public void pfmerge(byte[] destKeyBytes, List<byte[]> sourceKeys) {
            db.checkThread();
            if (sourceKeys == null || sourceKeys.isEmpty()) {
                throw new IllegalArgumentException("sourceKeys must not be empty");
            }

            int[] registers = new int[YierdisHyperLogLog.REGISTERS];
            for (byte[] keyBytes : sourceKeys) {
                YierdisObject e = db.getObjectIfNotExpired(keyBytes);
                if (e == null) {
                    continue;
                }
                if (e.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                if (!YierdisHyperLogLog.isHllString(e)) {
                    throw new WrongTypeException();
                }
                YierdisHyperLogLog.mergeHllIntoRegisters(e.stringBytesView(), registers);
            }

            byte[] mergedDense = YierdisHyperLogLog.denseBytesFromRegisters(registers);
            long now = System.currentTimeMillis();
            final long[] deltaBytes = new long[]{0};
            try {
                db.store.computeWithHandle(destKeyBytes, (k, old) -> {
                    long oldEstimate = old == null ? 0 : old.estimatedBytes;
                    if (old != null && db.isKeyExpired(k, now)) {
                        old.releasePayloadIfAny();
                        db.removeExpire(k);
                        deltaBytes[0] -= oldEstimate;
                        old = null;
                        oldEstimate = 0;
                    }

                    if (old == null) {
                        YierdisObject next = YierdisObject.newString(db.offHeapAllocator, mergedDense);
                        db.touch(next);
                        db.refreshEstimatedBytes(k, next);
                        deltaBytes[0] += next.estimatedBytes;
                        return next;
                    }

                    old.overwriteWithString(db.offHeapAllocator, mergedDense);
                    db.touch(old);
                    deltaBytes[0] -= oldEstimate;
                    db.refreshEstimatedBytes(k, old);
                    deltaBytes[0] += old.estimatedBytes;
                    return old;
                });
            } catch (OffHeapOutOfMemoryException e) {
                db.rollbackWrite();
                throw new YierdisCommandException("OOM off-heap memory limit exceeded");
            }
            db.commitWrite(deltaBytes[0]);
            // 与 SET 类似：PFMERGE 结果写入后应清除 destKey 的 TTL。
            db.removeExpire(destKeyBytes);
            YierdisChangeTracking.markValueChanged();
        }
    }
}
