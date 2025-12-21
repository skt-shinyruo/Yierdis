package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class YierdisDb {
    private static final long NO_EXPIRE = -1L;

    private final ByteArrayKeyspace<YierdisObject> store = new ByteArrayKeyspace<>();

    public YierdisDb() {
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    public void shutdown() {
        // No-op: this DB does not own threads. Kept for API symmetry / tests.
    }

    public void flushDb() {
        store.clear();
    }

    public int size() {
        return store.size();
    }

    public long del(Collection<byte[]> keys) {
        long now = System.currentTimeMillis();
        long removed = 0;
        for (byte[] keyBytes : keys) {
            YierdisObject e = store.get(keyBytes);
            if (e == null) {
                continue;
            }
            if (removeIfExpired(keyBytes, e, now)) {
                continue;
            }
            if (store.remove(keyBytes, e)) {
                removed++;
            }
        }
        return removed;
    }

    public long exists(Collection<byte[]> keys) {
        long count = 0;
        for (byte[] keyBytes : keys) {
            if (getObjectIfNotExpired(keyBytes) != null) {
                count++;
            }
        }
        return count;
    }

    public ValueType typeOf(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            return e.type;
        }
    }

    public boolean expire(byte[] keyBytes, long seconds) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        long expireAt = System.currentTimeMillis() + Math.max(0, seconds) * 1000L;
        synchronized (e) {
            e.expireAtMillis = expireAt;
        }
        return true;
    }

    public long ttlSeconds(byte[] keyBytes) {
        YierdisObject e = store.get(keyBytes);
        if (e == null) {
            return -2;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(keyBytes, e, now)) {
            return -2;
        }

        synchronized (e) {
            if (e.expireAtMillis == NO_EXPIRE) {
                return -1;
            }
            long remainingMillis = e.expireAtMillis - now;
            return remainingMillis <= 0 ? -2 : remainingMillis / 1000L;
        }
    }

    public List<byte[]> keys(byte[] globPattern) {
        if (globPattern == null) {
            return Collections.emptyList();
        }
        long now = System.currentTimeMillis();
        List<byte[]> out = new ArrayList<>();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        store.forEach((k, e) -> {
            if (isEntryExpired(e, now)) {
                expiredKeys.add(k);
                expiredValues.add(e);
                return;
            }
            if (globMatches(globPattern, k)) {
                out.add(k);
            }
        });
        for (int i = 0; i < expiredKeys.size(); i++) {
            store.remove(expiredKeys.get(i), expiredValues.get(i));
        }
        return out;
    }

    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        long now = System.currentTimeMillis();
        long expireAt = expireOption == null ? NO_EXPIRE : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (mode == SetMode.NX && old != null) {
                return old;
            }
            if (mode == SetMode.XX && old == null) {
                return null;
            }
            if (old == null) {
                didSet[0] = true;
                return YierdisObject.newString(value, expireAt);
            }
            synchronized (old) {
                old.overwriteWithString(value, expireAt);
                didSet[0] = true;
                return old;
            }
        });
        return didSet[0];
    }

    public byte[] getStringBytes(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            return e.stringBytesView();
        }
    }

    public int strlen(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (e.type != ValueType.STRING) {
                throw new WrongTypeException();
            }
            return e.stringByteLength();
        }
    }

    public int append(byte[] keyBytes, byte[] appendValue) {
        long now = System.currentTimeMillis();
        final int[] newLen = new int[]{0};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                YierdisObject o = YierdisObject.newString(appendValue, NO_EXPIRE);
                newLen[0] = o.stringByteLength();
                return o;
            }

            synchronized (old) {
                if (old.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                newLen[0] = old.stringAppend(appendValue);
                return old;
            }
        });
        return newLen[0];
    }

    public long incrBy(byte[] keyBytes, long delta) {
        long now = System.currentTimeMillis();
        final long[] result = new long[]{0L};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                long next = delta;
                result[0] = next;
                return YierdisObject.newStringInt(next, NO_EXPIRE);
            }

            synchronized (old) {
                if (old.type != ValueType.STRING) {
                    throw new WrongTypeException();
                }
                result[0] = old.stringIncrBy(delta);
                return old;
            }
        });
        return result[0];
    }

    public int lpush(byte[] keyBytes, List<byte[]> values) {
        return pushInternal(keyBytes, values, true);
    }

    public int rpush(byte[] keyBytes, List<byte[]> values) {
        return pushInternal(keyBytes, values, false);
    }

    private int pushInternal(byte[] keyBytes, List<byte[]> values, boolean left) {
        long now = System.currentTimeMillis();
        final int[] len = new int[]{0};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                ListValue lv = new ListValue();
                if (left) {
                    lv.lpushAll(values);
                } else {
                    lv.rpushAll(values);
                }
                len[0] = lv.size();
                return YierdisObject.newList(lv, NO_EXPIRE);
            }

            synchronized (old) {
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
                return old;
            }
        });
        return len[0];
    }

    public List<byte[]> lrange(byte[] keyBytes, int start, int stop) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.LIST) {
                throw new WrongTypeException();
            }
            return ((ListValue) e.payload).range(start, stop);
        }
    }

    public List<byte[]> lpop(byte[] keyBytes, int count) {
        return popInternal(keyBytes, count, true);
    }

    public List<byte[]> rpop(byte[] keyBytes, int count) {
        return popInternal(keyBytes, count, false);
    }

    private List<byte[]> popInternal(byte[] keyBytes, int count, boolean left) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        final List<byte[]>[] popped = new List[]{null};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                popped[0] = new ArrayList<>();
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.LIST) {
                    throw new WrongTypeException();
                }
                ListValue lv = (ListValue) old.payload;
                popped[0] = left ? lv.lpop(count) : lv.rpop(count);
                if (lv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return popped[0] == null ? new ArrayList<>() : popped[0];
    }

    public int hset(byte[] keyBytes, List<byte[]> fieldValuePairs) {
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                HashValue hv = new HashValue();
                added[0] = hv.hsetMany(fieldValuePairs);
                return YierdisObject.newHash(hv, NO_EXPIRE);
            }
            synchronized (old) {
                if (old.type != ValueType.HASH) {
                    throw new WrongTypeException();
                }
                added[0] = ((HashValue) old.payload).hsetMany(fieldValuePairs);
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return added[0];
    }

    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.payload).hget(fieldBytes);
        }
    }

    public List<byte[]> hgetall(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.payload).hgetallPairs();
        }
    }

    public int hlen(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (e.type != ValueType.HASH) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.payload).size();
        }
    }

    public int hdel(byte[] keyBytes, List<byte[]> fields) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.HASH) {
                    throw new WrongTypeException();
                }
                HashValue hv = (HashValue) old.payload;
                removed[0] = hv.hdel(fields);
                if (hv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return removed[0];
    }

    public int sadd(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                SetValue sv = new SetValue();
                added[0] = sv.addAll(members);
                return YierdisObject.newSet(sv, NO_EXPIRE);
            }
            synchronized (old) {
                if (old.type != ValueType.SET) {
                    throw new WrongTypeException();
                }
                added[0] = ((SetValue) old.payload).addAll(members);
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return added[0];
    }

    public int srem(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.SET) {
                    throw new WrongTypeException();
                }
                SetValue sv = (SetValue) old.payload;
                removed[0] = sv.removeAll(members);
                if (sv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return removed[0];
    }

    public List<byte[]> smembers(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.payload).members();
        }
    }

    public boolean sismember(byte[] keyBytes, byte[] memberBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return false;
        }
        synchronized (e) {
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.payload).contains(memberBytes);
        }
    }

    public int scard(byte[] keyBytes) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (e.type != ValueType.SET) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.payload).size();
        }
    }

    public int zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
        }
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(keyBytes, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                ZSetValue zv = new ZSetValue();
                added[0] = zv.zaddMany(scoreMemberPairs);
                return YierdisObject.newZSet(zv, NO_EXPIRE);
            }
            synchronized (old) {
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                added[0] = ((ZSetValue) old.payload).zaddMany(scoreMemberPairs);
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return added[0];
    }

    public List<byte[]> zrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            return ((ZSetValue) e.payload).zrange(start, stop, withScores);
        }
    }

    public List<byte[]> zrevrange(byte[] keyBytes, long start, long stop, boolean withScores) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            return ((ZSetValue) e.payload).zrevrange(start, stop, withScores);
        }
    }

    public List<byte[]> zrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            return ((ZSetValue) e.payload).zrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
    }

    public List<byte[]> zrevrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive, boolean withScores, long offset, long count) {
        YierdisObject e = getObjectIfNotExpired(keyBytes);
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (e.type != ValueType.ZSET) {
                throw new WrongTypeException();
            }
            return ((ZSetValue) e.payload).zrevrangeByScore(min, minExclusive, max, maxExclusive, withScores, offset, count);
        }
    }

    public int zrem(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zrem(members);
                if (zv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return removed[0];
    }

    public int zremrangeByRank(byte[] keyBytes, long start, long stop) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zremrangeByRank(start, stop);
                if (zv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return removed[0];
    }

    public int zremrangeByScore(byte[] keyBytes, double min, boolean minExclusive, double max, boolean maxExclusive) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(keyBytes, (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (old.type != ValueType.ZSET) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.payload;
                removed[0] = zv.zremrangeByScore(min, minExclusive, max, maxExclusive);
                if (zv.size() == 0) {
                    return null;
                }
                old.refreshCompositeEncodingFromPayload();
                return old;
            }
        });
        return removed[0];
    }

    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        List<byte[]> expiredKeys = new ArrayList<>();
        List<YierdisObject> expiredValues = new ArrayList<>();
        store.forEach((k, e) -> {
            if (isEntryExpired(e, now)) {
                expiredKeys.add(k);
                expiredValues.add(e);
            }
        });
        for (int i = 0; i < expiredKeys.size(); i++) {
            store.remove(expiredKeys.get(i), expiredValues.get(i));
        }
    }

    private YierdisObject getObjectIfNotExpired(byte[] keyBytes) {
        YierdisObject e = store.get(keyBytes);
        if (e == null) {
            return null;
        }
        if (removeIfExpired(keyBytes, e, System.currentTimeMillis())) {
            return null;
        }
        return e;
    }

    private boolean removeIfExpired(byte[] keyBytes, YierdisObject e, long nowMillis) {
        if (!isEntryExpired(e, nowMillis)) {
            return false;
        }
        store.remove(keyBytes, e);
        return true;
    }

    private boolean isEntryExpired(YierdisObject e, long nowMillis) {
        long expireAt;
        synchronized (e) {
            expireAt = e.expireAtMillis;
        }
        if (expireAt == NO_EXPIRE) {
            return false;
        }
        return expireAt <= nowMillis;
    }

    private static boolean globMatches(byte[] pattern, byte[] text) {
        int p = 0;
        int t = 0;
        int star = -1;
        int match = 0;
        while (t < text.length) {
            if (p < pattern.length && (pattern[p] == '?' || pattern[p] == text[t])) {
                p++;
                t++;
                continue;
            }
            if (p < pattern.length && pattern[p] == '*') {
                star = p++;
                match = t;
                continue;
            }
            if (star != -1) {
                p = star + 1;
                t = ++match;
                continue;
            }
            return false;
        }
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    public enum SetMode {
        NORMAL,
        NX,
        XX
    }

    public static final class ExpireOption {
        final TimeUnit unit;
        final long duration;

        public ExpireOption(TimeUnit unit, long duration) {
            this.unit = unit;
            this.duration = duration;
        }

        long toExpireAtMillis(long nowMillis) {
            long ms = unit.toMillis(duration);
            if (ms <= 0) {
                return nowMillis;
            }
            return nowMillis + ms;
        }
    }

    public static final class YierdisCommandException extends RuntimeException {
        public YierdisCommandException(String message) {
            super(message);
        }
    }

    public static final class WrongTypeException extends RuntimeException {
        public WrongTypeException() {
            super("WRONGTYPE Operation against a key holding the wrong kind of value");
        }
    }

}
