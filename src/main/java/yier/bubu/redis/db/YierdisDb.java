package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public final class YierdisDb {
    private static final long NO_EXPIRE = -1L;

    private final ConcurrentHashMap<ByteArrayKey, Entry> store = new ConcurrentHashMap<>();

    public YierdisDb() {
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    public void shutdown() {
        // No-op: this DB does not own threads. Kept for API symmetry / tests.
    }

    public void flushDb() {
        store.clear();
    }

    public long del(Collection<byte[]> keys) {
        long now = System.currentTimeMillis();
        long removed = 0;
        for (byte[] keyBytes : keys) {
            ByteArrayKey key = new ByteArrayKey(keyBytes);
            Entry e = store.get(key);
            if (e == null) {
                continue;
            }
            if (removeIfExpired(key, e, now)) {
                continue;
            }
            if (store.remove(key, e)) {
                removed++;
            }
        }
        return removed;
    }

    public long exists(Collection<byte[]> keys) {
        long count = 0;
        for (byte[] keyBytes : keys) {
            if (getEntryIfNotExpired(new ByteArrayKey(keyBytes)) != null) {
                count++;
            }
        }
        return count;
    }

    public ValueType typeOf(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return null;
        }
        synchronized (e) {
            return e.value.type();
        }
    }

    public boolean expire(byte[] keyBytes, long seconds) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
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
        ByteArrayKey key = new ByteArrayKey(keyBytes);
        Entry e = store.get(key);
        if (e == null) {
            return -2;
        }
        long now = System.currentTimeMillis();
        if (removeIfExpired(key, e, now)) {
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
        for (Map.Entry<ByteArrayKey, Entry> e : store.entrySet()) {
            if (removeIfExpired(e.getKey(), e.getValue(), now)) {
                continue;
            }
            if (globMatches(globPattern, e.getKey().bytes())) {
                out.add(e.getKey().bytes());
            }
        }
        return out;
    }

    public boolean setString(byte[] keyBytes, byte[] value, SetMode mode, ExpireOption expireOption) {
        long now = System.currentTimeMillis();
        long expireAt = expireOption == null ? NO_EXPIRE : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (mode == SetMode.NX && old != null) {
                return old;
            }
            if (mode == SetMode.XX && old == null) {
                return null;
            }

            Entry next = new Entry(new StringValue(value), expireAt);
            didSet[0] = true;
            return next;
        });
        return didSet[0];
    }

    public byte[] getStringBytes(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (!(e.value instanceof StringValue)) {
                throw new WrongTypeException();
            }
            return ((StringValue) e.value).toBytes();
        }
    }

    public int strlen(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (!(e.value instanceof StringValue)) {
                throw new WrongTypeException();
            }
            return ((StringValue) e.value).byteLength();
        }
    }

    public int append(byte[] keyBytes, byte[] appendValue) {
        long now = System.currentTimeMillis();
        final int[] newLen = new int[]{0};
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                StringValue v = new StringValue(appendValue);
                newLen[0] = v.byteLength();
                return new Entry(v, NO_EXPIRE);
            }

            synchronized (old) {
                if (!(old.value instanceof StringValue)) {
                    throw new WrongTypeException();
                }
                StringValue sv = (StringValue) old.value;
                newLen[0] = sv.append(appendValue);
                return old;
            }
        });
        return newLen[0];
    }

    public long incrBy(byte[] keyBytes, long delta) {
        long now = System.currentTimeMillis();
        final long[] result = new long[]{0L};
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                long next = delta;
                result[0] = next;
                return new Entry(new StringValue(Long.toString(next)), NO_EXPIRE);
            }

            synchronized (old) {
                if (!(old.value instanceof StringValue)) {
                    throw new WrongTypeException();
                }
                StringValue sv = (StringValue) old.value;
                result[0] = sv.incrBy(delta);
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
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
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
                return new Entry(lv, NO_EXPIRE);
            }

            synchronized (old) {
                if (!(old.value instanceof ListValue)) {
                    throw new WrongTypeException();
                }
                ListValue lv = (ListValue) old.value;
                if (left) {
                    lv.lpushAll(values);
                } else {
                    lv.rpushAll(values);
                }
                len[0] = lv.size();
                return old;
            }
        });
        return len[0];
    }

    public List<byte[]> lrange(byte[] keyBytes, int start, int stop) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (!(e.value instanceof ListValue)) {
                throw new WrongTypeException();
            }
            return ((ListValue) e.value).range(start, stop);
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
        store.computeIfPresent(new ByteArrayKey(keyBytes), (k, old) -> {
            if (isEntryExpired(old, now)) {
                popped[0] = new ArrayList<>();
                return null;
            }
            synchronized (old) {
                if (!(old.value instanceof ListValue)) {
                    throw new WrongTypeException();
                }
                ListValue lv = (ListValue) old.value;
                popped[0] = left ? lv.lpop(count) : lv.rpop(count);
                if (lv.size() == 0) {
                    return null;
                }
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
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                HashValue hv = new HashValue();
                added[0] = hv.hsetMany(fieldValuePairs);
                return new Entry(hv, NO_EXPIRE);
            }
            synchronized (old) {
                if (!(old.value instanceof HashValue)) {
                    throw new WrongTypeException();
                }
                added[0] = ((HashValue) old.value).hsetMany(fieldValuePairs);
                return old;
            }
        });
        return added[0];
    }

    public byte[] hget(byte[] keyBytes, byte[] fieldBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (!(e.value instanceof HashValue)) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.value).hget(fieldBytes);
        }
    }

    public List<byte[]> hgetall(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (!(e.value instanceof HashValue)) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.value).hgetallPairs();
        }
    }

    public int hlen(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (!(e.value instanceof HashValue)) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.value).size();
        }
    }

    public int hdel(byte[] keyBytes, List<byte[]> fields) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(new ByteArrayKey(keyBytes), (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (!(old.value instanceof HashValue)) {
                    throw new WrongTypeException();
                }
                HashValue hv = (HashValue) old.value;
                removed[0] = hv.hdel(fields);
                if (hv.size() == 0) {
                    return null;
                }
                return old;
            }
        });
        return removed[0];
    }

    public int sadd(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                SetValue sv = new SetValue();
                added[0] = sv.addAll(members);
                return new Entry(sv, NO_EXPIRE);
            }
            synchronized (old) {
                if (!(old.value instanceof SetValue)) {
                    throw new WrongTypeException();
                }
                added[0] = ((SetValue) old.value).addAll(members);
                return old;
            }
        });
        return added[0];
    }

    public int srem(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(new ByteArrayKey(keyBytes), (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (!(old.value instanceof SetValue)) {
                    throw new WrongTypeException();
                }
                SetValue sv = (SetValue) old.value;
                removed[0] = sv.removeAll(members);
                if (sv.size() == 0) {
                    return null;
                }
                return old;
            }
        });
        return removed[0];
    }

    public List<byte[]> smembers(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (!(e.value instanceof SetValue)) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.value).members();
        }
    }

    public boolean sismember(byte[] keyBytes, byte[] memberBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return false;
        }
        synchronized (e) {
            if (!(e.value instanceof SetValue)) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.value).contains(memberBytes);
        }
    }

    public int scard(byte[] keyBytes) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return 0;
        }
        synchronized (e) {
            if (!(e.value instanceof SetValue)) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.value).size();
        }
    }

    public int zadd(byte[] keyBytes, List<byte[]> scoreMemberPairs) {
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
        }
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(new ByteArrayKey(keyBytes), (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                ZSetValue zv = new ZSetValue();
                added[0] = zv.zaddMany(scoreMemberPairs);
                return new Entry(zv, NO_EXPIRE);
            }
            synchronized (old) {
                if (!(old.value instanceof ZSetValue)) {
                    throw new WrongTypeException();
                }
                added[0] = ((ZSetValue) old.value).zaddMany(scoreMemberPairs);
                return old;
            }
        });
        return added[0];
    }

    public List<byte[]> zrange(byte[] keyBytes, int start, int stop, boolean withScores) {
        Entry e = getEntryIfNotExpired(new ByteArrayKey(keyBytes));
        if (e == null) {
            return new ArrayList<>();
        }
        synchronized (e) {
            if (!(e.value instanceof ZSetValue)) {
                throw new WrongTypeException();
            }
            return ((ZSetValue) e.value).zrange(start, stop, withScores);
        }
    }

    public int zrem(byte[] keyBytes, List<byte[]> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(new ByteArrayKey(keyBytes), (k, old) -> {
            if (isEntryExpired(old, now)) {
                return null;
            }
            synchronized (old) {
                if (!(old.value instanceof ZSetValue)) {
                    throw new WrongTypeException();
                }
                ZSetValue zv = (ZSetValue) old.value;
                removed[0] = zv.zrem(members);
                if (zv.size() == 0) {
                    return null;
                }
                return old;
            }
        });
        return removed[0];
    }

    public void cleanupExpired() {
        long now = System.currentTimeMillis();
        for (Map.Entry<ByteArrayKey, Entry> e : store.entrySet()) {
            removeIfExpired(e.getKey(), e.getValue(), now);
        }
    }

    private Entry getEntryIfNotExpired(ByteArrayKey key) {
        Entry e = store.get(key);
        if (e == null) {
            return null;
        }
        if (removeIfExpired(key, e, System.currentTimeMillis())) {
            return null;
        }
        return e;
    }

    private boolean removeIfExpired(ByteArrayKey key, Entry e, long nowMillis) {
        if (!isEntryExpired(e, nowMillis)) {
            return false;
        }
        store.remove(key, e);
        return true;
    }

    private boolean isEntryExpired(Entry e, long nowMillis) {
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

    static final class Entry {
        final YierdisValue value;
        volatile long expireAtMillis;

        Entry(YierdisValue value, long expireAtMillis) {
            this.value = value;
            this.expireAtMillis = expireAtMillis;
        }
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
