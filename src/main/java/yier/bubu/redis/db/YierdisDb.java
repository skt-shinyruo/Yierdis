package yier.bubu.redis.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class YierdisDb {
    private static final long NO_EXPIRE = -1L;

    private final ConcurrentHashMap<String, Entry> store = new ConcurrentHashMap<>();

    public YierdisDb() {
        // Scheduling (if any) is done by the Netty event loop in YierdisServer, not by a dedicated thread.
    }

    public void shutdown() {
        // No-op: this DB does not own threads. Kept for API symmetry / tests.
    }

    public void flushDb() {
        store.clear();
    }

    public long del(Collection<String> keys) {
        long now = System.currentTimeMillis();
        long removed = 0;
        for (String key : keys) {
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

    public long exists(Collection<String> keys) {
        long count = 0;
        for (String k : keys) {
            if (getEntryIfNotExpired(k) != null) {
                count++;
            }
        }
        return count;
    }

    public ValueType typeOf(String key) {
        Entry e = getEntryIfNotExpired(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            return e.value.type();
        }
    }

    public boolean expire(String key, long seconds) {
        Entry e = getEntryIfNotExpired(key);
        if (e == null) {
            return false;
        }
        long expireAt = System.currentTimeMillis() + Math.max(0, seconds) * 1000L;
        synchronized (e) {
            e.expireAtMillis = expireAt;
        }
        return true;
    }

    public long ttlSeconds(String key) {
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

    public Set<String> keys(String globPattern) {
        if (globPattern == null) {
            return Collections.emptySet();
        }
        Pattern regex = Pattern.compile(globToRegex(globPattern));
        long now = System.currentTimeMillis();
        Set<String> out = ConcurrentHashMap.newKeySet();
        for (Map.Entry<String, Entry> e : store.entrySet()) {
            if (removeIfExpired(e.getKey(), e.getValue(), now)) {
                continue;
            }
            if (regex.matcher(e.getKey()).matches()) {
                out.add(e.getKey());
            }
        }
        return out;
    }

    public boolean setString(String key, String value, SetMode mode, ExpireOption expireOption) {
        long now = System.currentTimeMillis();
        long expireAt = expireOption == null ? NO_EXPIRE : expireOption.toExpireAtMillis(now);

        final boolean[] didSet = new boolean[]{false};
        store.compute(key, (k, old) -> {
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

    public String getString(String key) {
        Entry e = getEntryIfNotExpired(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (!(e.value instanceof StringValue)) {
                throw new WrongTypeException();
            }
            return ((StringValue) e.value).get();
        }
    }

    public int strlen(String key) {
        String s = getString(key);
        return s == null ? 0 : s.length();
    }

    public int append(String key, String appendValue) {
        long now = System.currentTimeMillis();
        final int[] newLen = new int[]{0};
        store.compute(key, (k, old) -> {
            if (old != null && isEntryExpired(old, now)) {
                old = null;
            }
            if (old == null) {
                StringValue v = new StringValue(appendValue);
                newLen[0] = v.get().length();
                return new Entry(v, NO_EXPIRE);
            }

            synchronized (old) {
                if (!(old.value instanceof StringValue)) {
                    throw new WrongTypeException();
                }
                StringValue sv = (StringValue) old.value;
                sv.set(sv.get() + appendValue);
                newLen[0] = sv.get().length();
                return old;
            }
        });
        return newLen[0];
    }

    public long incrBy(String key, long delta) {
        long now = System.currentTimeMillis();
        final long[] result = new long[]{0L};
        store.compute(key, (k, old) -> {
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
                long current;
                try {
                    current = Long.parseLong(sv.get().trim());
                } catch (NumberFormatException e) {
                    throw new YierdisCommandException("ERR value is not an integer or out of range");
                }
                long next = current + delta;
                sv.set(Long.toString(next));
                result[0] = next;
                return old;
            }
        });
        return result[0];
    }

    public int lpush(String key, List<String> values) {
        return pushInternal(key, values, true);
    }

    public int rpush(String key, List<String> values) {
        return pushInternal(key, values, false);
    }

    private int pushInternal(String key, List<String> values, boolean left) {
        long now = System.currentTimeMillis();
        final int[] len = new int[]{0};
        store.compute(key, (k, old) -> {
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

    public List<String> lrange(String key, int start, int stop) {
        Entry e = getEntryIfNotExpired(key);
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

    public List<String> lpop(String key, int count) {
        return popInternal(key, count, true);
    }

    public List<String> rpop(String key, int count) {
        return popInternal(key, count, false);
    }

    private List<String> popInternal(String key, int count, boolean left) {
        if (count <= 0) {
            return new ArrayList<>();
        }
        long now = System.currentTimeMillis();
        final List<String>[] popped = new List[]{null};
        store.computeIfPresent(key, (k, old) -> {
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

    public int hset(String key, List<String> fieldValuePairs) {
        if (fieldValuePairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'hset' command");
        }
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(key, (k, old) -> {
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

    public String hget(String key, String field) {
        Entry e = getEntryIfNotExpired(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (!(e.value instanceof HashValue)) {
                throw new WrongTypeException();
            }
            return ((HashValue) e.value).hget(field);
        }
    }

    public List<String> hgetall(String key) {
        Entry e = getEntryIfNotExpired(key);
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

    public int hlen(String key) {
        Entry e = getEntryIfNotExpired(key);
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

    public int hdel(String key, List<String> fields) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(key, (k, old) -> {
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

    public int sadd(String key, List<String> members) {
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(key, (k, old) -> {
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

    public int srem(String key, List<String> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(key, (k, old) -> {
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

    public List<String> smembers(String key) {
        Entry e = getEntryIfNotExpired(key);
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

    public boolean sismember(String key, String member) {
        Entry e = getEntryIfNotExpired(key);
        if (e == null) {
            return false;
        }
        synchronized (e) {
            if (!(e.value instanceof SetValue)) {
                throw new WrongTypeException();
            }
            return ((SetValue) e.value).contains(member);
        }
    }

    public int scard(String key) {
        Entry e = getEntryIfNotExpired(key);
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

    public int zadd(String key, List<String> scoreMemberPairs) {
        if (scoreMemberPairs.size() % 2 != 0) {
            throw new YierdisCommandException("ERR wrong number of arguments for 'zadd' command");
        }
        long now = System.currentTimeMillis();
        final int[] added = new int[]{0};
        store.compute(key, (k, old) -> {
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

    public List<String> zrange(String key, int start, int stop, boolean withScores) {
        Entry e = getEntryIfNotExpired(key);
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

    public int zrem(String key, List<String> members) {
        long now = System.currentTimeMillis();
        final int[] removed = new int[]{0};
        store.computeIfPresent(key, (k, old) -> {
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
        for (Map.Entry<String, Entry> e : store.entrySet()) {
            removeIfExpired(e.getKey(), e.getValue(), now);
        }
    }

    private Entry getEntryIfNotExpired(String key) {
        Entry e = store.get(key);
        if (e == null) {
            return null;
        }
        if (removeIfExpired(key, e, System.currentTimeMillis())) {
            return null;
        }
        return e;
    }

    private boolean removeIfExpired(String key, Entry e, long nowMillis) {
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

    private static String globToRegex(String glob) {
        // Minimal glob -> regex conversion:
        // * => .*
        // ? => .
        // Escape other regex metas.
        StringBuilder sb = new StringBuilder(glob.length() + 8);
        sb.append('^');
        for (int i = 0; i < glob.length(); i++) {
            char c = glob.charAt(i);
            switch (c) {
                case '*':
                    sb.append(".*");
                    break;
                case '?':
                    sb.append('.');
                    break;
                case '.':
                case '\\':
                case '+':
                case '(':
                case ')':
                case '^':
                case '$':
                case '{':
                case '}':
                case '[':
                case ']':
                case '|':
                    sb.append('\\').append(c);
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append('$');
        return sb.toString();
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
