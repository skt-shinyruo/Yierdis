# YierdisDb Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `YierdisDb` from a broad DB implementation holder into a focused `RuntimeDbEngine` facade by extracting glob matching, memory estimation, policy/config parsing, and component construction.

**Architecture:** Keep `YierdisDb` as the source-compatible concrete engine. Move pure algorithms into package-private helpers, move construction into package-private component/config classes, and harden architecture guards so extracted responsibilities do not drift back.

**Tech Stack:** Java 25, Maven, JUnit 4, Yierdis core DB/runtime modules, FFM-backed storage, existing `DbReads`/`DbWrites`/`RuntimeDbEngine` contracts.

---

## Source Design

This plan implements:

- `docs/superpowers/specs/2026-04-27-yierdisdb-decomposition-design.md`

Work in this order:

1. Extract glob matching.
2. Extract memory estimation.
3. Extract maxmemory policy/config parsing.
4. Extract storage/component factory.
5. Harden architecture guards and update docs.

## File Map

### Create

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobMatcher.java`
  Redis-style byte glob matcher used by keyspace operations.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisGlobMatcherTest.java`
  Focused matcher semantics tests.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryEstimator.java`
  Entry and mutation upper-bound memory estimation.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbMemoryEstimatorTest.java`
  Focused estimator arithmetic tests.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemoryPolicies.java`
  Package helper that preserves `YierdisDb` null/blank policy defaults while reusing API normalization.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbConfig.java`
  Immutable validated construction config.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbConstructionTest.java`
  Constructor behavior tests for policy/default validation.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbStorageComponents.java`
  Passive holder for resolved runtime, allocator, owned resources, keyspace, and expire index.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponents.java`
  Passive holder for the DB object graph built for `YierdisDb`.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbRuntimeInternals.java`
  Package-private replacement for `YierdisDb`'s private `DbInternals` inner class.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponentFactory.java`
  Component factory that resolves storage and constructs collaborators.

### Modify

- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
  Remove pure helpers and constructor object graph assembly; delegate to components.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`
  Use `YierdisGlobMatcher`.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
  Use shared estimator helpers for string write upper bounds.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
  Use shared estimator helpers for common byte-length and collection arithmetic.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
  Use shared estimator helpers for common byte-length and collection arithmetic.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
  Use shared set write upper-bound helpers.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
  Use shared zset write upper-bound and member-length helpers.
- `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`
  Use shared string write upper-bound helpers.
- `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
  Guard extracted methods and direct FFM construction from returning to `YierdisDb`.
- `docs/db-internals.md`
  Explain that construction is now owned by component/config classes.

## Task 1: Extract Glob Matching

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobMatcher.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisGlobMatcherTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

- [ ] **Step 1: Write the failing glob matcher test**

Create `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisGlobMatcherTest.java`:

```java
package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;

import java.nio.charset.StandardCharsets;

public class YierdisGlobMatcherTest {
    @Test
    public void matchesLiteralStarQuestionAndBytesView() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("user:*"), b("user:123")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("user:??"), b("user:ab")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("user:??"), b("user:a")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a*c"), view(b("abbbc"))));
    }

    @Test
    public void matchesCharacterClassesNegationAndRanges() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[0-9]"), b("key7")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("key[0-9]"), b("keyx")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[^0-9]"), b("keyx")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("key[^0-9]"), b("key7")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("key[!a-c]"), b("keyz")));
    }

    @Test
    public void matchesEscapesAndMalformedClassesLikeCurrentDbMatcher() {
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a\\*b"), b("a*b")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("a\\*b"), b("axxb")));
        Assert.assertTrue(YierdisGlobMatcher.matches(new byte[]{'a', '\\'}, new byte[]{'a', '\\'}));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a["), b("a[")));
        Assert.assertTrue(YierdisGlobMatcher.matches(b("a[]]"), b("a]")));
    }

    @Test
    public void rejectsNullInputsAndNegativeLengthViews() {
        Assert.assertFalse(YierdisGlobMatcher.matches(null, b("x")));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), (byte[]) null));
        Assert.assertFalse(YierdisGlobMatcher.matches(b("*"), new BytesView() {
            @Override
            public int length() {
                return -1;
            }

            @Override
            public byte getByte(int index) {
                throw new AssertionError("negative length view must not be read");
            }
        }));
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static BytesView view(byte[] data) {
        return new BytesView() {
            @Override
            public int length() {
                return data.length;
            }

            @Override
            public byte getByte(int index) {
                return data[index];
            }
        };
    }
}
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisGlobMatcherTest test
```

Expected: FAIL at compilation with `cannot find symbol` for `YierdisGlobMatcher`.

- [ ] **Step 3: Create `YierdisGlobMatcher`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobMatcher.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.bytes.BytesView;

final class YierdisGlobMatcher {
    private YierdisGlobMatcher() {
    }

    static boolean matches(byte[] pattern, byte[] text) {
        if (pattern == null || text == null) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < text.length) {
            if (p < pattern.length) {
                byte pc = pattern[p];

                if (pc == '*') {
                    star = p++;
                    starText = t;
                    continue;
                }

                if (pc == '?') {
                    p++;
                    t++;
                    continue;
                }

                if (pc == '\\') {
                    if (p + 1 < pattern.length) {
                        byte literal = pattern[p + 1];
                        if (literal == text[t]) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else if (text[t] == '\\') {
                        p++;
                        t++;
                        continue;
                    }
                } else if (pc == '[') {
                    int end = findClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (classMatches(pattern, p + 1, end, text[t])) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else if (text[t] == '[') {
                        p++;
                        t++;
                        continue;
                    }
                } else if (pc == text[t]) {
                    p++;
                    t++;
                    continue;
                }
            }

            if (star >= 0) {
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    static boolean matches(byte[] pattern, BytesView text) {
        if (pattern == null || text == null) {
            return false;
        }
        int textLen = text.len();
        if (textLen < 0) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < textLen) {
            byte tb = text.byteAt(t);
            if (p < pattern.length) {
                byte pc = pattern[p];

                if (pc == '*') {
                    star = p++;
                    starText = t;
                    continue;
                }

                if (pc == '?') {
                    p++;
                    t++;
                    continue;
                }

                if (pc == '\\') {
                    if (p + 1 < pattern.length) {
                        byte literal = pattern[p + 1];
                        if (literal == tb) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else if (tb == '\\') {
                        p++;
                        t++;
                        continue;
                    }
                } else if (pc == '[') {
                    int end = findClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (classMatches(pattern, p + 1, end, tb)) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else if (tb == '[') {
                        p++;
                        t++;
                        continue;
                    }
                } else if (pc == tb) {
                    p++;
                    t++;
                    continue;
                }
            }

            if (star >= 0) {
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    private static int findClassEnd(byte[] pattern, int start) {
        if (pattern == null) {
            return -1;
        }
        int len = pattern.length;
        if (start >= len) {
            return -1;
        }

        int i = start;
        if (i < len && (pattern[i] == '^' || pattern[i] == '!')) {
            i++;
        }

        boolean first = true;
        while (i < len) {
            byte c = pattern[i];
            if (c == '\\') {
                i += i + 1 < len ? 2 : 1;
                first = false;
                continue;
            }
            if (c == ']' && !first) {
                return i;
            }
            i++;
            first = false;
        }
        return -1;
    }

    private static boolean classMatches(byte[] pattern, int start, int end, byte target) {
        if (pattern == null) {
            return false;
        }
        if (start < 0 || end < start || end >= pattern.length) {
            return false;
        }

        int i = start;
        boolean negate = false;
        if (i < end && (pattern[i] == '^' || pattern[i] == '!')) {
            negate = true;
            i++;
        }

        int tb = target & 0xff;
        boolean matched = false;

        if (i < end && pattern[i] == ']') {
            if (tb == (']' & 0xff)) {
                matched = true;
            }
            i++;
        }

        while (i < end) {
            int c1;
            if (pattern[i] == '\\' && i + 1 < end) {
                c1 = pattern[i + 1] & 0xff;
                i += 2;
            } else {
                c1 = pattern[i] & 0xff;
                i++;
            }

            if (i < end - 1 && pattern[i] == '-') {
                int j = i + 1;
                int c2;
                if (pattern[j] == '\\' && j + 1 < end) {
                    c2 = pattern[j + 1] & 0xff;
                    j += 2;
                } else {
                    c2 = pattern[j] & 0xff;
                    j++;
                }

                int lo = Math.min(c1, c2);
                int hi = Math.max(c1, c2);
                if (tb >= lo && tb <= hi) {
                    matched = true;
                }
                i = j;
                continue;
            }

            if (tb == c1) {
                matched = true;
            }
        }

        return negate ? !matched : matched;
    }
}
```

- [ ] **Step 4: Point keyspace operations at the matcher and remove matcher methods from `YierdisDb`**

In `YierdisKeyspaceOps`, replace both calls:

```java
YierdisDb.globMatches(globPattern, k)
```

with:

```java
YierdisGlobMatcher.matches(globPattern, k)
```

In `YierdisDb`, delete these methods completely:

```java
static boolean globMatches(byte[] pattern, byte[] text)
static boolean globMatches(byte[] pattern, BytesView text)
private static int findGlobClassEnd(byte[] pattern, int start)
private static boolean globClassMatches(byte[] pattern, int start, int end, byte target)
```

Also remove the now-unused `BytesSlice`, `java.util.ArrayList`, `java.util.Collection`,
`java.util.Collections`, and `java.util.List` imports if they become unused after this task.

- [ ] **Step 5: Run focused verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisGlobMatcherTest,ScanCursorContractTest,KeysBudgetTest test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisGlobMatcher.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisGlobMatcherTest.java
git commit -m "refactor: extract yierdis glob matcher"
```

## Task 2: Extract Memory Estimation

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryEstimator.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbMemoryEstimatorTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java`

- [ ] **Step 1: Write the failing estimator test**

Create `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbMemoryEstimatorTest.java`:

```java
package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.DbMemoryConstants;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class YierdisDbMemoryEstimatorTest {
    @Test
    public void estimatesHeapStringEntryBytesIncludingHeapKey() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(false, null);
        KeyHandle key = KeyHandle.forHeap(b("abc"), 1);
        YierdisObject object = YierdisObject.newString(null, b("hello"));

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 3L + 5L;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesHeapStringEntryBytesExcludingOffHeapKey() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(true, null);
        KeyHandle key = KeyHandle.forHeap(b("abc"), 1);
        YierdisObject object = YierdisObject.newString(null, b("hello"));

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 5L;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesIntegerEncodedStringPayloadAsLongBytes() {
        YierdisDbMemoryEstimator estimator = new YierdisDbMemoryEstimator(false, null);
        KeyHandle key = KeyHandle.forHeap(b("n"), 1);
        YierdisObject object = YierdisObject.newStringInt(42L);

        long expected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + Long.BYTES;

        Assert.assertEquals(expected, estimator.estimateEntryBytes(key, object));
    }

    @Test
    public void estimatesWriteUpperBoundsAndByteSums() {
        Assert.assertEquals(
                DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 3L + 5L,
                YierdisDbMemoryEstimator.estimateStringWriteUpperBound(3, 5)
        );
        Assert.assertEquals(6L, YierdisDbMemoryEstimator.sumByteLengths(List.of(b("a"), b("bc"), b("def"))));
        Assert.assertEquals(4L, YierdisDbMemoryEstimator.sumZSetMemberByteLengths(List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    @Test
    public void estimatesSetAndZSetCreationUpperBounds() {
        long setExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + 3L + (2L * 32L);
        long zsetExpected = DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + 1L + 4L + (2L * 96L);

        Assert.assertEquals(setExpected, YierdisDbMemoryEstimator.estimateSetWriteUpperBound(1, List.of(b("a"), b("bc"))));
        Assert.assertEquals(zsetExpected, YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(1, List.of(b("1"), b("aa"), b("2"), b("bb"))));
    }

    private static byte[] b(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
```

- [ ] **Step 2: Run the focused test to verify it fails**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbMemoryEstimatorTest test
```

Expected: FAIL at compilation with `cannot find symbol` for `YierdisDbMemoryEstimator`.

- [ ] **Step 3: Create `YierdisDbMemoryEstimator`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryEstimator.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.ops.DbMemoryConstants;
import yier.bubu.redis.ops.ValueType;

import java.util.List;

final class YierdisDbMemoryEstimator {
    private static final long SET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 32L;
    private static final long ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE = 96L;

    private final boolean keysStoredOffHeap;
    private final OffHeapAllocator offHeapAllocator;

    YierdisDbMemoryEstimator(boolean keysStoredOffHeap, OffHeapAllocator offHeapAllocator) {
        this.keysStoredOffHeap = keysStoredOffHeap;
        this.offHeapAllocator = offHeapAllocator;
    }

    long estimateEntryBytes(KeyHandle keyHandle, YierdisObject object) {
        if (keyHandle == null || object == null) {
            return 0;
        }
        int keyLen = Math.max(0, keyHandle.len());
        int keyBytesCost = keysStoredOffHeap ? 0 : keyLen;
        return DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE + keyBytesCost + estimateValueBytes(object);
    }

    private long estimateValueBytes(YierdisObject object) {
        if (object == null) {
            return 0;
        }
        if (object.type == ValueType.STRING) {
            if (object.encoding == ValueEncoding.STRING_INT) {
                return Long.BYTES;
            }
            if (offHeapAllocator != null && object.payload instanceof OffHeapBuf) {
                return 0;
            }
            return object.rawLen;
        }

        if (object.payload instanceof HashValue hv) {
            return hv.estimatedBytes();
        }
        if (object.payload instanceof ListValue lv) {
            return lv.estimatedBytes();
        }
        if (object.payload instanceof SetValue sv) {
            return sv.estimatedBytes();
        }
        if (object.payload instanceof ZSetValue zv) {
            return zv.estimatedBytes();
        }
        return 0;
    }

    static long estimateStringWriteUpperBound(int keyLength, int valueLength) {
        return (long) Math.max(0, keyLength)
                + Math.max(0, valueLength)
                + DbMemoryConstants.ENTRY_OVERHEAD_BYTES_ESTIMATE;
    }

    static long sumByteLengths(List<byte[]> values) {
        if (values == null || values.isEmpty()) {
            return 0L;
        }
        long total = 0L;
        for (byte[] value : values) {
            if (value != null) {
                total += value.length;
            }
        }
        return total;
    }

    static long sumZSetMemberByteLengths(List<byte[]> scoreMemberPairs) {
        long memberBytes = 0L;
        if (scoreMemberPairs != null) {
            for (int i = 1; i < scoreMemberPairs.size(); i += 2) {
                byte[] member = scoreMemberPairs.get(i);
                if (member != null) {
                    memberBytes += member.length;
                }
            }
        }
        return memberBytes;
    }

    static long estimateCollectionWriteUpperBound(int keyLength, long payloadBytes, long structuralBytes) {
        return estimateStringWriteUpperBound(keyLength, 0)
                + Math.max(0L, payloadBytes)
                + Math.max(0L, structuralBytes);
    }

    static long estimateSetWriteUpperBound(int keyLength, List<byte[]> members) {
        int memberCount = members == null ? 0 : members.size();
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumByteLengths(members),
                Math.multiplyExact((long) memberCount, SET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }

    static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs) {
        int memberCount = scoreMemberPairs == null ? 0 : scoreMemberPairs.size() / 2;
        return estimateCollectionWriteUpperBound(
                keyLength,
                sumZSetMemberByteLengths(scoreMemberPairs),
                Math.multiplyExact((long) memberCount, ZSET_MEMBER_OVERHEAD_BYTES_ESTIMATE)
        );
    }
}
```

- [ ] **Step 4: Wire the estimator into `YierdisDb`**

In `YierdisDb`, add a field near the other collaborator fields:

```java
private final YierdisDbMemoryEstimator memoryEstimator;
```

In the constructor, before constructing ops, add:

```java
this.memoryEstimator = new YierdisDbMemoryEstimator(this.keysStoredOffHeap, this.offHeapAllocator);
```

Replace all ops construction method references:

```java
this.stringOps = new YierdisStringOps(internals, this.memoryEstimator::estimateEntryBytes);
this.hashOps = new YierdisHashOps(internals, this.memoryEstimator::estimateEntryBytes);
this.listOps = new YierdisListOps(internals, this.memoryEstimator::estimateEntryBytes);
this.setOps = new YierdisSetOps(internals, this.memoryEstimator::estimateEntryBytes);
this.zsetOps = new YierdisZSetOps(internals, this.memoryEstimator::estimateEntryBytes);
this.hllOps = new YierdisHllOps(internals, this.memoryEstimator::estimateEntryBytes);
```

Delete these methods from `YierdisDb`:

```java
private long estimateEntryBytes(KeyHandle keyHandle, YierdisObject e)
private long estimateValueBytes(YierdisObject e)
static long estimateStringWriteUpperBound(int keyLength, int valueLength)
static long sumByteLengths(List<byte[]> values)
private static long estimateCollectionWriteUpperBound(int keyLength, long payloadBytes, long structuralBytes)
static long estimateSetWriteUpperBound(int keyLength, List<byte[]> members)
static long estimateZSetWriteUpperBound(int keyLength, List<byte[]> scoreMemberPairs)
```

Remove imports that become unused because of these deletions:

```java
import yier.bubu.redis.offheap.api.OffHeapBuf;
import yier.bubu.redis.ops.ValueType;
import yier.bubu.redis.ops.DbMemoryConstants;
```

- [ ] **Step 5: Replace ops helper calls**

Apply these replacements:

```java
// YierdisStringOps
estimateStringWriteUpperBound(keyLength, valueLength)
```

becomes:

```java
YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyLength, valueLength)
```

Then delete `YierdisStringOps`'s private static `estimateStringWriteUpperBound`.

```java
// YierdisHashOps and YierdisListOps
sumByteLengths(values)
```

becomes:

```java
YierdisDbMemoryEstimator.sumByteLengths(values)
```

and:

```java
estimateStringWriteUpperBound(keyLength, valueLength)
```

becomes:

```java
YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyLength, valueLength)
```

For `estimateCollectionWriteUpperBound`, replace the method body with:

```java
return YierdisDbMemoryEstimator.estimateCollectionWriteUpperBound(keyLength, payloadBytes, structuralBytes);
```

Then delete now-unused local static helpers from each class.

```java
// YierdisSetOps
YierdisDb.estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members)
```

becomes:

```java
YierdisDbMemoryEstimator.estimateSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, members)
```

and:

```java
YierdisDb.sumByteLengths(members)
```

becomes:

```java
YierdisDbMemoryEstimator.sumByteLengths(members)
```

```java
// YierdisZSetOps
YierdisDb.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs)
```

becomes:

```java
YierdisDbMemoryEstimator.estimateZSetWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, scoreMemberPairs)
```

Replace the manual member-byte loop in `estimateZSetWriteUpperBoundForMutation` with:

```java
return YierdisDbMemoryEstimator.sumZSetMemberByteLengths(scoreMemberPairs);
```

```java
// YierdisHllOps
YierdisDb.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength)
```

and:

```java
YierdisDb.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength)
```

become calls to:

```java
YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, upperValueLength)
YierdisDbMemoryEstimator.estimateStringWriteUpperBound(keyBytes == null ? 0 : keyBytes.length, mergedDenseLength)
```

- [ ] **Step 6: Run focused verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbMemoryEstimatorTest,MemoryStatsAccountingConsistencyTest,MutationExecutorReservationTest test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=MaxmemoryEvictionTest,TtlMaxmemoryTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMemoryEstimator.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisStringOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisListOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisSetOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHllOps.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbMemoryEstimatorTest.java
git commit -m "refactor: extract yierdis db memory estimator"
```

## Task 3: Extract Maxmemory Policy Parsing And DB Config

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemoryPolicies.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbConfig.java`
- Create: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbConstructionTest.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

- [ ] **Step 1: Write the failing construction tests**

Create `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbConstructionTest.java`:

```java
package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

public class YierdisDbConstructionTest {
    @Test
    public void nullAndBlankMaxmemoryPoliciesDefaultToNoeviction() {
        assertConstructsWithPolicy(null);
        assertConstructsWithPolicy("");
        assertConstructsWithPolicy("   ");
    }

    @Test
    public void policyParsingNormalizesCaseAndUnderscore() {
        assertConstructsWithPolicy("ALLKEYS_RANDOM");
        assertConstructsWithPolicy("allkeys_LRU");
        assertConstructsWithPolicy("  NoEviction  ");
    }

    @Test
    public void unknownPolicyStillThrowsIllegalArgumentException() {
        try {
            new YierdisDb((OffHeapAllocator) null, 0, "unknown-policy", 5, 5, 5);
            Assert.fail("unknown policy should fail construction");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("unsupported maxmemoryPolicy"));
        }
    }

    @Test
    public void invalidConstructionNumbersStillThrowIllegalArgumentException() {
        assertInvalid(-1, "noeviction", 5, 5, 5, "maxmemoryBytes");
        assertInvalid(0, "noeviction", 0, 5, 5, "maxmemorySamples");
        assertInvalid(0, "noeviction", 5, 0, 5, "evictionTimeLimitMillis");
        assertInvalid(0, "noeviction", 5, 5, 0, "expireCleanupTimeLimitMillis");
    }

    private static void assertConstructsWithPolicy(String policy) {
        YierdisDb db = new YierdisDb((OffHeapAllocator) null, 0, policy, 5, 5, 5);
        try {
            db.bindToCurrentThread();
        } finally {
            db.shutdown();
        }
    }

    private static void assertInvalid(
            long maxmemoryBytes,
            String policy,
            int samples,
            long evictionMillis,
            long expireMillis,
            String messagePart
    ) {
        try {
            new YierdisDb((OffHeapAllocator) null, maxmemoryBytes, policy, samples, evictionMillis, expireMillis);
            Assert.fail("invalid construction should fail: " + messagePart);
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains(messagePart));
        }
    }
}
```

- [ ] **Step 2: Run the construction tests before changing implementation**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbConstructionTest test
```

Expected: PASS against current behavior. These are characterization tests before moving parsing and validation.

- [ ] **Step 3: Create `YierdisDbMaxmemoryPolicies`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemoryPolicies.java`:

```java
package yier.bubu.redis.db;

final class YierdisDbMaxmemoryPolicies {
    private YierdisDbMaxmemoryPolicies() {
    }

    static YierdisDb.MaxmemoryPolicy parseOrDefault(String policy) {
        if (policy == null || policy.isBlank()) {
            return YierdisDb.MaxmemoryPolicy.NOEVICTION;
        }
        yier.bubu.redis.ops.MaxmemoryPolicy parsed;
        try {
            parsed = yier.bubu.redis.ops.MaxmemoryPolicy.parse(policy);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unsupported maxmemoryPolicy: " + policy);
        }
        return switch (parsed) {
            case NOEVICTION -> YierdisDb.MaxmemoryPolicy.NOEVICTION;
            case ALLKEYS_RANDOM -> YierdisDb.MaxmemoryPolicy.ALLKEYS_RANDOM;
            case ALLKEYS_LRU -> YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU;
        };
    }
}
```

- [ ] **Step 4: Create `YierdisDbConfig`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbConfig.java`:

```java
package yier.bubu.redis.db;

import java.util.concurrent.TimeUnit;

final class YierdisDbConfig {
    final long maxmemoryBytes;
    final YierdisDb.MaxmemoryPolicy maxmemoryPolicy;
    final int maxmemorySamples;
    final boolean lruEnabled;
    final long evictionTimeLimitNanos;
    final long expireCleanupTimeLimitNanos;

    private YierdisDbConfig(
            long maxmemoryBytes,
            YierdisDb.MaxmemoryPolicy maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitNanos,
            long expireCleanupTimeLimitNanos
    ) {
        this.maxmemoryBytes = maxmemoryBytes;
        this.maxmemoryPolicy = maxmemoryPolicy;
        this.maxmemorySamples = maxmemorySamples;
        this.lruEnabled = maxmemoryBytes > 0 && maxmemoryPolicy == YierdisDb.MaxmemoryPolicy.ALLKEYS_LRU;
        this.evictionTimeLimitNanos = evictionTimeLimitNanos;
        this.expireCleanupTimeLimitNanos = expireCleanupTimeLimitNanos;
    }

    static YierdisDbConfig create(
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        if (maxmemoryBytes < 0) {
            throw new IllegalArgumentException("maxmemoryBytes must be >= 0");
        }
        if (maxmemorySamples <= 0) {
            throw new IllegalArgumentException("maxmemorySamples must be > 0");
        }
        if (evictionTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("evictionTimeLimitMillis must be > 0");
        }
        if (expireCleanupTimeLimitMillis <= 0) {
            throw new IllegalArgumentException("expireCleanupTimeLimitMillis must be > 0");
        }
        return new YierdisDbConfig(
                maxmemoryBytes,
                YierdisDbMaxmemoryPolicies.parseOrDefault(maxmemoryPolicy),
                maxmemorySamples,
                TimeUnit.MILLISECONDS.toNanos(evictionTimeLimitMillis),
                TimeUnit.MILLISECONDS.toNanos(expireCleanupTimeLimitMillis)
        );
    }
}
```

- [ ] **Step 5: Replace validation and parsing in `YierdisDb`**

In the main private constructor, replace the validation/parsing block with:

```java
YierdisDbConfig config = YierdisDbConfig.create(
        maxmemoryBytes,
        maxmemoryPolicy,
        maxmemorySamples,
        evictionTimeLimitMillis,
        expireCleanupTimeLimitMillis
);

this.maxmemoryBytes = config.maxmemoryBytes;
this.maxmemoryPolicy = config.maxmemoryPolicy;
this.maxmemorySamples = config.maxmemorySamples;
this.lruEnabled = config.lruEnabled;
this.evictionTimeLimitNanos = config.evictionTimeLimitNanos;
this.expireCleanupTimeLimitNanos = config.expireCleanupTimeLimitNanos;
```

Delete this method from `YierdisDb`:

```java
private static MaxmemoryPolicy parseMaxmemoryPolicy(String policy)
```

Remove the now-unused `java.util.concurrent.TimeUnit` import from `YierdisDb`.

- [ ] **Step 6: Run focused verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbConstructionTest,YierdisInstanceTest,GlobalMaxmemoryLruAcrossDbsTest test
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemoryPolicies.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbConfig.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java \
  yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbConstructionTest.java
git commit -m "refactor: extract yierdis db construction config"
```

## Task 4: Extract Storage And Component Factory

**Files:**
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbStorageComponents.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponents.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbRuntimeInternals.java`
- Create: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponentFactory.java`
- Modify: `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java`

- [ ] **Step 1: Run baseline construction/off-heap tests before moving object graph assembly**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbConstructionTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest test
```

Expected: PASS before refactoring. These tests protect the constructor and resource ownership behavior during the next steps.

- [ ] **Step 2: Create `YierdisDbStorageComponents`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbStorageComponents.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.db.memory.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.db.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.offheap.api.OffHeapAllocator;

final class YierdisDbStorageComponents {
    final YierdisFfmMemoryRuntime memoryRuntime;
    final OffHeapAllocator offHeapAllocator;
    final YierdisDbOwnedResources resources;
    final YierdisKeyspace<YierdisObject> store;
    final YierdisExpireIndex expires;
    final boolean keysStoredOffHeap;

    private YierdisDbStorageComponents(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            YierdisDbOwnedResources resources,
            YierdisKeyspace<YierdisObject> store,
            YierdisExpireIndex expires,
            boolean keysStoredOffHeap
    ) {
        this.memoryRuntime = memoryRuntime;
        this.offHeapAllocator = offHeapAllocator;
        this.resources = resources;
        this.store = store;
        this.expires = expires;
        this.keysStoredOffHeap = keysStoredOffHeap;
    }

    static YierdisDbStorageComponents create(
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime
    ) {
        YierdisFfmMemoryRuntime resolvedRuntime = memoryRuntime;
        OffHeapAllocator resolvedAllocator = offHeapAllocator;
        boolean resolvedOwnsAllocator = ownsOffHeapAllocator;
        boolean resolvedOwnsRuntime = ownsMemoryRuntime;

        if (resolvedRuntime == null && resolvedAllocator == null) {
            resolvedRuntime = new YierdisFfmMemoryRuntime("db");
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
            resolvedOwnsRuntime = true;
        } else if (resolvedRuntime == null) {
            if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator foreignAllocator)) {
                throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
            }
            resolvedRuntime = foreignAllocator.memoryRuntime();
        } else if (resolvedAllocator == null) {
            resolvedAllocator = new YierdisForeignOffHeapAllocator(resolvedRuntime, 0);
            resolvedOwnsAllocator = true;
        } else if (!(resolvedAllocator instanceof YierdisForeignOffHeapAllocator)) {
            throw new IllegalArgumentException("Only the foreign off-heap allocator is supported");
        }

        YierdisDbOwnedResources resources = new YierdisDbOwnedResources(
                resolvedRuntime,
                resolvedAllocator,
                resolvedOwnsRuntime,
                resolvedOwnsAllocator
        );
        YierdisFfmBlobStore blobStore = new YierdisFfmBlobStore(resolvedRuntime, "ffm-key");
        return new YierdisDbStorageComponents(
                resolvedRuntime,
                resolvedAllocator,
                resources,
                new YierdisFfmKeyspace<>(blobStore),
                new YierdisFfmExpireIndex(blobStore),
                true
        );
    }
}
```

- [ ] **Step 3: Create `YierdisDbRuntimeInternals`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbRuntimeInternals.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.MemoryLedger;

import java.util.Objects;

final class YierdisDbRuntimeInternals implements YierdisDbInternals {
    private final Runnable threadChecker;
    private final YierdisDbMutationExecutor mutationExecutor;
    private final YierdisDbKeyLifecycle keyLifecycle;
    private final MemoryLedger ledger;

    YierdisDbRuntimeInternals(
            Runnable threadChecker,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbKeyLifecycle keyLifecycle,
            MemoryLedger ledger
    ) {
        this.threadChecker = Objects.requireNonNull(threadChecker, "threadChecker");
        this.mutationExecutor = Objects.requireNonNull(mutationExecutor, "mutationExecutor");
        this.keyLifecycle = Objects.requireNonNull(keyLifecycle, "keyLifecycle");
        this.ledger = Objects.requireNonNull(ledger, "ledger");
    }

    @Override
    public void checkThread() {
        threadChecker.run();
    }

    @Override
    public <T> T executeMutation(YierdisDbMutationExecutor.MutationPlan<T> plan) {
        return mutationExecutor.execute(plan);
    }

    @Override
    public YierdisDbKeyLifecycle keyLifecycle() {
        return keyLifecycle;
    }

    @Override
    public MemoryLedger ledger() {
        return ledger;
    }
}
```

- [ ] **Step 4: Create `YierdisDbComponents`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponents.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.ops.DbLifecycleOps;
import yier.bubu.redis.ops.DbReads;
import yier.bubu.redis.ops.DbWrites;
import yier.bubu.redis.ops.ExpirationManager;
import yier.bubu.redis.ops.MemoryOps;

final class YierdisDbComponents {
    final YierdisDbStorageComponents storage;
    final YierdisDbConfig config;
    final YierdisDbMemoryEstimator memoryEstimator;
    final YierdisDbMemoryLedger ledger;
    final YierdisDbMutationExecutor mutationExecutor;
    final YierdisDbExpirationSupport expirationSupport;
    final YierdisDbMaxmemorySupport maxmemorySupport;
    final YierdisDbKeyLifecycle keyLifecycle;
    final YierdisDbInternals internals;
    final YierdisStringOps stringOps;
    final YierdisHashOps hashOps;
    final YierdisListOps listOps;
    final YierdisSetOps setOps;
    final YierdisZSetOps zsetOps;
    final YierdisHllOps hllOps;
    final YierdisTtlOps ttlOps;
    final YierdisKeyspaceOps keyspaceOps;
    final YierdisDbMemoryReporter memoryReporter;
    final YierdisDbIntrospection introspection;
    final DbReads reads;
    final DbWrites writes;
    final ExpirationManager expirationManager;
    final MemoryOps memoryOps;
    final DbLifecycleOps lifecycleOps;

    YierdisDbComponents(
            YierdisDbStorageComponents storage,
            YierdisDbConfig config,
            YierdisDbMemoryEstimator memoryEstimator,
            YierdisDbMemoryLedger ledger,
            YierdisDbMutationExecutor mutationExecutor,
            YierdisDbExpirationSupport expirationSupport,
            YierdisDbMaxmemorySupport maxmemorySupport,
            YierdisDbKeyLifecycle keyLifecycle,
            YierdisDbInternals internals,
            YierdisStringOps stringOps,
            YierdisHashOps hashOps,
            YierdisListOps listOps,
            YierdisSetOps setOps,
            YierdisZSetOps zsetOps,
            YierdisHllOps hllOps,
            YierdisTtlOps ttlOps,
            YierdisKeyspaceOps keyspaceOps,
            YierdisDbMemoryReporter memoryReporter,
            YierdisDbIntrospection introspection,
            DbReads reads,
            DbWrites writes,
            ExpirationManager expirationManager,
            MemoryOps memoryOps,
            DbLifecycleOps lifecycleOps
    ) {
        this.storage = storage;
        this.config = config;
        this.memoryEstimator = memoryEstimator;
        this.ledger = ledger;
        this.mutationExecutor = mutationExecutor;
        this.expirationSupport = expirationSupport;
        this.maxmemorySupport = maxmemorySupport;
        this.keyLifecycle = keyLifecycle;
        this.internals = internals;
        this.stringOps = stringOps;
        this.hashOps = hashOps;
        this.listOps = listOps;
        this.setOps = setOps;
        this.zsetOps = zsetOps;
        this.hllOps = hllOps;
        this.ttlOps = ttlOps;
        this.keyspaceOps = keyspaceOps;
        this.memoryReporter = memoryReporter;
        this.introspection = introspection;
        this.reads = reads;
        this.writes = writes;
        this.expirationManager = expirationManager;
        this.memoryOps = memoryOps;
        this.lifecycleOps = lifecycleOps;
    }
}
```

- [ ] **Step 5: Create `YierdisDbComponentFactory`**

Create `yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponentFactory.java`:

```java
package yier.bubu.redis.db;

import yier.bubu.redis.db.memory.foreign.YierdisFfmMemoryRuntime;
import yier.bubu.redis.offheap.api.OffHeapAllocator;
import yier.bubu.redis.ops.MaxmemoryCoordinator;

final class YierdisDbComponentFactory {
    private YierdisDbComponentFactory() {
    }

    static YierdisDbComponents create(
            OwnerCallbacks owner,
            YierdisFfmMemoryRuntime memoryRuntime,
            OffHeapAllocator offHeapAllocator,
            boolean ownsOffHeapAllocator,
            boolean ownsMemoryRuntime,
            long maxmemoryBytes,
            String maxmemoryPolicy,
            int maxmemorySamples,
            long evictionTimeLimitMillis,
            long expireCleanupTimeLimitMillis
    ) {
        YierdisDbStorageComponents storage = YierdisDbStorageComponents.create(
                memoryRuntime,
                offHeapAllocator,
                ownsOffHeapAllocator,
                ownsMemoryRuntime
        );
        YierdisDbConfig config = YierdisDbConfig.create(
                maxmemoryBytes,
                maxmemoryPolicy,
                maxmemorySamples,
                evictionTimeLimitMillis,
                expireCleanupTimeLimitMillis
        );
        YierdisDbMemoryEstimator memoryEstimator = new YierdisDbMemoryEstimator(
                storage.keysStoredOffHeap,
                storage.offHeapAllocator
        );
        YierdisDbMemoryLedger ledger = new YierdisDbMemoryLedger(
                config.maxmemoryBytes,
                config.maxmemoryPolicy,
                owner::cleanupExpired,
                owner::evictUntilUnder,
                owner::usedBytesForMaxmemory,
                owner::maxmemoryCoordinator
        );
        YierdisDbMutationExecutor mutationExecutor = new YierdisDbMutationExecutor(owner::checkThread, ledger);
        YierdisDbExpirationSupport expirationSupport = new YierdisDbExpirationSupport(
                owner.db(),
                storage.keysStoredOffHeap,
                config.expireCleanupTimeLimitNanos
        );
        YierdisDbMaxmemorySupport maxmemorySupport = new YierdisDbMaxmemorySupport(
                owner.db(),
                config.maxmemoryPolicy,
                config.maxmemorySamples,
                config.evictionTimeLimitNanos
        );
        YierdisDbKeyLifecycle keyLifecycle = new YierdisDbKeyLifecycle(
                storage.store,
                storage.expires,
                storage.offHeapAllocator,
                storage.memoryRuntime,
                owner::touch,
                owner::adjustUsedBytes
        );
        YierdisDbInternals internals = new YierdisDbRuntimeInternals(
                owner::checkThread,
                mutationExecutor,
                keyLifecycle,
                ledger
        );
        YierdisStringOps stringOps = new YierdisStringOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisHashOps hashOps = new YierdisHashOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisListOps listOps = new YierdisListOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisSetOps setOps = new YierdisSetOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisZSetOps zsetOps = new YierdisZSetOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisHllOps hllOps = new YierdisHllOps(internals, memoryEstimator::estimateEntryBytes);
        YierdisTtlOps ttlOps = new YierdisTtlOps(internals);
        YierdisKeyspaceOps keyspaceOps = new YierdisKeyspaceOps(internals);
        YierdisDbMemoryReporter memoryReporter = new YierdisDbMemoryReporter(
                owner::checkThread,
                keyLifecycle,
                storage.store,
                storage.expires,
                config.maxmemoryBytes,
                storage.keysStoredOffHeap,
                ledger,
                () -> owner.maxmemoryCoordinator() == null
        );
        YierdisDbIntrospection introspection = new YierdisDbIntrospection(owner::checkThread, keyLifecycle);

        return new YierdisDbComponents(
                storage,
                config,
                memoryEstimator,
                ledger,
                mutationExecutor,
                expirationSupport,
                maxmemorySupport,
                keyLifecycle,
                internals,
                stringOps,
                hashOps,
                listOps,
                setOps,
                zsetOps,
                hllOps,
                ttlOps,
                keyspaceOps,
                memoryReporter,
                introspection,
                new YierdisDbReads(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps),
                new YierdisDbWrites(stringOps, hashOps, listOps, setOps, zsetOps, hllOps, keyspaceOps, ttlOps),
                new YierdisDbExpirationManager(expirationSupport),
                new YierdisDbMemoryOps(memoryReporter, introspection),
                new YierdisDbLifecycleOps(owner.db())
        );
    }

    interface OwnerCallbacks {
        YierdisDb db();

        void checkThread();

        void cleanupExpired();

        void evictUntilUnder(long limitBytes);

        long usedBytesForMaxmemory();

        MaxmemoryCoordinator maxmemoryCoordinator();

        void touch(YierdisObject object);

        void adjustUsedBytes(long deltaBytes);
    }
}
```

- [ ] **Step 6: Delegate `YierdisDb` construction to the factory**

In `YierdisDb`, replace the body of the main private constructor with component creation and field assignment:

```java
YierdisDbComponents components = YierdisDbComponentFactory.create(
        new YierdisDbComponentFactory.OwnerCallbacks() {
            @Override
            public YierdisDb db() {
                return YierdisDb.this;
            }

            @Override
            public void checkThread() {
                YierdisDb.this.checkThread();
            }

            @Override
            public void cleanupExpired() {
                YierdisDb.this.cleanupExpired();
            }

            @Override
            public void evictUntilUnder(long limitBytes) {
                YierdisDb.this.evictUntilUnder(limitBytes);
            }

            @Override
            public long usedBytesForMaxmemory() {
                return YierdisDb.this.usedBytesForMaxmemory();
            }

            @Override
            public MaxmemoryCoordinator maxmemoryCoordinator() {
                return YierdisDb.this.maxmemoryCoordinator;
            }

            @Override
            public void touch(YierdisObject object) {
                YierdisDb.this.touch(object);
            }

            @Override
            public void adjustUsedBytes(long deltaBytes) {
                YierdisDb.this.adjustUsedBytes(deltaBytes);
            }
        },
        memoryRuntime,
        offHeapAllocator,
        ownsOffHeapAllocator,
        ownsMemoryRuntime,
        maxmemoryBytes,
        maxmemoryPolicy,
        maxmemorySamples,
        evictionTimeLimitMillis,
        expireCleanupTimeLimitMillis
);

this.memoryRuntime = components.storage.memoryRuntime;
this.offHeapAllocator = components.storage.offHeapAllocator;
this.resources = components.storage.resources;
this.store = components.storage.store;
this.expires = components.storage.expires;
this.keysStoredOffHeap = components.storage.keysStoredOffHeap;
this.maxmemoryBytes = components.config.maxmemoryBytes;
this.maxmemoryPolicy = components.config.maxmemoryPolicy;
this.maxmemorySamples = components.config.maxmemorySamples;
this.lruEnabled = components.config.lruEnabled;
this.evictionTimeLimitNanos = components.config.evictionTimeLimitNanos;
this.expireCleanupTimeLimitNanos = components.config.expireCleanupTimeLimitNanos;
this.memoryEstimator = components.memoryEstimator;
this.ledger = components.ledger;
this.mutationExecutor = components.mutationExecutor;
this.expirationSupport = components.expirationSupport;
this.maxmemorySupport = components.maxmemorySupport;
this.keyLifecycle = components.keyLifecycle;
this.internals = components.internals;
this.stringOps = components.stringOps;
this.hashOps = components.hashOps;
this.listOps = components.listOps;
this.setOps = components.setOps;
this.zsetOps = components.zsetOps;
this.hllOps = components.hllOps;
this.ttlOps = components.ttlOps;
this.keyspaceOps = components.keyspaceOps;
this.memoryReporter = components.memoryReporter;
this.introspection = components.introspection;
this.reads = components.reads;
this.writes = components.writes;
this.expirationManager = components.expirationManager;
this.memoryOps = components.memoryOps;
this.lifecycleOps = components.lifecycleOps;
```

Delete the private `DbInternals` inner class from the bottom of `YierdisDb`.

Remove these imports from `YierdisDb` if they are now unused:

```java
import yier.bubu.redis.db.memory.ffm.YierdisFfmBlobStore;
import yier.bubu.redis.db.memory.ffm.YierdisFfmExpireIndex;
import yier.bubu.redis.db.memory.ffm.YierdisFfmKeyspace;
import yier.bubu.redis.db.memory.foreign.YierdisForeignOffHeapAllocator;
import yier.bubu.redis.db.memory.MemoryLedger;
```

- [ ] **Step 7: Run focused verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbConstructionTest,OffHeapKeysToggleTest,UnsafeOffHeapDbSmokeTest,UnsafeOffHeapKeyspaceTest test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=OffHeapStringStorageTest,OffHeapLeakRegressionTest test
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=ExpireIndexTest,ExpireKeySharingTest,OffHeapBytesViewTtlRegressionTest test
```

Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbStorageComponents.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponents.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbRuntimeInternals.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbComponentFactory.java \
  yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java
git commit -m "refactor: extract yierdis db component factory"
```

## Task 5: Harden Architecture Guards And Update DB Internals Docs

**Files:**
- Modify: `yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java`
- Modify: `docs/db-internals.md`

- [ ] **Step 1: Add architecture guard checks**

In `YierdisDbArchitectureGuardTest`, add this test method:

```java
@Test
public void yierdisDbMustNotOwnExtractedConstructionMatchingOrEstimationDetails() throws IOException {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("unable to resolve repository root", repoRoot);

    Path dbFile = repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java");
    Assert.assertTrue("missing YierdisDb.java", Files.isRegularFile(dbFile));

    List<String> offenders = new ArrayList<>();
    scanFileForForbiddenText(
            repoRoot,
            dbFile,
            offenders,
            "parseMaxmemoryPolicy(",
            "estimateEntryBytes(",
            "estimateValueBytes(",
            "estimateStringWriteUpperBound(",
            "estimateCollectionWriteUpperBound(",
            "estimateSetWriteUpperBound(",
            "estimateZSetWriteUpperBound(",
            "sumByteLengths(",
            "globMatches(",
            "findGlobClassEnd(",
            "globClassMatches(",
            "new YierdisFfmBlobStore(",
            "new YierdisFfmKeyspace<>(",
            "new YierdisFfmExpireIndex("
    );

    if (!offenders.isEmpty()) {
        Assert.fail(
                "YierdisDb still owns extracted construction/matching/estimation details:\n"
                        + String.join("\n", offenders)
        );
    }
}
```

Add a second guard to ensure keyspace matching points at the extracted matcher:

```java
@Test
public void keyspaceOpsMustUseExtractedGlobMatcher() throws IOException {
    Path repoRoot = resolveRepoRoot();
    Assert.assertNotNull("unable to resolve repository root", repoRoot);

    Path keyspaceOpsFile = repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisKeyspaceOps.java");
    Assert.assertTrue("missing YierdisKeyspaceOps.java", Files.isRegularFile(keyspaceOpsFile));

    String source = Files.readString(keyspaceOpsFile, StandardCharsets.UTF_8);
    Assert.assertFalse("YierdisKeyspaceOps must not call YierdisDb.globMatches", source.contains("YierdisDb.globMatches"));
    Assert.assertTrue("YierdisKeyspaceOps must use YierdisGlobMatcher", source.contains("YierdisGlobMatcher.matches"));
}
```

- [ ] **Step 2: Run architecture guard**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test
```

Expected: PASS if Tasks 1-4 were completed; FAIL with explicit offenders if any extracted responsibility remains in `YierdisDb`.

- [ ] **Step 3: Update `docs/db-internals.md`**

In the section currently titled ``## `YierdisDb` 拥有什么``, replace the opening sentence:

```markdown
从构造函数看，`YierdisDb` 长期持有下面这些核心对象。
```

with:

```markdown
`YierdisDb` 仍然长期持有单 DB 运行所需的核心对象，但这些对象的创建不再集中在
`YierdisDb` 构造函数里。构造细节由 `YierdisDbConfig`、
`YierdisDbStorageComponents`、`YierdisDbComponents` 和
`YierdisDbComponentFactory` 收敛。
```

After the facade bullet list, add:

```markdown
### 7. 构造和纯工具类

为了避免 `YierdisDb` 再次变成所有细节的落点，几个非 facade 职责被放在独立类里：

- `YierdisDbConfig`
  负责校验构造参数、解析本地 maxmemory policy、计算时间预算和 LRU 开关。
- `YierdisDbStorageComponents`
  负责 FFM runtime、allocator、keyspace、expire index 和 owned resources 的组装结果。
- `YierdisDbComponentFactory`
  负责把 storage、ledger、mutation executor、key lifecycle、ops 和 facade 拼成对象图。
- `YierdisDbMemoryEstimator`
  负责 entry 估算和写入上界估算。
- `YierdisGlobMatcher`
  负责 `KEYS` / `SCAN` 使用的 Redis 风格 byte glob 匹配。

这几个类的共同点是：它们是 DB 包内部实现细节，不扩大 command 层或 runtime 层能看到的 API。
```

In the text diagram under `可以把 YierdisDb 想成下面这张图`, replace:

```text
YierdisDb
  -> store(key -> YierdisObject)
```

with:

```text
YierdisDb
  -> components/config/factory assemble object graph
  -> store(key -> YierdisObject)
```

- [ ] **Step 4: Run guard and docs-adjacent verification**

Run:

```bash
jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest,ArchitectureBoundaryTest,DbEngineReadWriteBoundaryTest test
```

Expected: PASS.

- [ ] **Step 5: Run full verification**

Run:

```bash
jdk25 mvn test
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add yierdis-core/yierdis-core-runtime/src/test/java/yier/bubu/redis/db/YierdisDbArchitectureGuardTest.java \
  docs/db-internals.md
git commit -m "test: guard yierdis db decomposition boundaries"
```

## Final Review Checklist

- [ ] `rg -n "globMatches|findGlobClassEnd|globClassMatches" yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java` prints no matches.
- [ ] `rg -n "estimateEntryBytes|estimateValueBytes|estimateStringWriteUpperBound|estimateSetWriteUpperBound|estimateZSetWriteUpperBound|sumByteLengths" yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java` prints no matches.
- [ ] `rg -n "new YierdisFfmBlobStore|new YierdisFfmKeyspace|new YierdisFfmExpireIndex" yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java` prints no matches.
- [ ] `rg -n "YierdisDb.globMatches" yierdis-core/yierdis-core-db/src/main/java/yier/bubu/redis/db` prints no matches.
- [ ] `jdk25 mvn -pl yierdis-core/yierdis-core-runtime -Dtest=YierdisDbArchitectureGuardTest test` passes.
- [ ] `jdk25 mvn test` passes.
