package yier.bubu.redis.storage.memory.internal.value;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * 集合类命令（SADD/SREM/HDEL/ZREM）在 mutation 前统计候选元素时共用的“去重后按存在性计数”循环：
 * 同一候选只计一次，命中与否由 {@code membershipTest} 判定。
 */
final class DistinctCandidates {
    private DistinctCandidates() {
    }

    static int count(List<byte[]> candidates, Predicate<byte[]> membershipTest) {
        Objects.requireNonNull(candidates, "candidates");
        Objects.requireNonNull(membershipTest, "membershipTest");
        int count = 0;
        for (int index = 0; index < candidates.size(); index++) {
            byte[] candidate = candidates.get(index);
            Objects.requireNonNull(candidate, "candidate");
            if (appearedEarlier(candidates, index, candidate)) {
                continue;
            }
            if (membershipTest.test(candidate)) {
                count++;
            }
        }
        return count;
    }

    // ponytail: pairwise O(n²) 去重沿用被收敛的原实现；批量 SADD/ZREM 大列表成为热点时换成 hash-set 去重。
    private static boolean appearedEarlier(List<byte[]> candidates, int limit, byte[] candidate) {
        for (int index = 0; index < limit; index++) {
            if (Arrays.equals(candidates.get(index), candidate)) {
                return true;
            }
        }
        return false;
    }
}
