package yier.bubu.redis.client;

import java.util.List;

/** ZSCAN 的 cursor，以及成员和分数文本。 */
public record ZScan(String cursor, List<ScoredMember> entries) {
}