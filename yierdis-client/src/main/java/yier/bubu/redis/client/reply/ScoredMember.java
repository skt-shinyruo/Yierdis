package yier.bubu.redis.client.reply;

/**
 * 有序集合的成员和分数。分数是服务端回复里的文本，不解析成数字。
 */
public record ScoredMember(String member, String score) {
}
