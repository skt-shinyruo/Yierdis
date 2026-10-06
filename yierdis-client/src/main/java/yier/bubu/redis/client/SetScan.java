package yier.bubu.redis.client;

import java.util.Set;

/** SSCAN 的 cursor，以及按回复顺序保留的成员。 */
public record SetScan(String cursor, Set<String> members) {
}
