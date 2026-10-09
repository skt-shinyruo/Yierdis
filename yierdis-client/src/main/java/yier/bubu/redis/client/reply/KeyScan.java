package yier.bubu.redis.client.reply;

import java.util.List;

/** SCAN 的 cursor，以及这一页的 key，顺序与回复一致。 */
public record KeyScan(String cursor, List<String> keys) {
}
