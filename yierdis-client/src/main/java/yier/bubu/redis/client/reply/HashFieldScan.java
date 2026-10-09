package yier.bubu.redis.client.reply;

import java.util.List;

/** HSCAN NOVALUES 的 cursor，以及这一页的 field。没有 value。 */
public record HashFieldScan(String cursor, List<String> fields) {
}
