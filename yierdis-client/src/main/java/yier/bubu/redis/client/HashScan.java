package yier.bubu.redis.client;

import java.util.List;

/** HSCAN 的 cursor，以及 field/value。不含 NOVALUES。 */
public record HashScan(String cursor, List<HashEntry> entries) {
}
