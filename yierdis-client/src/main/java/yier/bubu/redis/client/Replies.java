package yier.bubu.redis.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 把已经读成 Java 值的回复收成类型化方法声明的类型。形状对不上抛 {@link DecodeException}，连接保持可用。
 * <p>
 * 只有 {@code Connection.complete} 是先经 {@code command} 再在这里解码。
 * 管道在 {@code readCommandReply} 之后解码，事务在 {@code EXEC} 时解码。
 */
final class Replies {
    private Replies() {
    }

    static String text(Object reply) {
        if (reply instanceof String text) {
            return text;
        }
        throw wrongShape("string", reply);
    }

    static String textOrNull(Object reply) {
        if (reply == null || reply instanceof String) {
            return (String) reply;
        }
        throw wrongShape("string or null", reply);
    }

    static long integer(Object reply) {
        if (reply instanceof Long value) {
            return value;
        }
        throw wrongShape("integer", reply);
    }

    static Long integerOrNull(Object reply) {
        if (reply == null) {
            return null;
        }
        return integer(reply);
    }

    static List<String> strings(Object reply) {
        List<?> elements = list(reply, "string array");
        ArrayList<String> values = new ArrayList<>(elements.size());
        for (Object element : elements) {
            values.add(text(element));
        }
        return values;
    }

    static List<String> stringsOrNull(Object reply) {
        if (reply == null) {
            return null;
        }
        return strings(reply);
    }

    static Set<String> orderedSet(Object reply) {
        return new LinkedHashSet<>(strings(reply));
    }

    static Map<String, String> stringMap(Object reply) {
        List<HashEntry> entries = hashEntries(reply);
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        for (HashEntry entry : entries) {
            map.put(entry.field(), entry.value());
        }
        return map;
    }

    /**
     * RESP2 的 map 在这条连接上是扁平数组，字段和值交替出现。数值保持 {@code Long}，文本保持 {@code String}。
     */
    static Map<String, Object> fieldMap(Object reply) {
        List<?> elements = list(reply, "field/value array");
        if ((elements.size() & 1) != 0) {
            throw wrongShape("field/value pairs", reply);
        }
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        for (int index = 0; index < elements.size(); index += 2) {
            String field = text(elements.get(index));
            Object value = elements.get(index + 1);
            if (!(value instanceof String) && !(value instanceof Long)) {
                throw wrongShape("string or integer", value);
            }
            map.put(field, value);
        }
        return map;
    }

    static Map<String, Long> longMap(Object reply) {
        LinkedHashMap<String, Long> map = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : fieldMap(reply).entrySet()) {
            if (!(entry.getValue() instanceof Long value)) {
                throw wrongShape("integer", entry.getValue());
            }
            map.put(entry.getKey(), value);
        }
        return map;
    }

    static List<HashEntry> hashEntries(Object reply) {
        return pairs(reply, HashEntry::new);
    }

    static List<ScoredMember> scoredMembers(Object reply) {
        return pairs(reply, ScoredMember::new);
    }

    static KeyScan keyScan(Object reply) {
        List<?> row = scanRow(reply);
        return new KeyScan(text(row.get(0)), strings(row.get(1)));
    }

    static HashScan hashScan(Object reply) {
        List<?> row = scanRow(reply);
        return new HashScan(text(row.get(0)), hashEntries(row.get(1)));
    }

    static HashFieldScan hashFieldScan(Object reply) {
        List<?> row = scanRow(reply);
        return new HashFieldScan(text(row.get(0)), strings(row.get(1)));
    }

    static SetScan setScan(Object reply) {
        List<?> row = scanRow(reply);
        return new SetScan(text(row.get(0)), orderedSet(row.get(1)));
    }

    static ZScan zscan(Object reply) {
        List<?> row = scanRow(reply);
        return new ZScan(text(row.get(0)), scoredMembers(row.get(1)));
    }

    static List<CommandInfo> commandInfos(Object reply) {
        List<?> rows = list(reply, "command info array");
        ArrayList<CommandInfo> infos = new ArrayList<>(rows.size());
        for (Object row : rows) {
            infos.add(commandInfo(row));
        }
        return infos;
    }

    private static CommandInfo commandInfo(Object row) {
        if (row == null) {
            return null;
        }
        List<?> fields = list(row, "command info entry");
        if (fields.size() != 6) {
            throw wrongShape("command info entry", row);
        }
        return new CommandInfo(
                text(fields.get(0)),
                integer(fields.get(1)),
                strings(fields.get(2)),
                integer(fields.get(3)),
                integer(fields.get(4)),
                integer(fields.get(5))
        );
    }

    private static List<?> scanRow(Object reply) {
        List<?> row = list(reply, "scan array");
        if (row.size() != 2) {
            throw wrongShape("cursor and elements", reply);
        }
        return row;
    }

    private static <T> List<T> pairs(Object reply, PairFactory<T> factory) {
        List<?> elements = list(reply, "pair array");
        if ((elements.size() & 1) != 0) {
            throw wrongShape("pairs", reply);
        }
        ArrayList<T> pairs = new ArrayList<>(elements.size() / 2);
        for (int index = 0; index < elements.size(); index += 2) {
            pairs.add(factory.create(text(elements.get(index)), text(elements.get(index + 1))));
        }
        return pairs;
    }

    private static List<?> list(Object reply, String expected) {
        if (reply instanceof List<?> elements) {
            return elements;
        }
        throw wrongShape(expected, reply);
    }

    private static DecodeException wrongShape(String expected, Object reply) {
        String actual = reply == null ? "null" : reply.getClass().getSimpleName();
        return new DecodeException("reply was " + actual + ", expected " + expected, null);
    }

    @FunctionalInterface
    private interface PairFactory<T> {
        T create(String left, String right);
    }
}
