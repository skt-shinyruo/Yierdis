package yier.bubu.redis.integration.command;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.command.kernel.CommandRegistry;
import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static yier.bubu.redis.testutil.TestDbs.forEachDb;

public class OperationCoverageMatrixTest {
    private static final Path MATRIX = Path.of("docs/project-docs/operation-test-coverage-matrix.md");
    private static final Pattern COMMAND_HEADING = Pattern.compile("(?m)^### ([A-Z][A-Z0-9-]*)$");
    private static final Pattern STATUS_LINE = Pattern.compile(
            "^- \\*\\*(Command layer|DB API|Native internals)\\*\\*: `([^`]+)` - .+$"
    );
    private static final Pattern VARIANT_INVENTORY_ROW = Pattern.compile(
            "^\\| `([^`]+)` \\| (.+) \\|$"
    );
    private static final Pattern COMMAND_VARIANT_LINE = Pattern.compile(
            "(?m)^- \\*\\*Command variant\\*\\*: `([^`]+)` - `([^`]+)` - .+$"
    );
    private static final Pattern INVENTORY_STATUS_ROW = Pattern.compile(
            "^\\| (.+) \\| `([^`]+)` \\| (.+) \\|$"
    );
    private static final Set<String> VALID_STATUSES = Set.of(
            "covered",
            "covered-by-shared-test",
            "missing",
            "not-applicable"
    );
    private static final Set<String> EXPECTED_DB_API_METHODS = Set.of(
            "StringReadOps.getStringBytes",
            "StringReadOps.getStringValue",
            "StringReadOps.strlen",
            "StringReadOps.getBit",
            "StringReadOps.bitcount",
            "StringReadOps.bitcount(start,end)",
            "StringWriteOps.set",
            "StringWriteOps.setString(byte[])",
            "StringWriteOps.setString(BytesSlice)",
            "StringWriteOps.append",
            "StringWriteOps.setBit",
            "StringWriteOps.incrBy",
            "HashReadOps.hget",
            "HashReadOps.hgetall",
            "HashReadOps.hlen",
            "HashWriteOps.hset",
            "HashWriteOps.hdel",
            "ListReadOps.lrange",
            "ListWriteOps.lpush",
            "ListWriteOps.rpush",
            "ListWriteOps.lpop",
            "ListWriteOps.rpop",
            "SetReadOps.smembers",
            "SetReadOps.sismember",
            "SetReadOps.scard",
            "SetWriteOps.sadd",
            "SetWriteOps.srem",
            "ZSetReadOps.zrange",
            "ZSetReadOps.zrevrange",
            "ZSetReadOps.zrangeByScore",
            "ZSetReadOps.zrevrangeByScore",
            "ZSetWriteOps.zadd",
            "ZSetWriteOps.zremrangeByScore",
            "ZSetWriteOps.zremrangeByRank",
            "ZSetWriteOps.zrem",
            "HllReadOps.pfcount",
            "HllWriteOps.pfadd",
            "HllWriteOps.pfmerge",
            "KeyspaceReadOps.typeOf",
            "KeyspaceReadOps.existsKey",
            "KeyspaceReadOps.keys",
            "KeyspaceReadOps.scan",
            "KeyspaceWriteOps.del",
            "TtlReadOps.ttlSeconds",
            "TtlReadOps.ttlMillis",
            "TtlWriteOps.expire",
            "TtlWriteOps.pexpire",
            "TtlWriteOps.expireAtSeconds",
            "TtlWriteOps.expireAtMillis",
            "TtlWriteOps.persist",
            "DbLifecycleOps.flushDb",
            "MemoryOps.memoryUsage",
            "MemoryOps.memoryStats",
            "MemoryOps.objectEncoding",
            "ExpirationManager.cleanupExpired",
            "DbEngine.reads",
            "DbEngine.writes",
            "DbEngine.expiration",
            "DbEngine.memory",
            "DbEngine.lifecycle"
    );
    private static final Set<String> EXPECTED_NATIVE_INVENTORY_TERMS = Set.of(
            "EntryRecord",
            "EntryTable",
            "EntryHandle",
            "ValueHandle",
            "KeyHandle",
            "HeapKeyHandle",
            "FfmKeyHandle",
            "NativeKeyDirectory",
            "YierdisFfmBlobStore",
            "YierdisFfmKeyspace",
            "ByteArrayKeyspace",
            "StringRoot",
            "ListRoot",
            "HashRoot",
            "SetRoot",
            "ZSetRoot",
            "ListValue",
            "HashValue",
            "SetValue",
            "ZSetValue",
            "YierdisHyperLogLog",
            "YierdisExpireIndex",
            "YierdisHeapExpireIndex",
            "YierdisFfmExpireIndex",
            "YierdisDbMemoryLedger",
            "MemoryLedger",
            "InMemoryLedger",
            "YierdisDbMutationExecutor",
            "YierdisDbMemoryEstimator",
            "YierdisDbMemoryReporter",
            "YierdisDbIntrospection",
            "Maxmemory"
    );

    @Test
    public void matrixContainsEveryRegisteredDefaultCommand() {
        String matrix = readMatrix();
        Set<String> headings = commandHeadings(matrix);

        forEachDb(db -> {
            YierdisFastCommandProcessor processor = TestCommandProcessors.forDb(db);
            CommandRegistry registry = registryOf(processor);
            for (String upperName : registry.upperNamesSorted()) {
                Assert.assertTrue(
                        "missing matrix entry for registered command " + upperName,
                        headings.contains(upperName)
                );
            }
        });
    }

    @Test
    public void matrixRowsUseKnownStatusVocabulary() {
        List<String> invalid = invalidStatusLines(readMatrix());
        Assert.assertTrue("invalid matrix status lines: " + invalid, invalid.isEmpty());
    }

    @Test
    public void coveredCellsReferenceNamedTestMethods() {
        List<String> invalid = coveredStatusLinesWithoutMethodRefs(readMatrix());
        Assert.assertTrue("covered matrix cells must cite file#method references: " + invalid, invalid.isEmpty());
    }

    @Test
    public void commandOptionInventoryHasVariantCoverageRows() {
        String matrix = readMatrix();
        List<String> missing = missingCommandVariantRows(matrix);
        Assert.assertTrue("missing command variant coverage rows: " + missing, missing.isEmpty());
    }

    @Test
    public void dbApiInventoryHasRowsForEveryDesignedPublicApiMethod() {
        String matrix = readMatrix();
        Set<String> actual = inventoryRowNames(section(matrix, "## DB API Inventory", "## Native/Internal Inventory"));
        ArrayList<String> missing = new ArrayList<>();
        for (String expected : EXPECTED_DB_API_METHODS) {
            if (!actual.contains(expected)) {
                missing.add(expected);
            }
        }
        Assert.assertTrue("missing DB API inventory rows: " + missing, missing.isEmpty());
    }

    @Test
    public void nativeInventoryCoversEveryDesignedStructureAndResponsibility() {
        String matrix = readMatrix();
        String inventory = section(matrix, "## Native/Internal Inventory", "## Current Gap Queue");
        ArrayList<String> missing = new ArrayList<>();
        for (String expected : EXPECTED_NATIVE_INVENTORY_TERMS) {
            if (!inventory.contains(expected)) {
                missing.add(expected);
            }
        }
        Assert.assertTrue("missing native/internal inventory terms: " + missing, missing.isEmpty());
    }

    @Test
    public void inventoryTableRowsUseKnownStatusesAndNamedEvidence() {
        String matrix = readMatrix();
        List<String> invalid = invalidInventoryRows(
                section(matrix, "## DB API Inventory", "## Current Gap Queue")
        );
        Assert.assertTrue("invalid inventory table rows: " + invalid, invalid.isEmpty());
    }

    @Test
    public void stringAndBitmapRowsStartAsConcreteTemplate() {
        String matrix = readMatrix();

        assertRowStatus(matrix, "SET", "Command layer", "covered");
        assertRowStatus(matrix, "GET", "Command layer", "covered");
        assertRowStatus(matrix, "STRLEN", "Command layer", "covered");
        assertRowStatus(matrix, "APPEND", "Command layer", "covered");
        assertRowStatus(matrix, "SETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "GETBIT", "Command layer", "covered");
        assertRowStatus(matrix, "BITCOUNT", "Command layer", "covered");
        assertRowStatus(matrix, "INCR", "Command layer", "covered");
        assertRowStatus(matrix, "DECR", "Command layer", "covered");
    }

    private static String readMatrix() {
        Path matrix = findMatrix();
        Assert.assertTrue("missing coverage matrix file: " + MATRIX, Files.isRegularFile(matrix));
        try {
            return Files.readString(matrix, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("unable to read coverage matrix " + MATRIX, e);
        }
    }

    private static Path findMatrix() {
        Path cwd = Path.of("").toAbsolutePath();
        for (Path dir = cwd; dir != null; dir = dir.getParent()) {
            Path candidate = dir.resolve(MATRIX);
            if (Files.isRegularFile(candidate)) {
                return candidate;
            }
        }
        return MATRIX;
    }

    private static Set<String> commandHeadings(String matrix) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        Matcher matcher = COMMAND_HEADING.matcher(matrix);
        while (matcher.find()) {
            out.add(matcher.group(1));
        }
        return out;
    }

    private static List<String> invalidStatusLines(String matrix) {
        ArrayList<String> invalid = new ArrayList<>();
        String[] lines = matrix.split("\\R");
        for (String line : lines) {
            if (!line.startsWith("- **Command layer**:")
                    && !line.startsWith("- **DB API**:")
                    && !line.startsWith("- **Native internals**:")
                    && !line.startsWith("- **Command variant**:")) {
                continue;
            }

            Matcher statusMatcher = STATUS_LINE.matcher(line);
            if (statusMatcher.matches()) {
                if (!VALID_STATUSES.contains(statusMatcher.group(2))) {
                    invalid.add(line);
                }
                continue;
            }

            Matcher variantMatcher = COMMAND_VARIANT_LINE.matcher(line);
            if (!variantMatcher.matches() || !VALID_STATUSES.contains(variantMatcher.group(2))) {
                invalid.add(line);
            }
        }
        return invalid;
    }

    private static List<String> coveredStatusLinesWithoutMethodRefs(String matrix) {
        ArrayList<String> invalid = new ArrayList<>();
        String[] lines = matrix.split("\\R");
        for (String line : lines) {
            if (!line.startsWith("- **Command layer**:")
                    && !line.startsWith("- **DB API**:")
                    && !line.startsWith("- **Native internals**:")
                    && !line.startsWith("- **Command variant**:")) {
                continue;
            }

            String status;
            Matcher statusMatcher = STATUS_LINE.matcher(line);
            if (statusMatcher.matches()) {
                status = statusMatcher.group(2);
            } else {
                Matcher variantMatcher = COMMAND_VARIANT_LINE.matcher(line);
                if (!variantMatcher.matches()) {
                    continue;
                }
                status = variantMatcher.group(2);
            }
            if (("covered".equals(status) || "covered-by-shared-test".equals(status)) && !line.contains("#")) {
                invalid.add(line);
            }
        }
        return invalid;
    }

    private static List<String> missingCommandVariantRows(String matrix) {
        Set<String> expected = commandVariantInventory(matrix);
        Set<String> actual = commandVariantRows(matrix);
        ArrayList<String> missing = new ArrayList<>();
        for (String variant : expected) {
            if (!actual.contains(variant)) {
                missing.add(variant);
            }
        }
        return missing;
    }

    private static Set<String> commandVariantInventory(String matrix) {
        String inventory = section(matrix, "## Option And Subcommand Inventory", "## DB API Inventory");
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        for (String line : inventory.split("\\R")) {
            Matcher matcher = VARIANT_INVENTORY_ROW.matcher(line);
            if (!matcher.matches()) {
                continue;
            }
            String operation = matcher.group(1);
            for (String variant : matcher.group(2).split(",")) {
                variants.add(operation + " / " + stripBackticks(variant.trim()));
            }
        }
        return variants;
    }

    private static Set<String> commandVariantRows(String matrix) {
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        Matcher matcher = COMMAND_VARIANT_LINE.matcher(matrix);
        while (matcher.find()) {
            variants.add(matcher.group(1));
        }
        return variants;
    }

    private static Set<String> inventoryRowNames(String inventory) {
        LinkedHashSet<String> rows = new LinkedHashSet<>();
        for (String line : inventory.split("\\R")) {
            Matcher matcher = INVENTORY_STATUS_ROW.matcher(line);
            if (matcher.matches()) {
                rows.add(stripBackticks(matcher.group(1)).trim());
            }
        }
        return rows;
    }

    private static List<String> invalidInventoryRows(String inventory) {
        ArrayList<String> invalid = new ArrayList<>();
        for (String line : inventory.split("\\R")) {
            if (!line.startsWith("| ") || line.startsWith("| ---") || line.startsWith("| API method")
                    || line.startsWith("| Area")) {
                continue;
            }

            Matcher matcher = INVENTORY_STATUS_ROW.matcher(line);
            if (!matcher.matches()) {
                invalid.add(line);
                continue;
            }

            String status = matcher.group(2);
            String evidence = matcher.group(3);
            if (!VALID_STATUSES.contains(status)) {
                invalid.add(line);
            } else if (("covered".equals(status) || "covered-by-shared-test".equals(status)) && !evidence.contains("#")) {
                invalid.add(line);
            }
        }
        return invalid;
    }

    private static String section(String matrix, String startHeading, String endHeading) {
        int start = matrix.indexOf(startHeading);
        Assert.assertTrue("missing section " + startHeading, start >= 0);
        int bodyStart = start + startHeading.length();
        int end = matrix.indexOf(endHeading, bodyStart);
        Assert.assertTrue("missing section " + endHeading, end >= 0);
        return matrix.substring(bodyStart, end);
    }

    private static String stripBackticks(String value) {
        return value.replace("`", "");
    }

    private static void assertRowStatus(String matrix, String command, String layer, String expectedStatus) {
        Pattern sectionPattern = Pattern.compile(
                "(?ms)^### " + Pattern.quote(command) + "$(?<section>.*?)(?=^### |\\z)"
        );
        Matcher sectionMatcher = sectionPattern.matcher(matrix);
        Assert.assertTrue("missing matrix section for " + command, sectionMatcher.find());

        Pattern row = Pattern.compile(
                "(?m)^- \\*\\*" + Pattern.quote(layer) + "\\*\\*: `([^`]+)` - .+$"
        );
        Matcher matcher = row.matcher(sectionMatcher.group("section"));
        Assert.assertTrue("missing " + layer + " row for " + command, matcher.find());
        Assert.assertEquals(command + " " + layer, expectedStatus, matcher.group(1));
    }

    private static CommandRegistry registryOf(YierdisFastCommandProcessor processor) {
        try {
            Field field = YierdisFastCommandProcessor.class.getDeclaredField("registry");
            field.setAccessible(true);
            return (CommandRegistry) field.get(processor);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("unable to access command processor registry", e);
        }
    }
}
