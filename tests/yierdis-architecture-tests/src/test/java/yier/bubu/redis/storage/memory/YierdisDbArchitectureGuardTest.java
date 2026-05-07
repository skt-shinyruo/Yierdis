package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.key.KeyHandle;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.memory.internal.value.YierdisObject;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class YierdisDbArchitectureGuardTest {
    @Test
    public void yierdisDbMustNotOwnExtractedStringTtlAndKeyspaceMethods() {
        Assert.assertNull(findDeclaredMethod(
                YierdisDb.class,
                "setString",
                byte[].class,
                byte[].class,
                SetMode.class,
                ExpireOption.class
        ));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "getStringBytes", byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "expire", BytesView.class, long.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "keys", byte[].class, int.class, long.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "hset", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "hget", byte[].class, byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "lpush", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "lrange", byte[].class, int.class, int.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "sadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "smembers", byte[].class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "zadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "zrange", byte[].class, long.class, long.class, boolean.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "pfadd", byte[].class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "pfcount", java.util.List.class));
    }

    @Test
    public void yierdisDbInternalsMustNotExposeRawContainersOrMemoryRuntime() {
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "store"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "expires"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "offHeapAllocator"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "memoryRuntime"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "adjustUsedBytes"));
        Assert.assertNull(findDeclaredMethod(YierdisDbInternals.class, "refreshEstimatedBytes", KeyHandle.class, YierdisObject.class));
    }

    @Test
    public void yierdisDbMustNotOwnMemoryReportingOrSnapshotMethods() {
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "memoryStats"));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "memoryUsage", BytesView.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "snapshot", ScanCursorV2.class, int.class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "estimateListWriteUpperBound", int.class, java.util.List.class));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "estimateHashWriteUpperBound", int.class, java.util.List.class));
    }

    @Test
    public void maxmemoryPolicyMustUseCoreApiEnumOnly() {
        Assert.assertNull(findDeclaredClass(YierdisDb.class, "MaxmemoryPolicy"));
        Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDb.class, "maxmemoryPolicy"));
        Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDbConfig.class, "maxmemoryPolicy"));
        Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDbMemoryLedger.class, "maxmemoryPolicy"));
        Assert.assertEquals(MaxmemoryPolicy.class, fieldType(YierdisDbMaxmemorySupport.class, "maxmemoryPolicy"));
        Assert.assertNull(findDeclaredMethod(YierdisDb.class, "parse" + "MaxmemoryPolicy", String.class));
    }

    @Test
    public void yierdisDbMustDelegateResourceLifetimeAndLedgerImplementation() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path dbFile = storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisDb.java");
        Assert.assertTrue("missing YierdisDb.java", Files.isRegularFile(dbFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                dbFile,
                offenders,
                "private final boolean ownsOffHeapAllocator;",
                "private final boolean ownsMemoryRuntime;",
                "private final class DbMemoryLedger",
                "offHeapAllocator.close();",
                "memoryRuntime.close();"
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "YierdisDb still owns resource flags or ledger implementation details directly:\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void yierdisDbMustNotOwnExtractedConstructionMatchingOrEstimationDetails() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path dbFile = storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisDb.java");
        Assert.assertTrue("missing YierdisDb.java", Files.isRegularFile(dbFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                dbFile,
                offenders,
                "parse" + "MaxmemoryPolicy(",
                "estimateEntryBytes(",
                "estimateValueBytes(",
                "estimateStringWriteUpperBound(",
                "estimateCollectionWriteUpperBound(",
                "estimateSetWriteUpperBound(",
                "estimateZSetWriteUpperBound(",
                "sumByteLengths(",
                "sumZSetMemberByteLengths(",
                "globMatches(",
                "findGlobClassEnd(",
                "globClassMatches(",
                "new YierdisFfmMemoryRuntime(",
                "new YierdisFfmBlobStore(",
                "new YierdisFfmKeyspace",
                "new YierdisFfmExpireIndex(",
                "new YierdisStringOps(",
                "new YierdisHashOps(",
                "new YierdisListOps(",
                "new YierdisSetOps(",
                "new YierdisZSetOps(",
                "new YierdisHllOps(",
                "new YierdisTtlOps(",
                "new YierdisKeyspaceOps(",
                "new YierdisDbReads(",
                "new YierdisDbWrites(",
                "new YierdisDbExpirationManager(",
                "new YierdisDbMemoryOps(",
                "new YierdisDbLifecycleOps(",
                "new YierdisDbMemoryReporter(",
                "new YierdisDbIntrospection(",
                "new YierdisDbMemoryLedger(",
                "new YierdisDbMutationExecutor(",
                "new YierdisDbExpirationSupport(",
                "new YierdisDbMaxmemorySupport(",
                "new YierdisDbKeyLifecycle(",
                "new YierdisDbMemoryEstimator("
        );

        String source = Files.readString(dbFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "YierdisDb must keep delegating object graph assembly to YierdisDbComponentFactory.create(",
                stripJavaCommentsAndLiterals(source).contains("YierdisDbComponentFactory.create(")
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "YierdisDb still owns extracted construction/matching/estimation details:\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void keyspaceOpsMustUseExtractedGlobMatcher() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path keyspaceOpsFile = storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisKeyspaceOps.java");
        Assert.assertTrue("missing YierdisKeyspaceOps.java", Files.isRegularFile(keyspaceOpsFile));

        String source = Files.readString(keyspaceOpsFile, StandardCharsets.UTF_8);
        String code = stripJavaCommentsAndLiterals(source);
        Pattern legacyMatcherCall = Pattern.compile("YierdisDb\\s*\\.\\s*globMatches\\s*\\(");
        Matcher legacyMatcher = legacyMatcherCall.matcher(code);
        Assert.assertFalse(
                "YierdisKeyspaceOps must not call YierdisDb.globMatches from executable code",
                legacyMatcher.find()
        );

        Pattern extractedMatcherCall = Pattern.compile("YierdisGlobMatcher\\s*\\.\\s*matches\\s*\\(");
        int extractedMatcherCalls = countMatches(extractedMatcherCall, code);
        Assert.assertTrue(
                "YierdisKeyspaceOps must use YierdisGlobMatcher.matches in both KEYS and SCAN paths; found "
                        + extractedMatcherCalls + " executable call site(s)",
                extractedMatcherCalls >= 2
        );
    }

    private static Method findDeclaredMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        }
    }

    private static Class<?> findDeclaredClass(Class<?> type, String simpleName) {
        for (Class<?> nested : type.getDeclaredClasses()) {
            if (nested.getSimpleName().equals(simpleName)) {
                return nested;
            }
        }
        return null;
    }

    private static Class<?> fieldType(Class<?> type, String fieldName) {
        try {
            java.lang.reflect.Field field = type.getDeclaredField(fieldName);
            return field.getType();
        } catch (NoSuchFieldException e) {
            Assert.fail("missing field " + type.getName() + "." + fieldName);
            return null;
        }
    }

    private static Path resolveRepoRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path cursor = cwd;
        while (cursor != null) {
            if (Files.isRegularFile(cursor.resolve("pom.xml"))
                    && Files.isRegularFile(cursor.resolve("libs/storage/pom.xml"))
                    && Files.isRegularFile(cursor.resolve("libs/runtime/pom.xml"))
                    && Files.isDirectory(cursor.resolve("libs/storage/yierdis-storage-memory/src/main/java"))
                    && Files.isDirectory(cursor.resolve("libs/runtime/yierdis-runtime-embedded"))) {
                return cursor.normalize();
            }
            cursor = cursor.getParent();
        }
        return null;
    }

    private static Path storageMemoryMain(Path repoRoot) {
        return repoRoot.resolve("libs/storage/yierdis-storage-memory/src/main/java").normalize();
    }

    private static void scanFileForForbiddenText(Path workspaceRoot, Path file, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        if (!Files.isRegularFile(file)) {
            offenders.add(relativePath(workspaceRoot, file) + " (missing file)");
            return;
        }
        String content = Files.readString(file, StandardCharsets.UTF_8);
        String rel = relativePath(workspaceRoot, file);
        for (String forbiddenText : forbiddenTexts) {
            if (forbiddenText != null && !forbiddenText.isBlank() && content.contains(forbiddenText)) {
                offenders.add(rel + " contains forbidden text: " + forbiddenText);
            }
        }
    }

    private static int countMatches(Pattern pattern, String source) {
        Matcher matcher = pattern.matcher(source);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    private static String stripJavaCommentsAndLiterals(String source) {
        StringBuilder out = new StringBuilder(source.length());
        boolean inLineComment = false;
        boolean inBlockComment = false;
        boolean inString = false;
        boolean inTextBlock = false;
        boolean inChar = false;
        boolean escaped = false;

        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            char next = i + 1 < source.length() ? source.charAt(i + 1) : '\0';
            char next2 = i + 2 < source.length() ? source.charAt(i + 2) : '\0';

            if (inLineComment) {
                if (c == '\n') {
                    inLineComment = false;
                    out.append(c);
                } else {
                    out.append(' ');
                }
                continue;
            }
            if (inBlockComment) {
                if (c == '*' && next == '/') {
                    out.append("  ");
                    i++;
                    inBlockComment = false;
                } else {
                    out.append(c == '\n' ? '\n' : ' ');
                }
                continue;
            }
            if (inTextBlock) {
                if (c == '"' && next == '"' && next2 == '"') {
                    out.append("   ");
                    i += 2;
                    inTextBlock = false;
                } else {
                    out.append(c == '\n' ? '\n' : ' ');
                }
                continue;
            }
            if (inString) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '"') {
                    inString = false;
                }
                out.append(c == '\n' ? '\n' : ' ');
                continue;
            }
            if (inChar) {
                if (escaped) {
                    escaped = false;
                } else if (c == '\\') {
                    escaped = true;
                } else if (c == '\'') {
                    inChar = false;
                }
                out.append(c == '\n' ? '\n' : ' ');
                continue;
            }

            if (c == '/' && next == '/') {
                out.append("  ");
                i++;
                inLineComment = true;
            } else if (c == '/' && next == '*') {
                out.append("  ");
                i++;
                inBlockComment = true;
            } else if (c == '"' && next == '"' && next2 == '"') {
                out.append("   ");
                i += 2;
                inTextBlock = true;
            } else if (c == '"') {
                out.append(' ');
                inString = true;
                escaped = false;
            } else if (c == '\'') {
                out.append(' ');
                inChar = true;
                escaped = false;
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }

    private static String relativePath(Path root, Path path) {
        if (root == null || path == null) {
            return String.valueOf(path);
        }
        try {
            return root.toAbsolutePath().normalize().relativize(path.toAbsolutePath().normalize()).toString();
        } catch (Exception ignored) {
            return path.toString();
        }
    }
}
