package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.storage.memory.internal.ledger.YierdisDbMemoryLedger;
import yier.bubu.redis.storage.api.ExpireOption;
import yier.bubu.redis.storage.api.MaxmemoryPolicy;
import yier.bubu.redis.storage.api.ScanCursorV2;
import yier.bubu.redis.storage.api.SetMode;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;

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
        Assert.assertNull(findDeclaredMethodByName(YierdisDbInternals.class, "refreshEstimatedBytes"));
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
        Assert.assertNull(
                "YierdisDb should not own maxmemoryPolicy directly; configuration/participants own that state",
                fieldTypeOrNull(YierdisDb.class, "maxmemoryPolicy")
        );
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
                "private final boolean owns" + "Off" + "HeapAllocator;",
                "private final boolean ownsMemoryRuntime;",
                "private final class DbMemoryLedger",
                "off" + "HeapAllocator.close();",
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
                "new YierdisFfm" + "MemoryRuntime(",
                "new YierdisFfm" + "BlobStore(",
                "new YierdisFfm" + "Keyspace",
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
                "YierdisKeyspaceOps must use YierdisGlobMatcher.matches from its shared key-window matcher; found "
                        + extractedMatcherCalls + " executable call site(s)",
                extractedMatcherCalls >= 1
        );

        Pattern keyWindowMatcherCall = Pattern.compile(
                "matchesForWindow\\s*\\(\\s*globPattern\\s*,\\s*key\\s*,\\s*record\\s*,\\s*nowMillis\\s*\\)"
        );
        int keyWindowMatcherCalls = countMatches(keyWindowMatcherCall, code);
        Assert.assertTrue(
                "YierdisKeyspaceOps must route both KEYS and SCAN through its shared key-window matcher; found "
                        + keyWindowMatcherCalls + " executable call site(s)",
                keyWindowMatcherCalls >= 2
        );
    }

    @Test
    public void dbMemoryProductionMustNotReferenceYierdisObject() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path mainRoot = storageMemoryMain(repoRoot);
        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(repoRoot, mainRoot, offenders, "YierdisObject");

        Assert.assertTrue("expected to scan yierdis-db-memory production sources", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail("yierdis-db-memory production sources must not reference YierdisObject:\n"
                    + String.join("\n", offenders));
        }
    }

    @Test
    public void dbMemoryMainAndTestHaveNoFfmImports() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        for (Path root : List.of(storageMemoryMain(repoRoot), storageMemoryTest(repoRoot))) {
            scanned += scanForForbiddenText(
                    repoRoot,
                    root,
                    offenders,
                    "yier.bubu.redis.memory." + "foreign",
                    "java.lang." + "foreign"
            );
        }

        Assert.assertTrue("expected DB-memory Java sources", scanned > 0);
        Assert.assertTrue("DB-memory must not import FFM:\n" + String.join("\n", offenders),
                offenders.isEmpty());
    }

    @Test
    public void yierdisDbHasFactoryOnlyComposition() throws IOException {
        Assert.assertEquals(0, YierdisDb.class.getConstructors().length);
        Assert.assertFalse(Arrays.stream(YierdisDb.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().startsWith("createWith")));
        Constructor<?>[] constructors = YierdisDbEngineFactory.class.getConstructors();
        Assert.assertEquals(1, constructors.length);
        Assert.assertArrayEquals(
                new Class<?>[]{StableMemoryBackendFactory.class, YierdisDbBackendConfig.class},
                constructors[0].getParameterTypes()
        );
        Assert.assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "yier.bubu.redis.storage.memory.internal.ffm.YierdisFfm" + "IntSet"
        ));
        Assert.assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "yier.bubu.redis.storage.memory.internal.value.NativeRaw" + "HandleSet"
        ));

        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);
        Assert.assertFalse(Files.exists(storageMemoryTest(repoRoot).resolve(
                "yier/bubu/redis/storage/memory/internal/entry/RawPathRecording" + "Allocator.java"
        )));
    }

    @Test
    public void dbMemoryMustNotReferenceLegacyKeyspaceOrKeyHandleImplementations() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        for (Path root : List.of(storageMemoryMain(repoRoot), storageMemoryTest(repoRoot))) {
            scanned += scanForForbiddenText(
                    repoRoot,
                    root,
                    offenders,
                    "ByteArray" + "Keyspace",
                    "YierdisFfm" + "Keyspace",
                    "Heap" + "KeyHandle",
                    "Ffm" + "KeyHandle",
                    "KeyHandle.for" + "Heap",
                    "KeyHandle.for" + "Ffm"
            );
        }

        Assert.assertTrue("expected to scan yierdis-db-memory sources", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail("db-memory must not reference legacy keyspace/key handle implementations:\n"
                    + String.join("\n", offenders));
        }
    }

    @Test
    public void dbMemoryMustNotReferenceLegacyExpireIndexImplementations() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        for (Path root : List.of(storageMemoryMain(repoRoot), storageMemoryTest(repoRoot))) {
            scanned += scanForForbiddenText(
                    repoRoot,
                    root,
                    offenders,
                    "Yierdis" + "HeapExpireIndex",
                    "YierdisFfmExpireIndex(YierdisFfm" + "BlobStore",
                    "Yierdis" + "NativeExpireIndex",
                    "Yierdis" + "ExpireIndex",
                    "Prepared" + "TtlMutation"
            );
        }

        Assert.assertTrue("expected to scan yierdis-db-memory sources", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail("db-memory must not reference legacy expire index implementations:\n"
                    + String.join("\n", offenders));
        }
    }

    @Test
    public void dbMemoryProductionMustBeNativeHandleOnlyStorage() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path mainRoot = storageMemoryMain(repoRoot);
        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                mainRoot,
                offenders,
                "Off" + "HeapAllocator",
                "Off" + "HeapBuf",
                "Off" + "HeapSlice",
                "YierdisForeign" + "Off" + "HeapAllocator",
                "YierdisFfm" + "SlabAllocator",
                "Heap" + "KeyHandle",
                "Ffm" + "KeyHandle",
                "KeyHandle.for" + "Heap",
                "KeyHandle.for" + "Ffm",
                "ByteArray" + "Keyspace",
                "YierdisFfm" + "Keyspace",
                "Yierdis" + "HeapExpireIndex",
                "YierdisFfm" + "BytesRef",
                "YierdisFfm" + "BytesRefSlice",
                "YierdisFfm" + "BlobStore",
                "YierdisFfm" + "ByteMap",
                "YierdisFfm" + "Listpack",
                "ByteArray" + "HashMap",
                "ByteArray" + "HashSet"
        );

        Assert.assertTrue("expected to scan yierdis-db-memory production sources", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail("yierdis-db-memory production sources must not reference legacy storage symbols:\n"
                    + String.join("\n", offenders));
        }

        String productionSource = readAllJavaSources(mainRoot);
        for (String requiredText : List.of(
                "StableMemoryBackend",
                "NativeKeyDirectory",
                "NativeBytesSlice",
                "KeyHandle.forNative"
        )) {
            Assert.assertTrue(
                    "yierdis-db-memory production sources must contain native-only storage marker: " + requiredText,
                    productionSource.contains(requiredText)
            );
        }
    }

    private static Method findDeclaredMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        }
    }

    private static Method findDeclaredMethodByName(Class<?> type, String name) {
        for (Method method : type.getDeclaredMethods()) {
            if (method.getName().equals(name)) {
                return method;
            }
        }
        return null;
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

    private static Class<?> fieldTypeOrNull(Class<?> type, String fieldName) {
        try {
            java.lang.reflect.Field field = type.getDeclaredField(fieldName);
            return field.getType();
        } catch (NoSuchFieldException ignored) {
            return null;
        }
    }

    private static Path resolveRepoRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path cursor = cwd;
        while (cursor != null) {
            if (Files.isRegularFile(cursor.resolve("pom.xml"))
                    && Files.isRegularFile(cursor.resolve("yierdis-db/pom.xml"))
                    && Files.isRegularFile(cursor.resolve("yierdis-server/pom.xml"))
                    && Files.isDirectory(cursor.resolve("yierdis-db/yierdis-db-memory/src/main/java"))
                    && Files.isDirectory(cursor.resolve("yierdis-server/yierdis-server-runtime"))) {
                return cursor.normalize();
            }
            cursor = cursor.getParent();
        }
        return null;
    }

    private static Path storageMemoryMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-db/yierdis-db-memory/src/main/java").normalize();
    }

    private static Path storageMemoryTest(Path repoRoot) {
        return repoRoot.resolve("yierdis-db/yierdis-db-memory/src/test/java").normalize();
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

    private static int scanForForbiddenText(Path workspaceRoot, Path root, List<String> offenders, String forbiddenText)
            throws IOException {
        return scanForForbiddenText(workspaceRoot, root, offenders, new String[]{forbiddenText});
    }

    private static int scanForForbiddenText(Path workspaceRoot, Path root, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        if (!Files.isDirectory(root)) {
            offenders.add(relativePath(workspaceRoot, root) + " (missing directory)");
            return 0;
        }
        List<Path> javaFiles;
        try (java.util.stream.Stream<Path> files = Files.walk(root)) {
            javaFiles = files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .toList();
        }
        for (Path javaFile : javaFiles) {
            scanJavaFileForForbiddenText(workspaceRoot, javaFile, offenders, forbiddenTexts);
        }
        return javaFiles.size();
    }

    private static void scanJavaFileForForbiddenText(Path workspaceRoot, Path file, List<String> offenders, String... forbiddenTexts)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            for (String forbiddenText : forbiddenTexts) {
                if (lines.get(i).contains(forbiddenText)) {
                    offenders.add(relativePath(workspaceRoot, file) + ":" + (i + 1)
                            + " contains forbidden text: " + forbiddenText);
                }
            }
        }
    }

    private static String readAllJavaSources(Path root) throws IOException {
        if (!Files.isDirectory(root)) {
            return "";
        }
        StringBuilder out = new StringBuilder();
        List<Path> javaFiles;
        try (java.util.stream.Stream<Path> files = Files.walk(root)) {
            javaFiles = files
                    .filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .toList();
        }
        for (Path javaFile : javaFiles) {
            out.append(Files.readString(javaFile, StandardCharsets.UTF_8)).append('\n');
        }
        return out.toString();
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
