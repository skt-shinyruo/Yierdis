package yier.bubu.redis.db;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.db.key.KeyHandle;
import yier.bubu.redis.ops.ExpireOption;
import yier.bubu.redis.ops.ScanCursorV2;
import yier.bubu.redis.ops.SetMode;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

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
    public void yierdisDbMustDelegateResourceLifetimeAndLedgerImplementation() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("unable to resolve repository root", repoRoot);

        Path dbFile = repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java");
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

    private static Method findDeclaredMethod(Class<?> type, String name, Class<?>... parameterTypes) {
        try {
            return type.getDeclaredMethod(name, parameterTypes);
        } catch (NoSuchMethodException ignored) {
            return null;
        }
    }

    private static Path resolveRepoRoot() {
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path cursor = cwd;
        while (cursor != null) {
            if (Files.isDirectory(cursor.resolve("yierdis-core/yierdis-core-db"))
                    && Files.isDirectory(cursor.resolve("yierdis-core/yierdis-core-runtime"))) {
                return cursor.resolve("yierdis-core").normalize();
            }
            if (Files.isDirectory(cursor.resolve("yierdis-core-db"))
                    && Files.isDirectory(cursor.resolve("yierdis-core-runtime"))) {
                return cursor.normalize();
            }
            cursor = cursor.getParent();
        }
        return null;
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
