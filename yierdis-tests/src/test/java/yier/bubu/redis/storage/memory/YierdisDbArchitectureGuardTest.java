package yier.bubu.redis.storage.memory;

import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.memory.api.StableMemoryBackendFactory;
import yier.bubu.redis.storage.memory.internal.entry.EntryTable;
import yier.bubu.redis.storage.memory.internal.entry.HashRoot;
import yier.bubu.redis.storage.memory.internal.entry.ListRoot;
import yier.bubu.redis.storage.memory.internal.entry.SetRoot;
import yier.bubu.redis.storage.memory.internal.entry.StringRoot;
import yier.bubu.redis.storage.memory.internal.entry.ZSetRoot;
import yier.bubu.redis.storage.memory.internal.keyspace.NativeKeyDirectory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YierdisDbArchitectureGuardTest {
    @Test
    public void dbMemorySourcesDoNotImportFfmImplementations() throws IOException {
        Path repoRoot = resolveRepoRoot();
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
    public void yierdisDbUsesFactoryOnlyComposition() {
        Assert.assertEquals(0, YierdisDb.class.getConstructors().length);
        Assert.assertFalse(Arrays.stream(YierdisDb.class.getDeclaredMethods())
                .anyMatch(method -> method.getName().startsWith("createWith")));

        Constructor<?>[] constructors = YierdisDbEngineFactory.class.getConstructors();
        Assert.assertEquals(1, constructors.length);
        Assert.assertArrayEquals(
                new Class<?>[]{StableMemoryBackendFactory.class, YierdisDbBackendConfig.class},
                constructors[0].getParameterTypes()
        );
    }

    @Test
    public void dbConstructionGraphDoesNotRegrowComponentBagsOrLateBinding() throws IOException {
        Path main = storageMemoryMain(resolveRepoRoot()).resolve("yier/bubu/redis/storage/memory");
        for (String removedType : List.of(
                "YierdisDbComponentFactory.java",
                "YierdisDbComponents.java",
                "YierdisDbStorageComponents.java"
        )) {
            Assert.assertFalse("removed construction type returned: " + removedType,
                    Files.exists(main.resolve(removedType)));
        }
        String runtimeState = Files.readString(main.resolve("YierdisDbRuntimeState.java"), StandardCharsets.UTF_8);
        Assert.assertFalse("runtime state must not late-bind the storage graph", runtimeState.contains("void bind("));
        Assert.assertFalse("runtime state must not retain the storage backend",
                runtimeState.contains("StableMemoryBackend"));
    }

    @Test
    public void contextualWritesDoNotRebuildRuntimeOrFamilyModules() throws IOException {
        Path main = storageMemoryMain(resolveRepoRoot()).resolve("yier/bubu/redis/storage/memory");
        String writes = Files.readString(main.resolve("YierdisDbWrites.java"), StandardCharsets.UTF_8);
        String internals = Files.readString(main.resolve("YierdisDbRuntimeInternals.java"), StandardCharsets.UTF_8);
        for (String family : List.of(
                "YierdisStringOps",
                "YierdisHashOps",
                "YierdisListOps",
                "YierdisSetOps",
                "YierdisZSetOps",
                "YierdisHllOps",
                "YierdisKeyspaceOps",
                "YierdisTtlOps"
        )) {
            Assert.assertFalse("context binding must reuse " + family, writes.contains("new " + family));
        }
        Assert.assertFalse("runtime internals must not retain request context",
                internals.contains("MutationContext mutationContext"));
        Assert.assertFalse("runtime internals must not be rebuilt for request context",
                internals.contains("withMutationContext("));
    }

    @Test
    public void keyLifecycleDoesNotExposeOwnedStorageComponents() throws IOException {
        Set<Class<?>> forbiddenReturnTypes = Set.of(
                StableMemoryBackend.class,
                EntryTable.class,
                NativeKeyDirectory.class,
                StringRoot.class,
                ListRoot.class,
                HashRoot.class,
                SetRoot.class,
                ZSetRoot.class
        );
        Arrays.stream(YierdisDbKeyLifecycle.class.getDeclaredMethods()).forEach(method -> {
            Class<?> leakedType = leakedStorageType(
                    method.getReturnType(),
                    forbiddenReturnTypes,
                    new HashSet<>()
            );
            Assert.assertNull(
                    "key lifecycle leaks " + simpleName(leakedType) + " via " + method.getName(),
                    leakedType
            );
        });
        Arrays.stream(YierdisDb.class.getDeclaredMethods()).forEach(method ->
                Assert.assertNotEquals(
                        "database facade must not expose its owned backend via " + method.getName(),
                        StableMemoryBackend.class,
                        method.getReturnType()
                )
        );

        Path packageRoot = storageMemoryMain(resolveRepoRoot()).resolve("yier/bubu/redis/storage/memory");
        for (String removedType : List.of(
                "EntryMutationEntries.java",
                "YierdisDbOwnedResources.java",
                "internal/keyspace/YierdisKeyspace.java"
        )) {
            Assert.assertFalse("removed lifecycle helper returned: " + removedType,
                    Files.exists(packageRoot.resolve(removedType)));
        }

        List<String> offenders = new ArrayList<>();
        scanForForbiddenText(
                resolveRepoRoot(),
                storageMemoryMain(resolveRepoRoot()),
                offenders,
                "inspectionForTesting"
        );
        Assert.assertTrue("production code must not declare or use lifecycle inspection:\n"
                        + String.join("\n", offenders),
                offenders.isEmpty());
    }

    private static Class<?> leakedStorageType(
            Class<?> type,
            Set<Class<?>> forbiddenTypes,
            Set<Class<?>> visited
    ) {
        if (forbiddenTypes.contains(type)) {
            return type;
        }
        if (type.isArray()) {
            return leakedStorageType(type.componentType(), forbiddenTypes, visited);
        }
        if (type.isPrimitive() || type == Void.TYPE || !visited.add(type)) {
            return null;
        }
        Package typePackage = type.getPackage();
        if (typePackage == null || !typePackage.getName().startsWith("yier.bubu.redis.storage.memory")) {
            return null;
        }
        for (Method accessor : type.getDeclaredMethods()) {
            int modifiers = accessor.getModifiers();
            if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)
                    || accessor.getParameterCount() != 0) {
                continue;
            }
            Class<?> leakedType = leakedStorageType(accessor.getReturnType(), forbiddenTypes, visited);
            if (leakedType != null) {
                return leakedType;
            }
        }
        return null;
    }

    private static String simpleName(Class<?> type) {
        return type == null ? "storage component" : type.getSimpleName();
    }

    private static Path resolveRepoRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isRegularFile(current.resolve("yierdis-tests/pom.xml"))
                    && Files.isRegularFile(current.resolve("yierdis-db/yierdis-db-memory/pom.xml"))
                    && Files.isDirectory(current.resolve("yierdis-server/yierdis-server-runtime"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new AssertionError("unable to locate repository root");
    }

    private static Path storageMemoryMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-db/yierdis-db-memory/src/main/java");
    }

    private static Path storageMemoryTest(Path repoRoot) {
        return repoRoot.resolve("yierdis-db/yierdis-db-memory/src/test/java");
    }

    private static int scanForForbiddenText(
            Path workspaceRoot,
            Path root,
            List<String> offenders,
            String... forbiddenTexts
    ) throws IOException {
        if (!Files.isDirectory(root)) {
            offenders.add(workspaceRoot.relativize(root) + " (missing directory)");
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
            List<String> lines = Files.readAllLines(javaFile, StandardCharsets.UTF_8);
            for (int lineIndex = 0; lineIndex < lines.size(); lineIndex++) {
                for (String forbiddenText : forbiddenTexts) {
                    if (lines.get(lineIndex).contains(forbiddenText)) {
                        offenders.add(workspaceRoot.relativize(javaFile) + ":" + (lineIndex + 1)
                                + " contains forbidden text: " + forbiddenText);
                    }
                }
            }
        }
        return javaFiles.size();
    }
}
