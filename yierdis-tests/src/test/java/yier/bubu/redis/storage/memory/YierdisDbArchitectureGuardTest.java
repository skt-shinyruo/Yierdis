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
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

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
        Assert.assertFalse("runtime state must not late-bind the storage graph",
                Arrays.stream(YierdisDbRuntimeState.class.getDeclaredMethods())
                        .anyMatch(method -> method.getName().equals("bind")));
        assertClassSignatureDoesNotReference(
                YierdisDbRuntimeState.class,
                Set.of(StableMemoryBackend.class)
        );
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
        Set<Class<?>> forbiddenReturnTypes = forbiddenStorageTypes();
        Arrays.stream(YierdisDbKeyLifecycle.class.getDeclaredMethods())
                .filter(method -> !Modifier.isPrivate(method.getModifiers()))
                .forEach(method -> {
            Class<?> leakedType = leakedStorageType(method, forbiddenReturnTypes);
            Assert.assertNull(
                    "key lifecycle leaks " + simpleName(leakedType) + " via " + method.getName(),
                    leakedType
            );
        });
        assertVisibleFieldsDoNotExposeStorage(YierdisDbKeyLifecycle.class, forbiddenReturnTypes);
        Arrays.stream(YierdisDb.class.getDeclaredMethods())
                .filter(method -> !Modifier.isPrivate(method.getModifiers()))
                .forEach(method -> {
            Class<?> leakedType = leakedStorageType(method, forbiddenReturnTypes);
            Assert.assertNull(
                    "database facade leaks " + simpleName(leakedType) + " via " + method.getName(),
                    leakedType
            );
        });
        assertVisibleFieldsDoNotExposeStorage(YierdisDb.class, forbiddenReturnTypes);

        Path packageRoot = storageMemoryMain(resolveRepoRoot()).resolve("yier/bubu/redis/storage/memory");
        for (String removedType : List.of(
                "EntryMutationEntries.java",
                "YierdisDbOwnedResources.java",
                "internal/keyspace/YierdisKeyspace.java"
        )) {
            Assert.assertFalse("removed lifecycle helper returned: " + removedType,
                    Files.exists(packageRoot.resolve(removedType)));
        }

        Assert.assertFalse("production lifecycle must not expose a test inspection method",
                Arrays.stream(YierdisDbKeyLifecycle.class.getDeclaredMethods())
                        .anyMatch(method -> method.getName().equals("inspectionForTesting")));
    }

    @Test
    public void storageLeakDetectorTraversesGenericsWrappersAndCallbacks() throws Exception {
        Set<Class<?>> forbiddenTypes = forbiddenStorageTypes();
        for (String methodName : List.of("genericLeak", "wrappedLeak", "callbackLeak")) {
            Method method = LeakProbe.class.getDeclaredMethod(methodName);
            Assert.assertEquals(
                    StableMemoryBackend.class,
                    leakedStorageType(
                            method.getGenericReturnType(),
                            forbiddenTypes,
                            new HashSet<>(),
                            true
                    )
            );
        }
        Method parameterLeak = LeakProbe.class.getDeclaredMethod("parameterLeak", Consumer.class);
        Assert.assertEquals(
                StableMemoryBackend.class,
                leakedStorageType(parameterLeak, forbiddenTypes)
        );
        Method callbackParameterLeak = LeakProbe.class.getDeclaredMethod(
                "callbackParameterLeak",
                StorageCallback.class
        );
        Assert.assertEquals(
                StableMemoryBackend.class,
                leakedStorageType(callbackParameterLeak, forbiddenTypes)
        );
        Method constructionInput = LeakProbe.class.getDeclaredMethod(
                "constructionInput",
                StableMemoryBackend.class
        );
        Assert.assertNull(
                "static construction input must not be treated as an ownership leak",
                leakedStorageType(constructionInput, forbiddenTypes)
        );
        Method staticCallbackLeak = LeakProbe.class.getDeclaredMethod(
                "staticCallbackLeak",
                Consumer.class
        );
        Assert.assertEquals(
                StableMemoryBackend.class,
                leakedStorageType(staticCallbackLeak, forbiddenTypes)
        );
    }

    private static Class<?> leakedStorageType(Method method, Set<Class<?>> forbiddenTypes) {
        Class<?> leakedType = leakedStorageType(
                method.getGenericReturnType(),
                forbiddenTypes,
                new HashSet<>(),
                true
        );
        if (leakedType != null) {
            return leakedType;
        }
        Type[] parameterTypes = method.getGenericParameterTypes();
        if (!Modifier.isStatic(method.getModifiers())) {
            return leakedStorageType(parameterTypes, forbiddenTypes, new HashSet<>(), false);
        }
        for (Type parameterType : parameterTypes) {
            if (parameterType instanceof Class<?> rawType && forbiddenTypes.contains(rawType)) {
                continue;
            }
            leakedType = leakedStorageType(
                    parameterType,
                    forbiddenTypes,
                    new HashSet<>(),
                    false
            );
            if (leakedType != null) {
                return leakedType;
            }
        }
        return null;
    }

    private static void assertVisibleFieldsDoNotExposeStorage(
            Class<?> owner,
            Set<Class<?>> forbiddenTypes
    ) {
        for (Field field : owner.getDeclaredFields()) {
            int modifiers = field.getModifiers();
            if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)) {
                continue;
            }
            Class<?> leakedType = leakedStorageType(
                    field.getGenericType(),
                    forbiddenTypes,
                    new HashSet<>(),
                    true
            );
            Assert.assertNull(
                    owner.getSimpleName() + "." + field.getName() + " exposes " + simpleName(leakedType),
                    leakedType
            );
        }
    }

    private static Class<?> leakedStorageType(
            Type type,
            Set<Class<?>> forbiddenTypes,
            Set<Type> visited,
            boolean expandAccessors
    ) {
        if (type == null || !visited.add(type)) {
            return null;
        }
        if (type instanceof ParameterizedType parameterizedType) {
            for (Type argument : parameterizedType.getActualTypeArguments()) {
                Class<?> leakedType = leakedStorageType(
                        argument,
                        forbiddenTypes,
                        visited,
                        expandAccessors
                );
                if (leakedType != null) {
                    return leakedType;
                }
            }
            return leakedStorageType(
                    parameterizedType.getRawType(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
        }
        if (type instanceof GenericArrayType arrayType) {
            return leakedStorageType(
                    arrayType.getGenericComponentType(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
        }
        if (type instanceof WildcardType wildcardType) {
            Class<?> leakedType = leakedStorageType(
                    wildcardType.getLowerBounds(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
            return leakedType != null
                    ? leakedType
                    : leakedStorageType(
                            wildcardType.getUpperBounds(),
                            forbiddenTypes,
                            visited,
                            expandAccessors
                    );
        }
        if (type instanceof TypeVariable<?> typeVariable) {
            return leakedStorageType(
                    typeVariable.getBounds(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
        }
        if (!(type instanceof Class<?> rawType)) {
            return null;
        }
        return leakedStorageClass(rawType, forbiddenTypes, visited, expandAccessors);
    }

    private static Class<?> leakedStorageType(
            Type[] types,
            Set<Class<?>> forbiddenTypes,
            Set<Type> visited,
            boolean expandAccessors
    ) {
        for (Type type : types) {
            Class<?> leakedType = leakedStorageType(type, forbiddenTypes, visited, expandAccessors);
            if (leakedType != null) {
                return leakedType;
            }
        }
        return null;
    }

    private static Class<?> leakedStorageClass(
            Class<?> type,
            Set<Class<?>> forbiddenTypes,
            Set<Type> visited,
            boolean expandAccessors
    ) {
        if (forbiddenTypes.contains(type)) {
            return type;
        }
        if (type.isArray()) {
            return leakedStorageType(
                    type.componentType(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
        }
        if (type.isPrimitive() || type == Void.TYPE) {
            return null;
        }
        Package typePackage = type.getPackage();
        if (typePackage == null || !typePackage.getName().startsWith("yier.bubu.redis.storage.memory")) {
            return null;
        }
        if (!expandAccessors && !type.isInterface() && !type.isRecord()) {
            return null;
        }
        for (Method method : type.getDeclaredMethods()) {
            int modifiers = method.getModifiers();
            if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)
                    || (!type.isInterface() && method.getParameterCount() != 0)) {
                continue;
            }
            Class<?> leakedType = leakedStorageType(
                    method.getGenericReturnType(),
                    forbiddenTypes,
                    visited,
                    expandAccessors
            );
            if (leakedType == null && type.isInterface()) {
                leakedType = leakedStorageType(
                        method.getGenericParameterTypes(),
                        forbiddenTypes,
                        visited,
                        false
                );
            }
            if (leakedType != null) {
                return leakedType;
            }
        }
        if (expandAccessors) {
            for (Field field : type.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (Modifier.isPrivate(modifiers) || Modifier.isStatic(modifiers)) {
                    continue;
                }
                Class<?> leakedType = leakedStorageType(
                        field.getGenericType(),
                        forbiddenTypes,
                        visited,
                        true
                );
                if (leakedType != null) {
                    return leakedType;
                }
            }
            Class<?> leakedType = leakedStorageType(
                    type.getGenericSuperclass(),
                    forbiddenTypes,
                    visited,
                    true
            );
            if (leakedType != null) {
                return leakedType;
            }
            return leakedStorageType(
                    type.getGenericInterfaces(),
                    forbiddenTypes,
                    visited,
                    true
            );
        }
        return null;
    }

    private static void assertClassSignatureDoesNotReference(
            Class<?> type,
            Set<Class<?>> forbiddenTypes
    ) {
        for (Field field : type.getDeclaredFields()) {
            assertNoStorageLeak(type, field.getName(), field.getGenericType(), forbiddenTypes);
        }
        for (Constructor<?> constructor : type.getDeclaredConstructors()) {
            for (Type parameterType : constructor.getGenericParameterTypes()) {
                assertNoStorageLeak(type, "constructor", parameterType, forbiddenTypes);
            }
        }
        for (Method method : type.getDeclaredMethods()) {
            assertNoStorageLeak(type, method.getName(), method.getGenericReturnType(), forbiddenTypes);
            for (Type parameterType : method.getGenericParameterTypes()) {
                assertNoStorageLeak(type, method.getName(), parameterType, forbiddenTypes);
            }
        }
    }

    private static void assertNoStorageLeak(
            Class<?> owner,
            String member,
            Type type,
            Set<Class<?>> forbiddenTypes
    ) {
        Class<?> leakedType = leakedStorageType(
                type,
                forbiddenTypes,
                new HashSet<>(),
                true
        );
        Assert.assertNull(
                owner.getSimpleName() + "." + member + " references " + simpleName(leakedType),
                leakedType
        );
    }

    private static Set<Class<?>> forbiddenStorageTypes() {
        return Set.of(
                StableMemoryBackend.class,
                EntryTable.class,
                NativeKeyDirectory.class,
                StringRoot.class,
                ListRoot.class,
                HashRoot.class,
                SetRoot.class,
                ZSetRoot.class
        );
    }

    private static Optional<StableMemoryBackend> genericLeak() {
        return Optional.empty();
    }

    private static WrappedLeak wrappedLeak() {
        return null;
    }

    private static CallbackLeak callbackLeak() {
        return null;
    }

    private record WrappedLeak(StableMemoryBackend backend) {
    }

    private record CallbackLeak(StorageCallback callback) {
    }

    private interface StorageCallback {
        void accept(StableMemoryBackend backend);
    }

    private static final class LeakProbe {
        private LeakProbe() {
        }

        private static Optional<StableMemoryBackend> genericLeak() {
            return YierdisDbArchitectureGuardTest.genericLeak();
        }

        private static WrappedLeak wrappedLeak() {
            return YierdisDbArchitectureGuardTest.wrappedLeak();
        }

        private static CallbackLeak callbackLeak() {
            return YierdisDbArchitectureGuardTest.callbackLeak();
        }

        private void parameterLeak(Consumer<StableMemoryBackend> consumer) {
        }

        private void callbackParameterLeak(StorageCallback callback) {
        }

        private static void constructionInput(StableMemoryBackend backend) {
        }

        private static void staticCallbackLeak(Consumer<StableMemoryBackend> consumer) {
        }
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
