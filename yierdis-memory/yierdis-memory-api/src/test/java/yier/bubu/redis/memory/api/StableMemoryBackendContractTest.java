package yier.bubu.redis.memory.api;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import yier.bubu.redis.common.memory.MemoryPressureBudget;
import yier.bubu.redis.common.memory.MemoryReclaimResult;
import yier.bubu.redis.common.memory.MemoryUsageSnapshot;

public class StableMemoryBackendContractTest {
    @Test
    public void statsRecordExposesAllocatorCounters() {
        NativeAllocatorStats stats = new NativeAllocatorStats(
                10,
                64,
                128,
                64,
                54,
                2,
                3,
                4,
                2,
                1,
                3,
                4,
                5,
                6,
                7,
                8
        );

        Assert.assertEquals(10L, stats.logicalUsedBytes());
        Assert.assertEquals(64L, stats.reservedBytes());
        Assert.assertEquals(128L, stats.committedBytes());
        Assert.assertEquals(64L, stats.freeBytes());
        Assert.assertEquals(54L, stats.internalFragmentationBytes());
        Assert.assertEquals(2L, stats.liveSmallPages());
        Assert.assertEquals(3L, stats.liveMediumSpanPages());
        Assert.assertEquals(4L, stats.liveLargeSpanPages());
        Assert.assertEquals(2L, stats.liveObjects());
        Assert.assertEquals(1L, stats.pinnedObjects());
        Assert.assertEquals(3L, stats.quarantinedObjects());
        Assert.assertEquals(4L, stats.staleHandleDetections());
        Assert.assertEquals(5L, stats.reallocInPlaceCount());
        Assert.assertEquals(6L, stats.reallocMovedCount());
        Assert.assertEquals(7L, stats.defragMovedBytes());
        Assert.assertEquals(8L, stats.defragSkippedPinnedObjects());
    }

    @Test
    public void statsRecordExposesProductionAllocatorCounters() {
        NativeObjectKindCounts counts = new NativeObjectKindCounts(
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                17,
                18,
                19,
                20,
                21
        );
        NativeAllocationLatencyHistogram histogram = new NativeAllocationLatencyHistogram(
                20,
                10_000,
                2_000,
                11,
                4,
                3,
                2,
                0
        );
        NativeAllocatorStats stats = new NativeAllocatorStats(
                10,
                64,
                128,
                64,
                54,
                2,
                3,
                4,
                2,
                1,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                10,
                11,
                12,
                13,
                14,
                15,
                16,
                counts,
                histogram
        );

        Assert.assertEquals(9L, stats.externalFragmentationBytes());
        Assert.assertEquals(10L, stats.smallFreeBytes());
        Assert.assertEquals(11L, stats.mediumFreeBytes());
        Assert.assertEquals(12L, stats.largeFreeBytes());
        Assert.assertEquals(13L, stats.freePages());
        Assert.assertEquals(14L, stats.quarantineBytes());
        Assert.assertEquals(15L, stats.doubleFreeDetections());
        Assert.assertEquals(16L, stats.defragReclaimedPages());
        Assert.assertEquals(2L, stats.objectCount(NativeObjectKind.STRING_BYTES));
        Assert.assertEquals(3L, stats.objectCount(NativeObjectKind.LISTPACK_BYTES));
        Assert.assertEquals(9L, stats.objectCount(NativeObjectKind.ENTRY_RECORD));
        Assert.assertEquals(11L, stats.objectCount(NativeObjectKind.LIST_ROOT));
        Assert.assertEquals(15L, stats.objectCount(NativeObjectKind.LIST_NODE));
        Assert.assertEquals(16L, stats.objectCount(NativeObjectKind.HASH_TABLE));
        Assert.assertEquals(15L, stats.objectKindCounts().listNodeObjects());
        Assert.assertEquals(20L, stats.allocationLatencyHistogram().allocationCount());
        Assert.assertEquals(10_000L, stats.allocationLatencyHistogram().totalNanos());
    }

    @Test
    public void defragResultFactoriesExposeMovementOutcomes() {
        NativeDefragResult moved = NativeDefragResult.moved(12);
        Assert.assertTrue(moved.moved());
        Assert.assertEquals(12L, moved.movedBytes());

        NativeDefragResult pinned = NativeDefragResult.skippedPinnedObject();
        Assert.assertFalse(pinned.moved());
        Assert.assertTrue(pinned.skippedPinned());

        NativeDefragResult budget = NativeDefragResult.skippedMoveBudget();
        Assert.assertFalse(budget.moved());
        Assert.assertTrue(budget.skippedBudget());
    }

    @Test
    public void defragCycleRecordsExposeBudgetsAndCounters() {
        NativeDefragOptions options = new NativeDefragOptions(64, 3, 1_000);
        Assert.assertEquals(64L, options.maxMoveBytes());
        Assert.assertEquals(3L, options.maxObjects());
        Assert.assertEquals(1_000L, options.timeBudgetNanos());

        NativeDefragReport report = new NativeDefragReport(
                4,
                2,
                48,
                1,
                1,
                1,
                true,
                false,
                true
        );
        Assert.assertEquals(4L, report.scannedObjects());
        Assert.assertEquals(2L, report.movedObjects());
        Assert.assertEquals(48L, report.movedBytes());
        Assert.assertEquals(1L, report.skippedPinnedObjects());
        Assert.assertEquals(1L, report.skippedBudgetObjects());
        Assert.assertEquals(1L, report.failedMoves());
        Assert.assertTrue(report.stoppedByByteBudget());
        Assert.assertFalse(report.stoppedByObjectBudget());
        Assert.assertTrue(report.stoppedByTimeBudget());
    }

    @Test
    public void epochKindsCoverAllocatorReadSafetyScopes() {
        Assert.assertEquals(NativeEpochKind.COMMAND, NativeEpochKind.valueOf("COMMAND"));
        Assert.assertEquals(NativeEpochKind.SCAN, NativeEpochKind.valueOf("SCAN"));
        Assert.assertEquals(NativeEpochKind.SNAPSHOT, NativeEpochKind.valueOf("SNAPSHOT"));
        Assert.assertEquals(NativeEpochKind.DEFRAG, NativeEpochKind.valueOf("DEFRAG"));
    }

    @Test
    public void exceptionTypesCarryMessages() {
        NativeMemoryException base = new NativeMemoryException("base");
        StaleNativeHandleException stale = new StaleNativeHandleException("stale");

        Assert.assertEquals("base", base.getMessage());
        Assert.assertEquals("stale", stale.getMessage());
    }

    @Test
    public void nativeCapacityExceptionIsAnOffHeapOom() {
        Assert.assertTrue(OffHeapOutOfMemoryException.class
                .isAssignableFrom(NativeCapacityExceededException.class));
        Assert.assertFalse(NativeMemoryException.class
                .isAssignableFrom(NativeCapacityExceededException.class));
    }

    @Test
    public void requiredBackendBehaviorIsAbstract() {
        Set<String> required = Set.of(
                "allocatorId", "bindToCurrentThread", "allocate", "reallocate",
                "free", "pin", "unpin", "beginEpoch", "beginAllocationScope",
                "estimateAllocationScopeBookkeepingBytes", "resolve", "resolvePinned",
                "allocateRegion", "defragOne", "defragCycle", "logicalUsedBytes",
                "stats", "metadataStats", "memoryUsage", "trimEmptyPages",
                "estimateAdditionalGrowth", "estimateConservativeAdditionalGrowth",
                "liveRegionCount", "close"
        );
        Method[] declaredMethods = StableMemoryBackend.class.getDeclaredMethods();
        Assert.assertEquals(required.size(), declaredMethods.length);
        Set<String> declared = Arrays.stream(declaredMethods)
                .peek(method -> Assert.assertTrue(
                        method.getName() + " must be abstract",
                        Modifier.isAbstract(method.getModifiers())
                ))
                .map(Method::getName)
                .collect(Collectors.toSet());

        Assert.assertEquals(required, declared);
    }

    @Test
    public void backendMethodSignaturesAreExact() throws Exception {
        assertAbstractMethod(StableMemoryBackend.class, long.class, "allocatorId");
        assertAbstractMethod(StableMemoryBackend.class, void.class, "bindToCurrentThread");
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeHandle.class,
                "allocate",
                NativeObjectKind.class,
                int.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeHandle.class,
                "reallocate",
                NativeHandle.class,
                int.class,
                NativeReallocPolicy.class
        );
        assertAbstractMethod(StableMemoryBackend.class, void.class, "free", NativeHandle.class);
        assertAbstractMethod(StableMemoryBackend.class, void.class, "pin", NativeHandle.class);
        assertAbstractMethod(StableMemoryBackend.class, void.class, "unpin", NativeHandle.class);
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeEpochScope.class,
                "beginEpoch",
                NativeEpochKind.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeAllocationScope.class,
                "beginAllocationScope"
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                long.class,
                "estimateAllocationScopeBookkeepingBytes",
                int.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeObjectView.class,
                "resolve",
                NativeHandle.class,
                NativeAccessMode.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeObjectView.class,
                "resolvePinned",
                NativeHandle.class,
                NativeAccessMode.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                StableMemoryRegion.class,
                "allocateRegion",
                String.class,
                int.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeDefragResult.class,
                "defragOne",
                NativeHandle.class,
                long.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeDefragReport.class,
                "defragCycle",
                NativeDefragOptions.class
        );
        assertAbstractMethod(StableMemoryBackend.class, long.class, "logicalUsedBytes");
        assertAbstractMethod(StableMemoryBackend.class, NativeAllocatorStats.class, "stats");
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeAllocatorMetadataStats.class,
                "metadataStats"
        );
        assertAbstractMethod(StableMemoryBackend.class, MemoryUsageSnapshot.class, "memoryUsage");
        assertAbstractMethod(
                StableMemoryBackend.class,
                MemoryReclaimResult.class,
                "trimEmptyPages",
                MemoryPressureBudget.class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeAllocationGrowth.class,
                "estimateAdditionalGrowth",
                int[].class
        );
        assertAbstractMethod(
                StableMemoryBackend.class,
                NativeAllocationGrowth.class,
                "estimateConservativeAdditionalGrowth",
                int[].class
        );
        assertAbstractMethod(StableMemoryBackend.class, long.class, "liveRegionCount");
        assertAbstractMethod(StableMemoryBackend.class, void.class, "close");
    }

    @Test
    public void regionMethodSignaturesAreExact() throws Exception {
        Assert.assertEquals(11, StableMemoryRegion.class.getDeclaredMethods().length);
        assertAbstractMethod(StableMemoryRegion.class, int.class, "size");
        assertAbstractMethod(StableMemoryRegion.class, byte.class, "getByte", int.class);
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "setByte",
                int.class,
                byte.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                int.class,
                "getIntLittleEndian",
                int.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "setIntLittleEndian",
                int.class,
                int.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                long.class,
                "getLongLittleEndian",
                int.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "setLongLittleEndian",
                int.class,
                long.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "getBytes",
                int.class,
                byte[].class,
                int.class,
                int.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "setBytes",
                int.class,
                byte[].class,
                int.class,
                int.class
        );
        assertAbstractMethod(
                StableMemoryRegion.class,
                void.class,
                "copyTo",
                int.class,
                StableMemoryRegion.class,
                int.class,
                int.class
        );
        assertAbstractMethod(StableMemoryRegion.class, void.class, "close");
    }

    @Test
    public void ownerFactoryAndOwnershipExceptionContractsAreExact() throws Exception {
        Assert.assertEquals(3, MemoryOwner.class.getDeclaredMethods().length);
        assertAbstractMethod(MemoryOwner.class, void.class, "bindToCurrentThread");
        assertAbstractMethod(MemoryOwner.class, void.class, "checkCurrentThread");
        assertAbstractMethod(MemoryOwner.class, void.class, "checkCurrentThreadForShutdown");
        Assert.assertEquals(1, StableMemoryBackendFactory.class.getDeclaredMethods().length);
        assertAbstractMethod(
                StableMemoryBackendFactory.class,
                StableMemoryBackend.class,
                "create",
                String.class,
                int.class,
                MemoryOwner.class
        );
        Assert.assertTrue(
                StableMemoryBackendFactory.class.isAnnotationPresent(FunctionalInterface.class)
        );

        NativeHandleOwnershipException failure =
                new NativeHandleOwnershipException(11L, 12L);
        Assert.assertEquals(
                1,
                NativeHandleOwnershipException.class.getDeclaredConstructors().length
        );
        Assert.assertNotNull(
                NativeHandleOwnershipException.class.getConstructor(long.class, long.class)
        );
        Assert.assertEquals(
                Set.of("expectedAllocatorId", "actualAllocatorId"),
                Arrays.stream(NativeHandleOwnershipException.class.getDeclaredMethods())
                        // Surefire 的 JaCoCo agent 会注入 synthetic 探针方法；契约只检查用户声明的 API。
                        .filter(method -> !method.isSynthetic())
                        .map(Method::getName)
                        .collect(Collectors.toSet())
        );
        Assert.assertEquals(
                long.class,
                NativeHandleOwnershipException.class
                        .getDeclaredMethod("expectedAllocatorId")
                        .getReturnType()
        );
        Assert.assertEquals(
                long.class,
                NativeHandleOwnershipException.class
                        .getDeclaredMethod("actualAllocatorId")
                        .getReturnType()
        );
        Assert.assertEquals(11L, failure.expectedAllocatorId());
        Assert.assertEquals(12L, failure.actualAllocatorId());
        Assert.assertEquals(
                long.class,
                NativeHandleOwnershipException.class
                        .getDeclaredField("expectedAllocatorId")
                        .getType()
        );
        Assert.assertEquals(
                long.class,
                NativeHandleOwnershipException.class
                        .getDeclaredField("actualAllocatorId")
                        .getType()
        );
        Assert.assertTrue(failure instanceof NativeMemoryException);
    }

    @Test
    public void publicMemoryApiHasNoRawOperationOrLegacyAllocator() throws Exception {
        Set<String> methodNames = Arrays.stream(StableMemoryBackend.class.getMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());

        Assert.assertFalse(methodNames.stream().anyMatch(name -> name.endsWith("Raw")));
        Assert.assertThrows(
                ClassNotFoundException.class,
                () -> Class.forName("yier.bubu.redis.memory.api.NativeAllocator")
        );
        Assert.assertArrayEquals(
                new String[]{"allocatorId", "localRaw"},
                Arrays.stream(NativeHandle.class.getRecordComponents())
                        .map(component -> component.getName())
                        .toArray(String[]::new)
        );
        Assert.assertEquals(1, NativeHandle.class.getDeclaredConstructors().length);
        Assert.assertNotNull(NativeHandle.class.getConstructor(long.class, long.class));
        Assert.assertEquals(
                Set.of(
                        "allocatorId():long",
                        "localRaw():long",
                        "isNull():boolean",
                        "equals(java.lang.Object):boolean",
                        "hashCode():int",
                        "toString():java.lang.String"
                ),
                Arrays.stream(NativeHandle.class.getDeclaredMethods())
                        .filter(method -> !method.isSynthetic())
                        .map(StableMemoryBackendContractTest::methodDescriptor)
                        .collect(Collectors.toSet())
        );
        Set<String> retiredHandleMethods = Set.of(
                "raw", "fromRaw", "rawOf", "of", "requireNonNull", "requireValidRaw", "domain",
                "domainCode", "kindCode", "slotId", "generation", "flags"
        );
        Set<String> handleMethods = Arrays.stream(NativeHandle.class.getDeclaredMethods())
                .map(Method::getName)
                .collect(Collectors.toSet());
        for (String retired : retiredHandleMethods) {
            Assert.assertFalse("retired NativeHandle method remains: " + retired,
                    handleMethods.contains(retired));
        }
        Assert.assertThrows(
                NoSuchMethodException.class,
                () -> NativeHandle.class.getDeclaredMethod("isNull", long.class)
        );
    }

    private static void assertAbstractMethod(
            Class<?> owner,
            Class<?> returnType,
            String name,
            Class<?>... parameterTypes
    ) throws Exception {
        Method method = owner.getDeclaredMethod(name, parameterTypes);
        Assert.assertEquals(name, returnType, method.getReturnType());
        Assert.assertTrue(name + " must be abstract", Modifier.isAbstract(method.getModifiers()));
    }

    private static String methodDescriptor(Method method) {
        String parameters = Arrays.stream(method.getParameterTypes())
                .map(Class::getTypeName)
                .collect(Collectors.joining(","));
        return method.getName() + "(" + parameters + "):" + method.getReturnType().getTypeName();
    }
}
