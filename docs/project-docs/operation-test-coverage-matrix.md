# Operation Test Coverage Matrix

This document is the operation coverage inventory for command behavior, DB API behavior, and native/internal storage behavior.

Status values:

- `covered`: this layer has direct, named coverage for the operation.
- `covered-by-shared-test`: this layer is exercised through a broader cross-layer test, but does not yet have a dedicated narrow test.
- `missing`: this layer needs a direct test or a more explicit shared test reference.
- `not-applicable`: this operation does not touch that layer.

## Command Layer Coverage

### AUTH

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest#authReportsNoPasswordConfigured`.
- **DB API**: `not-applicable` - authentication currently has no DB state.
- **Native internals**: `not-applicable` - authentication currently has no native storage state.

### APPEND

- **Command layer**: `covered` - `StringCommandTest#stringCommandsCoverBinarySafeSetGetStrlenAndAppend`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootOverwriteReusesSpareCapacityForShorterValue`.

### BITCOUNT

- **Command layer**: `covered` - `BitmapCommandTest#bitcountRangeFollowsRedisByteRangeRules`.
- **DB API**: `covered` - `StringDirectOpsTest#bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootEnsureLengthSupportsBitmapStyleGrowthWithZeroFill`.

### CLIENT

- **Command layer**: `covered` - `CommandProcessorTest#clientMetadataCommandsAreAccepted`.
- **DB API**: `not-applicable` - client metadata lives on `ServerSession`.
- **Native internals**: `not-applicable` - client metadata has no native storage state.

### COMMAND

- **Command layer**: `covered` - `CommandVariantCoverageTest#commandVariantsCoverBaseCountInfoAndUnknownName`, `CommandMetadataRegressionTest#commandInfoKeepsMetadataForBuiltInAndExtraCommands`, and `CommandDescriptorRegistryTest#commandInfoUsesDescriptorFromRegistryRegistration`.
- **DB API**: `not-applicable` - command metadata is registry state.
- **Native internals**: `not-applicable` - command metadata has no native storage state.

### DECR

- **Command layer**: `covered` - `StringCommandTest#counterCommandsCoverIncrDecrAndInvalidInteger`.
- **DB API**: `covered-by-shared-test` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` covers `StringWriteOps.incrBy`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootStoresIntegerLikeBytesAsRawNativeBytes`.

### DEL

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest#binaryKeyIsSupportedEndToEnd`.
- **DB API**: `covered` - `OffHeapStringStorageTest#setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes`.
- **Native internals**: `covered-by-shared-test` - `NativeStorageRegressionTest#allNativeRootsReleaseToZeroAfterDelete`.

### DISCARD

- **Command layer**: `covered` - `TransactionCommandTest#execAndDiscardWithoutMultiReturnErrors` and `TransactionCommandTest#multiCannotBeNested`.
- **DB API**: `not-applicable` - transaction queue control does not directly mutate DB API state.
- **Native internals**: `not-applicable` - transaction queue control has no native storage state.

### ECHO

- **Command layer**: `covered` - `CommandVariantCoverageTest#connectionCommandsCoverPingEchoQuitAndSelectValidation`.
- **DB API**: `not-applicable` - echo has no DB state.
- **Native internals**: `not-applicable` - echo has no native storage state.

### EXEC

- **Command layer**: `covered` - `TransactionCommandTest#multiQueuesAndExecAppliesInOrder`.
- **DB API**: `covered-by-shared-test` - `TransactionCommandTest#multiQueuesAndExecAppliesInOrder`.
- **Native internals**: `covered-by-shared-test` - `TransactionCommandTest#multiQueuesAndExecAppliesInOrder`.

### EXISTS

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest#binaryKeyIsSupportedEndToEnd`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `NativeKeyDirectoryTest#nativeKeyDirectoryMapsKeysToStableHandlesAndReleasesThem`.

### EXPIRE

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest#setGetIncrExpireTtl` and `CommandProcessorTest#expireZeroDeletesKeyImmediately`.
- **DB API**: `covered` - `ExpireIndexTest#ttlAccountingAffectsUsedBytesForMaxmemory`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#heapExpireIndexRoundTripsHandleLookupAndClear` and `ExpireIndexContractTest#ffmExpireIndexRoundTripsHandleLookupAndClear`.

### EXPIREAT

- **Command layer**: `covered` - `Milestone1CompatTest#expireAtUsesUnixSecondsAndReportsRemainingTtl`.
- **DB API**: `covered` - `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#heapExpireIndexRoundTripsHandleLookupAndClear`.

### FLUSHDB

- **Command layer**: `covered` - `CommandVariantCoverageTest#flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode`.
- **DB API**: `covered` - `TtlLifecycleDirectOpsTest#lifecycleFlushDbAndMemoryObjectApisCoverExistingMissingAndAccessors`.
- **Native internals**: `covered-by-shared-test` - `NativeStorageRegressionTest#allNativeRootsReleaseToZeroAfterDelete`.

### GET

- **Command layer**: `covered` - `StringCommandTest#stringCommandsCoverBinarySafeSetGetStrlenAndAppend`.
- **DB API**: `covered` - `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads`.

### GETBIT

- **Command layer**: `covered` - `BitmapCommandTest#getbitSetbitBasicSemantics`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootEnsureLengthSupportsBitmapStyleGrowthWithZeroFill`.

### HELLO

- **Command layer**: `covered` - `RespHandshakeIntegrationTest#hello3SwitchesConnectionToResp3`, `RespHandshakeIntegrationTest#hello2SetnameUnsupportedProtoAndAuthAreHandled`, and `TransactionCommandTest#modulesCanRejectCommandsInsideMultiAndAbortTransaction`.
- **DB API**: `not-applicable` - HELLO changes session protocol state.
- **Native internals**: `not-applicable` - HELLO has no native storage state.

### HDEL

- **Command layer**: `covered` - `HashCommandTest#hdelRemovesHashKeyWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `HashValueTest#packedHashSupportsUpdateAndDeleteWithRepacking`.

### HGET

- **Command layer**: `covered` - `HashCommandTest#hsetHgetHlenAndHgetallAreBinarySafe`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `HashValueTest#packedHashSupportsUpdateAndDeleteWithRepacking`.

### HGETALL

- **Command layer**: `covered` - `HashCommandTest#hsetHgetHlenAndHgetallAreBinarySafe`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `HashValueTest#packedHashSupportsUpdateAndDeleteWithRepacking`.

### HLEN

- **Command layer**: `covered` - `HashCommandTest#hsetHgetHlenAndHgetallAreBinarySafe`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `HashValueTest#hashConvertsToHashTableAfterTooManyFields`.

### HSET

- **Command layer**: `covered` - `HashCommandTest#hsetHgetHlenAndHgetallAreBinarySafe`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `HashValueTest#packedHashSupportsUpdateAndDeleteWithRepacking`.

### INFO

- **Command layer**: `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **DB API**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Native internals**: `covered-by-shared-test` - `YierdisDbMemoryReporterTest#memoryStatsIncludesFfmNativeBytesWhenEnabledForMaxmemory`.

### INCR

- **Command layer**: `covered` - `StringCommandTest#counterCommandsCoverIncrDecrAndInvalidInteger` and `CommandProcessorTest#incrWorksAfterAppendWhenRawStringHasSpareCapacity`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootStoresIntegerLikeBytesAsRawNativeBytes`.

### KEYS

- **Command layer**: `covered` - `CommandProcessorTest#keysGlobMatchesOnRawBytes` and `CommandProcessorTest#keysGlobSupportsBracketsNegationRangesAndEscapes`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `ByteArrayKeyspaceTest#computeGetAndForEachWorkAcrossRehash` and `NativeKeyDirectoryTest#nativeKeyDirectoryExposesKeyHandlesForScanAndRandomSelection`.

### LPOP

- **Command layer**: `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty` and `Milestone1CompatTest#lpopCountHandlesNullArrayAndEmptyArray`.
- **DB API**: `covered` - `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ListRootTest#listRootSupportsPushPopAndStreaming`.

### LPUSH

- **Command layer**: `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ListRootTest#listRootSupportsPushPopAndStreaming`.

### LRANGE

- **Command layer**: `covered` - `ListCommandTest#lrangeClampsIndicesAndHandlesOutOfRange`.
- **DB API**: `covered` - `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ListRootTest#listRootSupportsPushPopAndStreaming`.

### MEMORY

- **Command layer**: `covered` - `MemoryStatsCommandTest#memoryStatsReturnsStableKeyValuePairs` and `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **DB API**: `covered` - `YierdisDbMemoryReporterTest#directMemoryUsageReadsNativeEntryAndValueMetadata` and `TtlLifecycleDirectOpsTest#lifecycleFlushDbAndMemoryObjectApisCoverExistingMissingAndAccessors`.
- **Native internals**: `covered-by-shared-test` - `MemoryLedgerContractTest#reserveCommitRollbackMaintainInvariants`.

### MULTI

- **Command layer**: `covered` - `TransactionCommandTest#multiQueuesAndExecAppliesInOrder` and `TransactionCommandTest#multiCannotBeNested`.
- **DB API**: `not-applicable` - MULTI only opens transaction queue state.
- **Native internals**: `not-applicable` - MULTI has no native storage state.

### OBJECT

- **Command layer**: `covered` - `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **DB API**: `covered` - `YierdisDbIntrospectionTest#objectEncodingReadsNativeEntryEncoding`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootStoresIntegerLikeBytesAsRawNativeBytes` and `HashValueTest#hashConvertsToHashTableAfterTooManyFields`.

### PERSIST

- **Command layer**: `covered-by-shared-test` - `Milestone1CompatTest#ttlFamilyPersistAndPexpireMatchRedisLikeConventions`.
- **DB API**: `covered` - `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexTest#ttlAccountingAffectsUsedBytesForMaxmemory`.

### PEXPIRE

- **Command layer**: `covered-by-shared-test` - `Milestone1CompatTest#ttlFamilyPersistAndPexpireMatchRedisLikeConventions` and `TtlMaxmemoryTest#pexpireIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction`.
- **DB API**: `covered` - `StringDirectOpsTest#bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#ffmExpireIndexRoundTripsHandleLookupAndClear`.

### PEXPIREAT

- **Command layer**: `covered-by-shared-test` - `TtlMaxmemoryTest#pexpireatIsRejectedWhenItWouldAddTtlMetadataUnderNoeviction`.
- **DB API**: `covered` - `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#ffmExpireIndexRoundTripsHandleLookupAndClear`.

### PFADD

- **Command layer**: `covered` - `HllCommandTest#pfaddCreatesAndUpdates`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics`.
- **Native internals**: `covered-by-shared-test` - `YierdisHyperLogLogTest#sparseHllAddsElementsAndMergesIntoRegisters`.

### PFCOUNT

- **Command layer**: `covered` - `HllCommandTest#pfcountAndPfmergeWorkOnUnion`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics`.
- **Native internals**: `covered-by-shared-test` - `YierdisHyperLogLogTest#denseHllUpdatesInPlaceAndMergesViaBytesSlice`.

### PFMERGE

- **Command layer**: `covered` - `HllCommandTest#pfcountAndPfmergeWorkOnUnion`.
- **DB API**: `covered` - `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics`.
- **Native internals**: `covered-by-shared-test` - `YierdisHyperLogLogTest#denseBytesFromRegistersClampsAndRoundTripsThroughMerge`.

### PING

- **Command layer**: `covered` - `CommandVariantCoverageTest#connectionCommandsCoverPingEchoQuitAndSelectValidation`.
- **DB API**: `not-applicable` - PING has no DB state.
- **Native internals**: `not-applicable` - PING has no native storage state.

### PTTL

- **Command layer**: `covered-by-shared-test` - `Milestone1CompatTest#ttlFamilyPersistAndPexpireMatchRedisLikeConventions`.
- **DB API**: `covered` - `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#ffmExpireIndexRoundTripsHandleLookupAndClear`.

### QUIT

- **Command layer**: `covered` - `CommandVariantCoverageTest#connectionCommandsCoverPingEchoQuitAndSelectValidation`.
- **DB API**: `not-applicable` - QUIT has no DB state.
- **Native internals**: `not-applicable` - QUIT has no native storage state.

### RPOP

- **Command layer**: `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty` and `CommandVariantCoverageTest#rpopCountVariantsCoverNullArrayEmptyArrayAndNegativeCount`.
- **DB API**: `covered` - `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ListRootTest#listRootSupportsPushPopAndStreaming`.

### RPUSH

- **Command layer**: `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ListRootTest#listRootSupportsPushPopAndStreaming`.

### SADD

- **Command layer**: `covered` - `SetCommandTest#upgradeFromIntsetKeepsExistingMembers`.
- **DB API**: `covered` - `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion`.
- **Native internals**: `covered-by-shared-test` - `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable`.

### SCAN

- **Command layer**: `covered` - `Milestone1CompatTest#scanMatchAndCountEventuallyReturnsAllMatchingKeys`, `ScanCursorContractTest#cursorTerminatesAtZeroAndMakesProgress`, and `CommandVariantCoverageTest#scanVariantsCoverInvalidCursorAndDuplicateOptions`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `NativeKeyDirectoryTest#nativeKeyDirectoryExposesKeyHandlesForScanAndRandomSelection`.

### SCARD

- **Command layer**: `covered` - `SetCommandTest#upgradeFromIntsetKeepsExistingMembers`.
- **DB API**: `covered` - `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion`.
- **Native internals**: `covered-by-shared-test` - `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable`.

### SELECT

- **Command layer**: `covered` - `CommandVariantCoverageTest#connectionCommandsCoverPingEchoQuitAndSelectValidation` and `YierdisServerBootstrapCommandWiringTest#bootstrapWiresServerAndCoreConnectionCommandsTogether`.
- **DB API**: `covered-by-shared-test` - `YierdisServerBootstrapCommandWiringTest#bootstrapWiresServerAndCoreConnectionCommandsTogether`.
- **Native internals**: `not-applicable` - SELECT changes session DB index, not native storage.

### SET

- **Command layer**: `covered` - `StringCommandTest#stringCommandsCoverBinarySafeSetGetStrlenAndAppend`, `StringCommandTest#setOptionsCoverNxXxGetAndTtlModes`, `CommandProcessorTest#setNxReturnsNilWhenKeyExists`, and `Milestone1CompatTest#setGetAndKeepTtlBehaveAsExpected`.
- **DB API**: `covered` - `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit`.
- **Native internals**: `covered` - `StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads`.

### SETBIT

- **Command layer**: `covered` - `BitmapCommandTest#getbitSetbitBasicSemantics` and `BitmapCommandTest#setbitZeroFillsGrownBytesWithinCapacity`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered` - `StringRootTest#stringRootEnsureLengthSupportsBitmapStyleGrowthWithZeroFill`.

### SISMEMBER

- **Command layer**: `covered` - `SetCommandTest#setMembersAreBinarySafeEvenWhenIntegerLike`.
- **DB API**: `covered` - `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion`.
- **Native internals**: `covered-by-shared-test` - `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable`.

### SMEMBERS

- **Command layer**: `covered` - `SetCommandTest#setMembersAreBinarySafeEvenWhenIntegerLike`.
- **DB API**: `covered` - `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion`.
- **Native internals**: `covered-by-shared-test` - `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable`.

### SREM

- **Command layer**: `covered` - `SetCommandTest#sremDeletesKeyWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion`.
- **Native internals**: `covered-by-shared-test` - `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable`.

### STATS

- **Command layer**: `covered` - `YierdisServerBootstrapCommandWiringTest#bootstrapBindsTransportNeutralExecutorIntoInfoProvider`.
- **DB API**: `not-applicable` - STATS reads server executor and connection counters.
- **Native internals**: `not-applicable` - STATS has no native storage state.

### STRLEN

- **Command layer**: `covered` - `StringCommandTest#stringCommandsCoverBinarySafeSetGetStrlenAndAppend`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads`.

### TTL

- **Command layer**: `covered-by-shared-test` - `CommandProcessorTest#setGetIncrExpireTtl` and `Milestone1CompatTest#ttlFamilyPersistAndPexpireMatchRedisLikeConventions`.
- **DB API**: `covered` - `ExpireIndexTest#ttlBytesViewLazilyDeletesExpiredKeys`.
- **Native internals**: `covered-by-shared-test` - `ExpireIndexContractTest#heapExpireIndexRoundTripsHandleLookupAndClear`.

### TYPE

- **Command layer**: `covered-by-shared-test` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`, `HashCommandTest#hdelRemovesHashKeyWhenEmpty`, and `ZSetCommandTest#zremDeletesKeyWhenEmpty`.
- **DB API**: `covered` - `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries`.
- **Native internals**: `covered-by-shared-test` - `EntryTableContractTest#entryRecordCarriesNativeMetadata`.

### ZADD

- **Command layer**: `covered` - `ZSetCommandTest#zrangeTieBreakIsRawByteLexAndBoundsWork` and `ZSetCommandTest#zaddRejectsInvalidScores`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZRANGE

- **Command layer**: `covered` - `ZSetCommandTest#zrangeTieBreakIsRawByteLexAndBoundsWork`, `ZSetCommandTest#zrevrangeAndZrangeRevReturnReverseOrder`, and `CommandErrorTest#arityAndSyntaxErrorsMatchExpectedMessages`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores` and `CommandErrorTest#scoreRangeCommandsValidateArityAndLimitArguments`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZREM

- **Command layer**: `covered` - `ZSetCommandTest#zremDeletesKeyWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZREMRANGEBYRANK

- **Command layer**: `covered` - `ZSetCommandTest#zremrangeByRankRemovesAndDeletesKeyWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#zsetUpgradesAfterTooManyEntries`.

### ZREMRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest#zremrangeByScoreRemovesAndDeletesKeyWhenEmpty`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZREVRANGE

- **Command layer**: `covered` - `ZSetCommandTest#zrevrangeAndZrangeRevReturnReverseOrder` and `CommandVariantCoverageTest#zrevrangeInvalidOptionIsSyntaxError`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates`.

### ZREVRANGEBYSCORE

- **Command layer**: `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores` and `CommandErrorTest#scoreRangeCommandsValidateArityAndLimitArguments`.
- **DB API**: `covered` - `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl`.
- **Native internals**: `covered-by-shared-test` - `ZSetValueTest#zsetUpgradesAfterTooManyEntries`.

## Option And Subcommand Coverage

- **Command variant**: `COMMAND / base` - `covered` - `CommandVariantCoverageTest#commandVariantsCoverBaseCountInfoAndUnknownName`.
- **Command variant**: `COMMAND / COUNT` - `covered` - `CommandVariantCoverageTest#commandVariantsCoverBaseCountInfoAndUnknownName`.
- **Command variant**: `COMMAND / INFO` - `covered` - `CommandVariantCoverageTest#commandVariantsCoverBaseCountInfoAndUnknownName`.
- **Command variant**: `COMMAND / unknown name` - `covered` - `CommandVariantCoverageTest#commandVariantsCoverBaseCountInfoAndUnknownName`.
- **Command variant**: `CLIENT / SETINFO` - `covered` - `CommandProcessorTest#clientMetadataCommandsAreAccepted`.
- **Command variant**: `CLIENT / SETNAME` - `covered` - `CommandProcessorTest#clientMetadataCommandsAreAccepted`.
- **Command variant**: `CLIENT / GETNAME` - `covered` - `CommandProcessorTest#clientMetadataCommandsAreAccepted`.
- **Command variant**: `CLIENT / unknown subcommand` - `covered` - `CommandVariantCoverageTest#clientUnknownSubcommandReturnsRedisStyleError`.
- **Command variant**: `HELLO / RESP2` - `covered` - `RespHandshakeIntegrationTest#hello2SetnameUnsupportedProtoAndAuthAreHandled`.
- **Command variant**: `HELLO / RESP3` - `covered` - `RespHandshakeIntegrationTest#hello3SwitchesConnectionToResp3`.
- **Command variant**: `HELLO / SETNAME` - `covered` - `RespHandshakeIntegrationTest#hello2SetnameUnsupportedProtoAndAuthAreHandled`.
- **Command variant**: `HELLO / unsupported proto` - `covered` - `RespHandshakeIntegrationTest#hello2SetnameUnsupportedProtoAndAuthAreHandled`.
- **Command variant**: `HELLO / AUTH` - `covered` - `RespHandshakeIntegrationTest#hello2SetnameUnsupportedProtoAndAuthAreHandled`.
- **Command variant**: `HELLO / disallowed in MULTI` - `covered` - `TransactionCommandTest#modulesCanRejectCommandsInsideMultiAndAbortTransaction`.
- **Command variant**: `INFO / no section` - `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Command variant**: `INFO / yierdis` - `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Command variant**: `INFO / memory` - `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Command variant**: `INFO / keyspace` - `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Command variant**: `INFO / unknown section` - `covered` - `YierdisServerBootstrapCommandWiringTest#infoVariantsCoverDefaultKnownAndUnknownSections`.
- **Command variant**: `MEMORY / STATS` - `covered` - `MemoryStatsCommandTest#memoryStatsReturnsStableKeyValuePairs`.
- **Command variant**: `MEMORY / USAGE` - `covered` - `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **Command variant**: `MEMORY / invalid subcommand` - `covered` - `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **Command variant**: `OBJECT / ENCODING` - `covered` - `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **Command variant**: `OBJECT / invalid subcommand` - `covered` - `MaxmemoryEvictionTest#objectEncodingAndMemoryUsageAreExposed`.
- **Command variant**: `SCAN / cursor` - `covered` - `ScanCursorContractTest#cursorTerminatesAtZeroAndMakesProgress`.
- **Command variant**: `SCAN / MATCH` - `covered` - `Milestone1CompatTest#scanMatchAndCountEventuallyReturnsAllMatchingKeys`.
- **Command variant**: `SCAN / COUNT` - `covered` - `ScanCursorContractTest#countAndMatchNeverDeadlockEvenWhenNoKeyMatches`.
- **Command variant**: `SCAN / invalid cursor` - `covered` - `CommandVariantCoverageTest#scanVariantsCoverInvalidCursorAndDuplicateOptions`.
- **Command variant**: `SCAN / duplicate option` - `covered` - `CommandVariantCoverageTest#scanVariantsCoverInvalidCursorAndDuplicateOptions`.
- **Command variant**: `SET / plain` - `covered` - `StringCommandTest#stringCommandsCoverBinarySafeSetGetStrlenAndAppend`.
- **Command variant**: `SET / NX` - `covered` - `CommandProcessorTest#setNxReturnsNilWhenKeyExists`.
- **Command variant**: `SET / XX` - `covered` - `CommandVariantCoverageTest#setVariantsCoverXxPxExatPxatAndConflicts`.
- **Command variant**: `SET / GET` - `covered` - `Milestone1CompatTest#setGetAndKeepTtlBehaveAsExpected`.
- **Command variant**: `SET / EX` - `covered` - `Milestone1CompatTest#setGetAndKeepTtlBehaveAsExpected`.
- **Command variant**: `SET / PX` - `covered` - `CommandVariantCoverageTest#setVariantsCoverXxPxExatPxatAndConflicts`.
- **Command variant**: `SET / EXAT` - `covered` - `CommandVariantCoverageTest#setVariantsCoverXxPxExatPxatAndConflicts`.
- **Command variant**: `SET / PXAT` - `covered` - `CommandVariantCoverageTest#setVariantsCoverXxPxExatPxatAndConflicts`.
- **Command variant**: `SET / KEEPTTL` - `covered` - `Milestone1CompatTest#setGetAndKeepTtlBehaveAsExpected`.
- **Command variant**: `SET / conflicts` - `covered` - `Milestone1CompatTest#setRejectsConflictingModeOptions`.
- **Command variant**: `BITCOUNT / full string` - `covered` - `BitmapCommandTest#bitcountRangeFollowsRedisByteRangeRules`.
- **Command variant**: `BITCOUNT / positive byte range` - `covered` - `BitmapCommandTest#bitcountRangeFollowsRedisByteRangeRules`.
- **Command variant**: `BITCOUNT / negative byte range` - `covered` - `BitmapCommandTest#bitcountRangeFollowsRedisByteRangeRules`.
- **Command variant**: `BITCOUNT / invalid bounds` - `covered` - `CommandVariantCoverageTest#bitcountInvalidBoundsRejectNonIntegerRanges`.
- **Command variant**: `LPOP / single pop` - `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **Command variant**: `LPOP / counted pop` - `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **Command variant**: `LPOP / zero count` - `covered` - `Milestone1CompatTest#lpopCountHandlesNullArrayAndEmptyArray`.
- **Command variant**: `LPOP / negative count` - `covered` - `Milestone1CompatTest#lpopCountHandlesNullArrayAndEmptyArray`.
- **Command variant**: `RPOP / single pop` - `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **Command variant**: `RPOP / counted pop` - `covered` - `ListCommandTest#lpopRpopCountVariantsAndDeleteWhenEmpty`.
- **Command variant**: `RPOP / zero count` - `covered` - `CommandVariantCoverageTest#rpopCountVariantsCoverNullArrayEmptyArrayAndNegativeCount`.
- **Command variant**: `RPOP / negative count` - `covered` - `CommandVariantCoverageTest#rpopCountVariantsCoverNullArrayEmptyArrayAndNegativeCount`.
- **Command variant**: `ZRANGE / normal` - `covered` - `ZSetCommandTest#zrangeTieBreakIsRawByteLexAndBoundsWork`.
- **Command variant**: `ZRANGE / WITHSCORES` - `covered` - `ZSetCommandTest#zrangeTieBreakIsRawByteLexAndBoundsWork`.
- **Command variant**: `ZRANGE / REV` - `covered` - `ZSetCommandTest#zrevrangeAndZrangeRevReturnReverseOrder`.
- **Command variant**: `ZRANGE / bounds` - `covered` - `ZSetCommandTest#zrangeTieBreakIsRawByteLexAndBoundsWork`.
- **Command variant**: `ZRANGE / invalid option` - `covered` - `CommandErrorTest#arityAndSyntaxErrorsMatchExpectedMessages`.
- **Command variant**: `ZREVRANGE / normal` - `covered` - `ZSetCommandTest#zrevrangeAndZrangeRevReturnReverseOrder`.
- **Command variant**: `ZREVRANGE / WITHSCORES` - `covered` - `ZSetCommandTest#zrevrangeAndZrangeRevReturnReverseOrder`.
- **Command variant**: `ZREVRANGE / invalid option` - `covered` - `CommandVariantCoverageTest#zrevrangeInvalidOptionIsSyntaxError`.
- **Command variant**: `ZRANGEBYSCORE / inclusive bounds` - `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZRANGEBYSCORE / exclusive bounds` - `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZRANGEBYSCORE / infinities` - `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZRANGEBYSCORE / WITHSCORES` - `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZRANGEBYSCORE / LIMIT` - `covered` - `ZSetCommandTest#zrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZRANGEBYSCORE / invalid syntax` - `covered` - `CommandErrorTest#scoreRangeCommandsValidateArityAndLimitArguments`.
- **Command variant**: `ZREVRANGEBYSCORE / inclusive bounds` - `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZREVRANGEBYSCORE / exclusive bounds` - `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZREVRANGEBYSCORE / infinities` - `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZREVRANGEBYSCORE / WITHSCORES` - `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZREVRANGEBYSCORE / LIMIT` - `covered` - `ZSetCommandTest#zrevrangeByScoreRespectsBoundsLimitAndWithScores`.
- **Command variant**: `ZREVRANGEBYSCORE / invalid syntax` - `covered` - `CommandErrorTest#scoreRangeCommandsValidateArityAndLimitArguments`.
- **Command variant**: `FLUSHDB / default` - `covered` - `CommandVariantCoverageTest#flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode`.
- **Command variant**: `FLUSHDB / SYNC` - `covered` - `CommandVariantCoverageTest#flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode`.
- **Command variant**: `FLUSHDB / ASYNC` - `covered` - `CommandVariantCoverageTest#flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode`.
- **Command variant**: `FLUSHDB / invalid mode` - `covered` - `CommandVariantCoverageTest#flushdbVariantsCoverDefaultSyncAsyncAndInvalidMode`.

## Option And Subcommand Inventory

| Operation | Required variants |
| --- | --- |
| `COMMAND` | base, `COUNT`, `INFO`, unknown name |
| `CLIENT` | `SETINFO`, `SETNAME`, `GETNAME`, unknown subcommand |
| `HELLO` | RESP2, RESP3, `SETNAME`, unsupported proto, `AUTH`, disallowed in `MULTI` |
| `INFO` | no section, `yierdis`, `memory`, `keyspace`, unknown section |
| `MEMORY` | `STATS`, `USAGE`, invalid subcommand |
| `OBJECT` | `ENCODING`, invalid subcommand |
| `SCAN` | cursor, `MATCH`, `COUNT`, invalid cursor, duplicate option |
| `SET` | plain, `NX`, `XX`, `GET`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`, conflicts |
| `BITCOUNT` | full string, positive byte range, negative byte range, invalid bounds |
| `LPOP` | single pop, counted pop, zero count, negative count |
| `RPOP` | single pop, counted pop, zero count, negative count |
| `ZRANGE` | normal, `WITHSCORES`, `REV`, bounds, invalid option |
| `ZREVRANGE` | normal, `WITHSCORES`, invalid option |
| `ZRANGEBYSCORE` | inclusive bounds, exclusive bounds, infinities, `WITHSCORES`, `LIMIT`, invalid syntax |
| `ZREVRANGEBYSCORE` | inclusive bounds, exclusive bounds, infinities, `WITHSCORES`, `LIMIT`, invalid syntax |
| `FLUSHDB` | default, `SYNC`, `ASYNC`, invalid mode |

## DB API Inventory

| API method | Status | Evidence |
| --- | --- | --- |
| `StringReadOps.getStringBytes` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `StringReadOps.getStringValue` | `covered` | `OffHeapStringStorageTest#setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes` |
| `StringReadOps.strlen` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `StringReadOps.getBit` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `StringReadOps.bitcount` | `covered` | `StringDirectOpsTest#bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType` |
| `StringReadOps.bitcount(start,end)` | `covered` | `StringDirectOpsTest#bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType` |
| `StringWriteOps.set` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `StringWriteOps.setString(byte[])` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `StringWriteOps.setString(BytesSlice)` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `StringWriteOps.append` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `StringWriteOps.setBit` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `StringWriteOps.incrBy` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `HashReadOps.hget` | `covered` | `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl` |
| `HashReadOps.hgetall` | `covered` | `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl` |
| `HashReadOps.hlen` | `covered` | `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl` |
| `HashWriteOps.hset` | `covered` | `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl` |
| `HashWriteOps.hdel` | `covered` | `CollectionDirectOpsTest#hashHlenAndHdelCoverMissingNoOpWrongTypeAndTtl` |
| `ListReadOps.lrange` | `covered` | `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl` |
| `ListWriteOps.lpush` | `covered` | `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl` |
| `ListWriteOps.rpush` | `covered` | `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl` |
| `ListWriteOps.lpop` | `covered` | `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl` |
| `ListWriteOps.rpop` | `covered` | `CollectionDirectOpsTest#listPushPopCoverBothEndsMissingWrongTypeAndTtl` |
| `SetReadOps.smembers` | `covered` | `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion` |
| `SetReadOps.sismember` | `covered` | `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion` |
| `SetReadOps.scard` | `covered` | `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion` |
| `SetWriteOps.sadd` | `covered` | `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion` |
| `SetWriteOps.srem` | `covered` | `CollectionDirectOpsTest#setSremCoversMissingNoOpWrongTypeTtlAndEmptyDeletion` |
| `ZSetReadOps.zrange` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetReadOps.zrevrange` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetReadOps.zrangeByScore` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetReadOps.zrevrangeByScore` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetWriteOps.zadd` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetWriteOps.zremrangeByScore` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetWriteOps.zremrangeByRank` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `ZSetWriteOps.zrem` | `covered` | `CollectionDirectOpsTest#zsetReadsAndRemovalsCoverReverseScoreRankMissingWrongTypeAndTtl` |
| `HllReadOps.pfcount` | `covered` | `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics` |
| `HllWriteOps.pfadd` | `covered` | `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics` |
| `HllWriteOps.pfmerge` | `covered` | `CollectionDirectOpsTest#hllPfcountAndPfmergeCoverMissingWrongTypeTtlAndDestinationSemantics` |
| `KeyspaceReadOps.typeOf` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `KeyspaceReadOps.existsKey` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `KeyspaceReadOps.keys` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `KeyspaceReadOps.scan` | `covered` | `NativeStorageRegressionTest#stringPublicOpsUseNativeRecordsWithoutCompatibilityStoreEntries` |
| `KeyspaceWriteOps.del` | `covered` | `OffHeapStringStorageTest#setGetUsesNativeStringSliceAndDelFreesStableAllocatorBytes` |
| `TtlReadOps.ttlSeconds` | `covered` | `ExpireIndexTest#ttlBytesViewLazilyDeletesExpiredKeys` |
| `TtlReadOps.ttlMillis` | `covered` | `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup` |
| `TtlWriteOps.expire` | `covered` | `ExpireIndexTest#ttlAccountingAffectsUsedBytesForMaxmemory` |
| `TtlWriteOps.pexpire` | `covered` | `StringDirectOpsTest#bitcountSupportsWholeStringRangesMissingKeysTtlAndWrongType` |
| `TtlWriteOps.expireAtSeconds` | `covered` | `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup` |
| `TtlWriteOps.expireAtMillis` | `covered` | `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup` |
| `TtlWriteOps.persist` | `covered` | `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup` |
| `DbLifecycleOps.flushDb` | `covered` | `TtlLifecycleDirectOpsTest#lifecycleFlushDbAndMemoryObjectApisCoverExistingMissingAndAccessors` |
| `MemoryOps.memoryUsage` | `covered` | `YierdisDbMemoryReporterTest#directMemoryUsageReadsNativeEntryAndValueMetadata` |
| `MemoryOps.memoryStats` | `covered` | `YierdisDbMemoryReporterTest#memoryStatsIncludesFfmNativeBytesWhenEnabledForMaxmemory` |
| `MemoryOps.objectEncoding` | `covered` | `YierdisDbIntrospectionTest#objectEncodingReadsNativeEntryEncoding` |
| `ExpirationManager.cleanupExpired` | `covered` | `TtlLifecycleDirectOpsTest#ttlMillisAndAbsoluteExpirationCoverMissingPersistentExpiredAndCleanup` |
| `DbEngine.reads` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `DbEngine.writes` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `DbEngine.expiration` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `DbEngine.memory` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |
| `DbEngine.lifecycle` | `covered` | `StringDirectOpsTest#setCoversByteArraySliceModesReturnOldTtlAndMemoryLimit` |

## Native/Internal Inventory

| Area | Status | Evidence |
| --- | --- | --- |
| `EntryRecord` metadata | `covered` | `EntryTableContractTest#entryRecordCarriesNativeMetadata` |
| `EntryTable` allocation and release | `covered` | `EntryTableContractTest#entryTableAllocatesAndReleasesHandles` |
| `EntryTable` replacement | `covered` | `EntryTableContractTest#entryTableReplacesRecordsInPlaceAndUsesStableNativeHandle` |
| `EntryTable` stale handle after slot reuse | `covered` | `EntryTableContractTest#entryTableRejectsStaleReleasedHandleAfterSlotReuse` |
| `EntryHandle` stable native handle wrapping | `covered` | `EntryHandleContractTest#entryHandleWrapsProductionNativeHandle` and `EntryHandleContractTest#entryHandleRejectsWrongNativeDomainOrKind` |
| `ValueHandle` stable native handle wrapping and null sentinel | `covered` | `ValueHandleContractTest#valueHandleWrapsProductionNativeHandle`, `ValueHandleContractTest#valueHandleRejectsReservedNonNullRawValues`, and `ValueHandleContractTest#valueHandleAllowsNativeNullOnlyAsSentinel` |
| `NativeHandle` ABI bit layout and validation | `covered` | `NativeHandleTest#encodesAndDecodesHandleFields`, `NativeHandleTest#rejectsOutOfRangeFields`, `NativeHandleTest#rejectsMismatchedDomainAndKind`, and `NativeHandleTest#rejectsNonZeroReservedDomain` |
| `NativeAllocator` stats / defrag / epoch API records | `covered` | `NativeAllocatorContractTest#statsRecordExposesProductionAllocatorCounters`, `NativeAllocatorContractTest#defragResultFactoriesExposeMovementOutcomes`, `NativeAllocatorContractTest#defragCycleRecordsExposeBudgetsAndCounters`, and `NativeAllocatorContractTest#epochKindsCoverAllocatorReadSafetyScopes` |
| `YierdisNativeObjectTable` metadata, generation, quarantine, stale and wrong-kind/domain detection | `covered` | `YierdisNativeObjectTableTest#allocatesGenerationBearingHandlesAndStoresMetadata`, `YierdisNativeObjectTableTest#freeIncrementsGenerationBeforeReuse`, `YierdisNativeObjectTableTest#generationWrapRetiresSlot`, `YierdisNativeObjectTableTest#pinnedFreeQuarantinesUntilUnpin`, and `YierdisNativeObjectTableTest#rejectsWrongKindAndDomainAndDoubleFree` |
| `YierdisNativePageAllocator` small size class, medium/large span, and accounting | `covered` | `YierdisNativePageAllocatorTest#choosesSmallSizeClasses`, `YierdisNativePageAllocatorTest#smallAllocationsNeverCrossPageBoundary`, `YierdisNativePageAllocatorTest#allocatesMediumSpansForObjectsAboveSmallLimit`, `YierdisNativePageAllocatorTest#allocatesLargeSpansForObjectsAboveOneMiB`, and `YierdisNativePageAllocatorTest#reportsCommittedUsedAndFreeBytes` |
| `YierdisStableNativeAllocator` resolve, stale detection, pin/quarantine, epoch, and views | `covered` | `YierdisStableNativeAllocatorTest#allocatesFromPageAllocatorAndRecordsNativeMetadata`, `YierdisStableNativeAllocatorTest#detectsUseAfterFree`, `YierdisStableNativeAllocatorTest#freePinnedObjectQuarantinesUntilUnpin`, `YierdisStableNativeAllocatorTest#activeEpochDelaysFreedSlotReuseUntilClosed`, `YierdisStableNativeAllocatorTest#resolvedViewPinsObjectUntilClosed`, and `YierdisStableNativeAllocatorTest#readOnlyViewRejectsMutation` |
| `YierdisStableNativeAllocator` realloc semantics and rollback | `covered` | `YierdisStableNativeAllocatorTest#reallocPinnedObjectFailsWithoutChangingObject`, `YierdisStableNativeAllocatorTest#reallocPreservesHandlePrefixAndUpdatesMetadataWhenMoved`, `YierdisStableNativeAllocatorTest#reallocNoMoveGrowsWithinCapacityAfterShrink`, `YierdisStableNativeAllocatorTest#reallocNoMoveFailsWithoutChangingObjectWhenGrowthNeedsMove`, and `YierdisStableNativeAllocatorTest#preservePrefixGrowsWithinCapacityAfterShrinkWithoutMove` |
| `YierdisStableNativeAllocator` active defrag move protocol, budgets, rollback, metrics, and stress | `covered` | `YierdisStableNativeAllocatorTest#defragMovesUnpinnedObjectWithoutChangingHandle`, `YierdisStableNativeAllocatorTest#activeEpochDelaysDefragOldBlockReleaseUntilClosed`, `YierdisStableNativeAllocatorTest#defragSkipsPinnedAndOverBudgetObjects`, `YierdisStableNativeAllocatorTest#defragCycleMovesEligibleObjectsWithinByteBudget`, `YierdisStableNativeAllocatorTest#defragValidationFailureRollsBackMove`, `YierdisStableNativeAllocatorTest#statsExposeProductionAllocatorMetrics`, and `YierdisStableNativeAllocatorTest#deterministicAllocatorChurnStressMaintainsAccounting` |
| `KeyHandle`, `HeapKeyHandle`, `FfmKeyHandle` equality and hash stability | `covered` | `KeyHandleContractTest#keyHandleEqualityIsContentBasedAcrossHeapAndFfm` and `KeyHandleContractTest#keyHandleDistinguishesDifferentKeys` |
| `NativeKeyDirectory` lookup, insert, replacement, removal, scan, random selection, and growth | `covered` | `NativeKeyDirectoryTest#nativeKeyDirectoryMapsKeysToStableHandlesAndReleasesThem`, `NativeKeyDirectoryTest#nativeKeyDirectoryExposesKeyHandlesForScanAndRandomSelection`, and `NativeKeyDirectoryTest#nativeKeyDirectoryScanCanStopEarlyAndRandomKeyIsNullWhenEmpty` |
| `YierdisFfmBlobStore` allocation and release | `covered` | `YierdisFfmBlobStoreTest#storeRetainReleaseTracksLiveBytesUntilFinalRelease` |
| `YierdisFfmKeyspace` allocation failure cleanup | `covered` | `YierdisFfmKeyspaceTest#computeWithHandleReleasesNewKeyWhenRemappingThrows` and `YierdisFfmKeyspaceTest#computeWithHandleKeepsExistingEntryWhenRemappingThrows` |
| `ByteArrayKeyspace` binary lookup, scan, tombstone, and rehash | `covered` | `ByteArrayKeyspaceTest#sliceLookupFindsExistingKeysAndReturnsCanonicalKey`, `ByteArrayKeyspaceTest#computeGetAndForEachWorkAcrossRehash`, and `ByteArrayKeyspaceTest#tombstonesTriggerRebuildWithoutGrowing` |
| `StringRoot` raw bytes, integer-like bytes, spare capacity, and bitmap growth | `covered` | `StringRootTest#stringRootOverwritesWithoutReintroducingHeapPayloads`, `StringRootTest#stringRootStoresIntegerLikeBytesAsRawNativeBytes`, `StringRootTest#stringRootOverwriteReusesSpareCapacityForShorterValue`, and `StringRootTest#stringRootEnsureLengthSupportsBitmapStyleGrowthWithZeroFill` |
| `ListRoot` push, pop, release, and streaming | `covered` | `ListRootTest#listRootSupportsPushPopAndStreaming` |
| `HashRoot`, `SetRoot`, and `ZSetRoot` round trips | `covered` | `CollectionRootTest#hashSetAndZsetRootsRoundTripMembers` |
| `ListValue` packed and quicklist paths | `covered` | `ListValueTest#packedListPreservesNullVsEmptyElements` and `ListValueTest#quicklistSplitsByBytesAndMerges` |
| `HashValue` packed and hashtable paths | `covered` | `HashValueTest#packedHashSupportsUpdateAndDeleteWithRepacking` and `HashValueTest#hashConvertsToHashTableAfterTooManyFields` |
| `SetValue` intset and hashtable paths | `covered` | `SetValueTest#ffmSetKeepsIntsetMembersOffHeapAndUpgradesToHashtable` |
| `ZSetValue` listpack and skiplist paths | `covered` | `ZSetValueTest#packedZSetKeepsScoreOrderingAndSupportsUpdates` and `ZSetValueTest#zsetUpgradesAfterTooManyEntries` |
| `YierdisHyperLogLog` sparse, dense, merge, and byte round trip | `covered` | `YierdisHyperLogLogTest#sparseHllAddsElementsAndMergesIntoRegisters`, `YierdisHyperLogLogTest#denseHllUpdatesInPlaceAndMergesViaBytesSlice`, and `YierdisHyperLogLogTest#denseBytesFromRegistersClampsAndRoundTripsThroughMerge` |
| `YierdisExpireIndex`, `YierdisHeapExpireIndex`, and `YierdisFfmExpireIndex` lookup and clear | `covered` | `ExpireIndexContractTest#heapExpireIndexRoundTripsHandleLookupAndClear` and `ExpireIndexContractTest#ffmExpireIndexRoundTripsHandleLookupAndClear` |
| `YierdisDbMemoryLedger`, `MemoryLedger`, and `InMemoryLedger` reserve, commit, and rollback | `covered` | `MemoryLedgerContractTest#reserveCommitRollbackMaintainInvariants` |
| `YierdisDbMutationExecutor` cleanup on failed mutation plans and no-op accounting | `covered` | `MutationExecutorReservationTest#failedMutationRollsBackReservationAndDoesNotPoisonNextMutation`, `MutationExecutorReservationTest#noevictionRejectsBeforeMutationCanRun`, and `MutationExecutorReservationTest#appendAndSetbitNoopsDoNotMarkValueChanged` |
| `YierdisDbMemoryEstimator` and `YierdisDbMemoryReporter` | `covered` | `YierdisDbMemoryEstimatorTest#estimatesWriteUpperBoundsAndByteSums` and `YierdisDbMemoryReporterTest#directMemoryUsageReadsNativeEntryAndValueMetadata` |
| `YierdisDbIntrospection` | `covered` | `YierdisDbIntrospectionTest#objectEncodingReadsNativeEntryEncoding` and `YierdisDbIntrospectionTest#snapshotCopiesNativeStringValueAndExpireMetadata` |
| Maxmemory candidate sampling, eviction, noeviction, reserve, commit, and rollback | `covered` | `MaxmemoryEvictionTest#allkeysRandomEvictsToStayWithinLimit`, `MaxmemoryEvictionTest#allkeysLruEvictsLeastRecentlyUsedWhenSamplesCoverAllKeys`, and `MutationExecutorReservationTest#noevictionRejectsBeforeMutationCanRun` |

## Current Gap Queue

1. Expand command option rows into one narrow test per option group where current rows rely on shared family tests, especially score-range syntax and SCAN option duplication.
2. Keep adding direct DB API and native/internal rows whenever new public API methods or native structures are introduced.
