package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javax.xml.parsers.DocumentBuilderFactory;

public class ArchitectureBoundaryTest {
    @Test
    public void productionHardeningOperationsMustRemainDocumented() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("cannot locate repository root", repoRoot);

        Path operationsGuide = repoRoot.resolve("docs/project-docs/production-hardening-operations.md");
        Assert.assertTrue("missing production hardening operations guide", Files.isRegularFile(operationsGuide));
        String guide = Files.readString(operationsGuide, StandardCharsets.UTF_8);
        for (String requiredTerm : List.of(
                "JDK 25",
                "--replyGlobalCapacityBytes",
                "268435456",
                "--replyPerConnectionCapacityBytes",
                "134217728",
                "--replyMaxTotalBytes",
                "67108864",
                "--replyChunkPayloadBytes",
                "65536",
                "--replyControlReservationBytes",
                "4096",
                "--replyDrainTimeoutMillis",
                "5000",
                "--protocolGlobalInFlightBytes",
                "commit-stream",
                "maxmemory",
                "FAIR",
                "GLOBAL",
                "result-unknown",
                "graceful shutdown",
                "production-hardening-soak.sh",
                "smoke.sh",
                "0.90",
                "GET",
                "SET",
                "HSET",
                "ZADD",
                "yierdis_native_live_objects",
                "yierdis_native_live_regions",
                "RSS",
                "16 MiB"
        )) {
            Assert.assertTrue(
                    "production hardening operations guide is missing: " + requiredTerm,
                    guide.contains(requiredTerm)
            );
        }

        String readme = Files.readString(repoRoot.resolve("README.md"), StandardCharsets.UTF_8);
        String logicIndex = Files.readString(
                repoRoot.resolve("docs/project-docs/core-logic-index.md"),
                StandardCharsets.UTF_8
        );
        Assert.assertTrue(
                "README must link to the production hardening operations guide",
                readme.contains("production-hardening-operations.md")
        );
        Assert.assertTrue(
                "core logic index must link to the production hardening operations guide",
                logicIndex.contains("production-hardening-operations.md")
        );

        for (Path legacyDocument : List.of(
                repoRoot.resolve("README.md"),
                repoRoot.resolve("docs/project-docs")
        )) {
            String content;
            if (Files.isDirectory(legacyDocument)) {
                try (Stream<Path> files = Files.walk(legacyDocument)) {
                    content = files
                            .filter(path -> path.toString().endsWith(".md"))
                            .map(path -> {
                                try {
                                    return Files.readString(path, StandardCharsets.UTF_8);
                                } catch (IOException e) {
                                    throw new IllegalStateException(e);
                                }
                            })
                            .reduce("", (left, right) -> left + '\n' + right);
                }
            } else {
                content = Files.readString(legacyDocument, StandardCharsets.UTF_8);
            }
            Assert.assertFalse("legacy docs still describe one growable ByteBuf", content.contains("one growable ByteBuf"));
            Assert.assertFalse("legacy docs still make command handlers publish changes", content.contains("command handlers publish changes"));
            Assert.assertFalse(
                    "legacy docs still allow successful close with active leases",
                    content.contains("successful close can ignore active leases")
            );
            for (String removedPath : List.of(
                    "CommandChangeEmitter",
                    "CommandChangeObserver",
                    "RuntimeChangeSinkCommandChangeObserver",
                    "YierdisChangeEventBridge",
                    "DbChangeContext",
                    "createWithDefaults",
                    "shared off-heap usage source",
                    "live region set"
            )) {
                Assert.assertFalse(
                        "project docs still describe retired production-hardening path: " + removedPath,
                        content.contains(removedPath)
                );
            }
        }
    }

    @Test
    public void commonMemoryMustRemainAProductionDependencyFreeContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("cannot locate repository root", repoRoot);

        Path commonPom = repoRoot.resolve("yierdis-common/pom.xml");
        Path modulePom = repoRoot.resolve("yierdis-common/yierdis-common-memory/pom.xml");
        Path rootPom = repoRoot.resolve("pom.xml");
        Assert.assertTrue(Files.readString(commonPom).contains("<module>yierdis-common-memory</module>"));
        Assert.assertTrue(Files.readString(rootPom).contains("<artifactId>yierdis-common-memory</artifactId>"));
        Assert.assertTrue(pomProductionDependencyArtifactIds(modulePom).isEmpty());

        Path policyFile = repoRoot.resolve(
                "yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml"
        );
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String section = policySection(policy, "yierdis-common-memory");
        Assert.assertTrue(section.contains("allowed_dependencies: []"));
        for (String forbidden : List.of(
                "yierdis-memory-api",
                "yierdis-memory-ffm",
                "yierdis-db-api",
                "yierdis-db-memory",
                "yierdis-server-api",
                "yierdis-server-runtime",
                "yierdis-networking-resp",
                "yierdis-networking-netty",
                "netty-all",
                "yier.bubu.redis.memory",
                "yier.bubu.redis.storage",
                "yier.bubu.redis.execution",
                "yier.bubu.redis.runtime",
                "yier.bubu.redis.protocol",
                "io.netty"
        )) {
            Assert.assertTrue("common-memory policy must forbid " + forbidden, section.contains(forbidden));
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-common/yierdis-common-memory/src/main/java"),
                offenders,
                "import yier.bubu.redis.memory",
                "import yier.bubu.redis.storage",
                "import yier.bubu.redis.execution",
                "import yier.bubu.redis.runtime",
                "import yier.bubu.redis.protocol",
                "import io.netty"
        );
        Assert.assertTrue("common-memory source guard scanned no Java files", scanned > 0);
        Assert.assertTrue("common-memory leaked upper-layer imports: " + offenders, offenders.isEmpty());
    }

    @Test
    public void dbOpsAndCoreCommandMustNotImportProtocolModel() throws IOException {
        List<String> offenders = new ArrayList<>();
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        int scanned = 0;
        scanned += scanForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                commandApiMain(repoRoot),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                commandKernelMain(repoRoot),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                commandDefaultsMain(repoRoot),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到协议模型依赖泄漏（core-db/storage-api/command-* 禁止 import yier.bubu.redis.protocol.*）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeMustNotOwnCommandAssemblyAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path instanceFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java，无法执行 runtime 边界护栏", Files.isRegularFile(instanceFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                instanceFile,
                offenders,
                "import yier.bubu.redis.command.api.ServerInfoProvider;",
                "import yier.bubu.redis.command.api.SlowCommandGovernor;",
                "import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "newCommandProcessor("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 runtime-embedded 重新承担命令处理器组装/装配职责（YierdisInstance 应只负责 DB 生命周期与路由）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreDefaultsMustNotOwnServerFacingCommands() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        Path serverCommands = repoRoot.resolve(
                "yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/ServerCommands.java"
        );
        if (Files.exists(serverCommands)) {
            offenders.add(relativePath(repoRoot, serverCommands) + " (server-facing commands should live in yierdis-server-main)");
        }

        Path processorFile = repoRoot.resolve(
                "yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisFastCommandProcessor.java，无法执行 command-kernel 默认装配护栏", Files.isRegularFile(processorFile));
        scanFileForForbiddenText(
                repoRoot,
                processorFile,
                offenders,
                "new ServerCommands(",
                "ERR HELLO is not allowed in MULTI",
                "asciiEqualsIgnoreCase(cmd, 0, \"HELLO\")",
                "descriptorAwareRegistration(",
                "coreDescriptor(",
                "coreDefaultArity(",
                "coreDefaultFirstKeyIndex(",
                "coreDefaultLastKeyIndex(",
                "coreDefaultKeyStep("
        );
        scanForForbiddenText(
                repoRoot,
                commandDefaultsMain(repoRoot),
                offenders,
                "register(\"HELLO\"",
                "register(\"INFO\"",
                "register(\"STATS\""
        );
        Path coreConnectionFile = repoRoot.resolve(
                "yierdis-command/yierdis-command-builtin/src/main/java/yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java"
        ).normalize();
        Assert.assertTrue("缺少 CoreConnectionCommands.java，无法执行 COMMAND metadata 护栏", Files.isRegularFile(coreConnectionFile));
        scanFileForForbiddenText(
                repoRoot,
                coreConnectionFile,
                offenders,
                "case \"HELLO\":",
                "case \"INFO\":",
                "case \"STATS\":"
        );
        Path syntaxFile = repoRoot.resolve(
                "yierdis-command/yierdis-command-api/src/main/java/yier/bubu/redis/command/api/CommandSyntax.java"
        ).normalize();
        Assert.assertTrue("缺少 CommandSyntax.java，无法执行 COMMAND metadata 护栏", Files.isRegularFile(syntaxFile));
        scanFileForForbiddenText(
                repoRoot,
                syntaxFile,
                offenders,
                "case \"HELLO\":",
                "case \"INFO\":",
                "case \"STATS\":"
        );
        Path registryFile = repoRoot.resolve(
                "yierdis-command/yierdis-command-core/src/main/java/yier/bubu/redis/command/kernel/CommandRegistry.java"
        ).normalize();
        Assert.assertTrue("缺少 CommandRegistry.java，无法执行 COMMAND metadata fallback 护栏", Files.isRegularFile(registryFile));
        scanFileForForbiddenText(
                repoRoot,
                registryFile,
                offenders,
                "defaultDescriptorForNameUpper(",
                "defaultArity(",
                "defaultFirstKeyIndex(",
                "defaultLastKeyIndex(",
                "defaultKeyStep("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server-facing commands 回流到 core 默认装配（这些命令应由 yierdis-server-main 组装）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreCommandMustNotReferenceLegacyWriteReservationApis() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                commandDefaultsMain(repoRoot),
                offenders,
                ".values()",
                ".eviction()",
                "prepareWrite(",
                "rollbackWriteReservationIfAny(",
                "DbMemoryConstants"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 command-defaults 仍依赖 legacy 写预留/混合 DB API：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreCommandMustStayIndependentFromMemoryApi() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String apiPolicy = policySection(policy, "yierdis-command-api");
        String kernelPolicy = policySection(policy, "yierdis-command-core");
        String defaultsPolicy = policySection(policy, "yierdis-command-builtin");
        Assert.assertTrue(
                "command-api policy must forbid direct memory-api dependency",
                apiPolicy.contains("yierdis-memory-api")
        );
        Assert.assertTrue(
                "command-kernel policy must forbid direct memory-api dependency",
                kernelPolicy.contains("yierdis-memory-api")
        );
        Assert.assertTrue(
                "command-defaults policy must forbid direct memory-api dependency",
                defaultsPolicy.contains("yierdis-memory-api")
        );
        Assert.assertTrue(
                "command policies must forbid offheap API imports",
                apiPolicy.contains("yier.bubu.redis.memory.api")
                        && kernelPolicy.contains("yier.bubu.redis.memory.api")
                        && defaultsPolicy.contains("yier.bubu.redis.memory.api")
        );

        for (Path commandPom : List.of(
                repoRoot.resolve("yierdis-command/yierdis-command-api/pom.xml").normalize(),
                repoRoot.resolve("yierdis-command/yierdis-command-core/pom.xml").normalize(),
                repoRoot.resolve("yierdis-command/yierdis-command-builtin/pom.xml").normalize()
        )) {
            Assert.assertTrue("缺少 command module pom.xml: " + commandPom, Files.isRegularFile(commandPom));
            String pom = Files.readString(commandPom, StandardCharsets.UTF_8);
            Assert.assertFalse(
                    "command modules must not depend on yierdis-memory-api: " + commandPom,
                    pom.contains("<artifactId>yierdis-memory-api</artifactId>")
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanCommandMainForForbiddenText(
                repoRoot,
                offenders,
                "import yier.bubu.redis.memory.api.",
                "yier.bubu.redis.memory.api."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 command-* Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 command-* 依赖 memory-api/offheap API（命令层只能接收 DB/API 层转换后的错误）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void migratedCommandsDoNotWriteSyntaxErrorsDirectly() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "StringCommands.java"),
                "parseSet(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "StringCommands.java"),
                "set(SetArgs args, CommandContext ctx)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "KeyCommands.java"),
                "parseScan(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "KeyCommands.java"),
                "scan(ScanArgs args, CommandContext ctx)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "ZSetCommands.java"),
                "parseZRange(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                commandDefaultsFile(repoRoot, "ZSetCommands.java"),
                "parseZRangeByScore(ArgReader args, boolean reverse)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void dbLayerDoesNotOwnCommandPairTailSyntax() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisHashOps.java"),
                offenders,
                "wrong number of arguments for 'hset' command"
        );
        scanFileForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisZSetOps.java"),
                offenders,
                "wrong number of arguments for 'zadd' command"
        );
        Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void replaySurfacesMustUseExecutionContracts() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java"
                ).normalize(),
                offenders,
                "byte[][] argv"
        );
        Path transactionStateFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/TransactionState.java"
        ).normalize();
        Assert.assertTrue("缺少 execution-api TransactionState.java，无法约束事务回放 surface", Files.isRegularFile(transactionStateFile));
        scanFileForForbiddenText(
                repoRoot,
                transactionStateFile,
                offenders,
                "enqueue(byte[][]",
                "tryEnqueue(byte[][]",
                "List<?> drain()",
                "drainRequests()"
        );
        scanFileForForbiddenText(
                repoRoot,
                engineRoot(repoRoot).resolve("src/main/java/yier/bubu/redis/execution/engine/EngineSession.java"),
                offenders,
                "ArrayList<byte[][]>",
                "List<byte[][]>",
                "tryEnqueue(byte[][]"
        );
        scanFileForForbiddenText(
                repoRoot,
                commandKernelFile(repoRoot, "TransactionCommands.java"),
                offenders,
                "drainRequests(",
                "new QueuedCommand(",
                "new QueuedExecutionRequest("
        );
        scanFileForForbiddenText(
                repoRoot,
                commandKernelFile(repoRoot, "YierdisFastCommandProcessor.java"),
                offenders,
                "tx.tryEnqueue(ByteArrayExecutionRequest.copyOf(request))"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java").normalize(),
                offenders,
                "SimpleChannelInboundHandler<Command>"
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到旧的 Command 生产路径、事务回放/变更事件 argv 容器，或错误的快照 ownership 重新出现；这些边界必须继续统一到 ExecutionRequest/ExecutionRecord：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void productionCommandsMustRegisterTypedCommandSpecs() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        String deletedDescriptor = "Command" + "Descriptor";
        Path deletedDescriptorFile = commandApiMain(repoRoot)
                .resolve("yier/bubu/redis/command/api/" + deletedDescriptor + ".java");
        Assert.assertFalse("retired command metadata contract must stay deleted", Files.exists(deletedDescriptorFile));
        String[] retiredCommandApis = {
                deletedDescriptor,
                "registerDisallowed" + "InMulti",
                "isTransaction" + "Control",
                "CommandParsers." + "exact",
                "CommandParsers." + "min",
                "CommandParsers." + "range",
                "CommandParsers." + "oneOf",
                "CommandParsers." + "pairTail"
        };

        List<String> offenders = new ArrayList<>();
        scanCommandMainForForbiddenText(
                repoRoot,
                offenders,
                "CommandModule.Handler",
                "new CommandSpec(",
                "register(String name, Handler"
        );
        scanCommandMainForForbiddenText(repoRoot, offenders, retiredCommandApis);
        scanFilesMatchingRegex(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command"),
                offenders,
                "registration\\.register\\(\\s*\"[A-Z0-9_]+\"\\s*,\\s*this::"
        );
        scanFilesMatchingRegex(
                repoRoot,
                commandKernelMain(repoRoot).resolve("yier/bubu/redis/command"),
                offenders,
                "registration\\.register\\(\\s*\"[A-Z0-9_]+\"\\s*,\\s*this::"
        );
        scanFilesMatchingRegex(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server").normalize(),
                offenders,
                "registration\\.register\\(\\s*\"[A-Z0-9_]+\"\\s*,\\s*this::"
        );
        scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize(),
                offenders,
                retiredCommandApis
        );

        Assert.assertTrue("legacy command registration remains:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void productionCodeMustNotUseDeprecatedCommandRequestCompatibility() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path commandAlias = repoRoot.resolve(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Command.java"
        ).normalize();
        Assert.assertFalse(
                "Command compatibility alias must be deleted; use ExecutionRequest directly",
                Files.exists(commandAlias)
        );

        List<String> offenders = new ArrayList<>();
        scanCommandMainForForbiddenText(
                repoRoot,
                offenders,
                "import yier.bubu.redis.execution.api.Command;",
                "instanceof yier.bubu.redis.execution.api.Command",
                "execute(Command"
        );
        scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.execution.api.Command;",
                "SimpleChannelInboundHandler<Command>",
                "instanceof Command"
        );

        Assert.assertTrue("deprecated Command compatibility remains:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void storagePressurePathsMustUseKeyHandles() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/MaxmemoryCandidate.java"),
                offenders,
                "byte[] key"
        );
        scanFileForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java"),
                offenders,
                ".randomKey()"
        );
        scanFileForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java"),
                offenders,
                ".randomKey()",
                ".forEach("
        );

        Assert.assertTrue("storage pressure paths must use KeyHandle identities:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void engineAndExecutorMustExposeSessionRequestReplyBoundary() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path engineFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java"
        );
        Assert.assertTrue("缺少 YierdisEngine.java，无法约束 engine 执行边界", Files.isRegularFile(engineFile));
        String engineSource = Files.readString(engineFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "YierdisEngine public execution boundary must expose CommandSession + ExecutionRequest + RedisReplyWriter",
                engineSource.contains("void execute(CommandSession session, ExecutionRequest request, RedisReplyWriter")
        );
        Assert.assertFalse(
                "YierdisEngine must not import or reference the deleted marker session",
                engineSource.contains("execution.api." + "Session")
        );
        Assert.assertFalse(
                "YierdisEngine public API must not expose CommandContext compatibility overloads",
                engineSource.contains("CommandContext")
        );

        Path executorEngineFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-executor/src/main/java/yier/bubu/redis/execution/executor/CommandExecutionEngine.java"
        ).normalize();
        Assert.assertTrue("缺少 CommandExecutionEngine.java，无法约束 executor 执行边界", Files.isRegularFile(executorEngineFile));
        String executorEngineSource = Files.readString(executorEngineFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "CommandExecutionEngine must accept CommandSession + ExecutionRequest + RedisReplyWriter",
                executorEngineSource.contains("void execute(CommandSession session, ExecutionRequest request, RedisReplyWriter")
        );
        Assert.assertFalse(
                "CommandExecutionEngine must not import or reference the deleted marker session",
                executorEngineSource.contains("execution.api." + "Session")
        );
        Assert.assertFalse(
                "executor-core execution seam must not expose CommandContext",
                executorEngineSource.contains("CommandContext")
        );
    }

    @Test
    public void executorCoreMustNotOwnCommandSessionSemantics() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-executor/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.execution.api.CommandContext;",
                "new CommandContext(",
                "import yier.bubu.redis.execution.api.ServerSession;",
                "import yier.bubu.redis.execution.api.TransactionState;",
                "DefaultExecutionSession",
                "DefaultTransactionState",
                "setDbIndex(",
                "dbIndex()",
                "clientName",
                "authenticated"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-executor Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 executor-core 重新持有命令上下文、DB 选择或事务语义（这些状态必须留在 EngineSession/engine-command 侧）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void commandParsingMustStayInCommandModules() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path serverRoot = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize();
        Path serverCommandModule = serverRoot.resolve("yier/bubu/redis/app/server/ServerCommandModule.java").normalize();
        List<Path> allowedServerFiles = List.of(serverCommandModule);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForForbiddenTextExcluding(
                repoRoot,
                serverRoot,
                offenders,
                allowedServerFiles,
                "import yier.bubu.redis.command.api.CommandParsers;",
                "import yier.bubu.redis.command.api.CommandSpec;",
                "import yier.bubu.redis.command.api.ArgReader;",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-executor/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                runtimeEmbeddedMain(repoRoot),
                offenders,
                "import yier.bubu.redis.command.api.CommandParsers;",
                "import yier.bubu.redis.command.api.CommandSpec;",
                "import yier.bubu.redis.command.api.ArgReader;",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        for (Path protocolMain : List.of(
                repoRoot.resolve("yierdis-networking/yierdis-networking-resp/src/main/java").normalize(),
                repoRoot.resolve("yierdis-networking/yierdis-networking-netty/src/main/java").normalize()
        )) {
            scanned += scanForForbiddenText(
                    repoRoot,
                    protocolMain,
                    offenders,
                    "import yier.bubu.redis.command.",
                    "CommandParseResult",
                    "wrong number of arguments for"
            );
        }
        scanned += scanForForbiddenText(
                repoRoot,
                storageMemoryMain(repoRoot),
                offenders,
                "import yier.bubu.redis.command.",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 command parsing 泄漏到 server/executor/runtime/protocol/storage（除 ServerCommandModule 的 server-local 命令注册外）：\n"
                    + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void commandModulesMustBeSplitIntoApiKernelAndDefaults() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-command",
                rootPomText.contains("<module>yierdis-command</module>")
        );
        Assert.assertFalse(
                "root pom.xml must not keep legacy yierdis-core-command dependency management",
                rootPomText.contains("<artifactId>yierdis-core-command</artifactId>")
        );
        for (String artifactId : List.of("yierdis-command-api", "yierdis-command-core", "yierdis-command-builtin")) {
            Assert.assertTrue(
                    "root dependencyManagement must include " + artifactId,
                    rootPomText.contains("<artifactId>" + artifactId + "</artifactId>")
            );
        }

        Path commandRoot = workspaceRoot.resolve("yierdis-command").normalize();
        Assert.assertTrue("缺少 yierdis-command/pom.xml", Files.isRegularFile(commandRoot.resolve("pom.xml")));
        String commandPom = Files.readString(commandRoot.resolve("pom.xml"), StandardCharsets.UTF_8);
        Assert.assertTrue("yierdis-command must aggregate command-api", commandPom.contains("<module>yierdis-command-api</module>"));
        Assert.assertTrue("yierdis-command must aggregate command-kernel", commandPom.contains("<module>yierdis-command-core</module>"));
        Assert.assertTrue("yierdis-command must aggregate command-defaults", commandPom.contains("<module>yierdis-command-builtin</module>"));

        Path commandApi = commandRoot.resolve("yierdis-command-api").normalize();
        Path commandKernel = commandRoot.resolve("yierdis-command-core").normalize();
        Path commandDefaults = commandRoot.resolve("yierdis-command-builtin").normalize();
        Assert.assertTrue("缺少 yierdis-command-api/pom.xml", Files.isRegularFile(commandApi.resolve("pom.xml")));
        Assert.assertTrue("缺少 yierdis-command-core/pom.xml", Files.isRegularFile(commandKernel.resolve("pom.xml")));
        Assert.assertTrue("缺少 yierdis-command-builtin/pom.xml", Files.isRegularFile(commandDefaults.resolve("pom.xml")));

        String kernelPom = Files.readString(commandKernel.resolve("pom.xml"), StandardCharsets.UTF_8);
        Assert.assertTrue("command-kernel must depend on command-api", kernelPom.contains("<artifactId>yierdis-command-api</artifactId>"));
        Assert.assertFalse("command-kernel must not depend on command-defaults", kernelPom.contains("<artifactId>yierdis-command-builtin</artifactId>"));

        String defaultsPom = Files.readString(commandDefaults.resolve("pom.xml"), StandardCharsets.UTF_8);
        Assert.assertTrue("command-defaults must depend on command-api", defaultsPom.contains("<artifactId>yierdis-command-api</artifactId>"));
        Assert.assertTrue("command-defaults must depend on execution-api", defaultsPom.contains("<artifactId>yierdis-server-api</artifactId>"));
        Assert.assertTrue("command-defaults must depend on storage-api", defaultsPom.contains("<artifactId>yierdis-db-api</artifactId>"));
        Assert.assertFalse("command-defaults must not depend on command-kernel", defaultsPom.contains("<artifactId>yierdis-command-core</artifactId>"));
        for (String concreteStorageArtifact : List.of("yierdis-core-db", "yierdis-db-memory")) {
            Assert.assertFalse(
                    "command-defaults must not depend on concrete storage " + concreteStorageArtifact,
                    defaultsPom.contains("<artifactId>" + concreteStorageArtifact + "</artifactId>")
            );
        }

        Path enginePom = engineRoot(repoRoot).resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server-core/pom.xml", Files.isRegularFile(enginePom));
        String enginePomText = Files.readString(enginePom, StandardCharsets.UTF_8);
        Assert.assertTrue("engine must depend on command-api", enginePomText.contains("<artifactId>yierdis-command-api</artifactId>"));
        Assert.assertTrue("engine must depend on command-kernel", enginePomText.contains("<artifactId>yierdis-command-core</artifactId>"));
        Assert.assertFalse("engine must not depend on command-defaults", enginePomText.contains("<artifactId>yierdis-command-builtin</artifactId>"));

        Path serverAppPom = workspaceRoot.resolve("yierdis-server/yierdis-server-main/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/pom.xml", Files.isRegularFile(serverAppPom));
        String serverAppPomText = Files.readString(serverAppPom, StandardCharsets.UTF_8);
        Assert.assertTrue("server-app must compose command-kernel", serverAppPomText.contains("<artifactId>yierdis-command-core</artifactId>"));
        Assert.assertTrue("server-app must compose command-defaults", serverAppPomText.contains("<artifactId>yierdis-command-builtin</artifactId>"));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                commandDefaults.resolve("src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.kernel.YierdisFastCommandProcessor;",
                "import yier.bubu.redis.command.kernel.CommandRegistry;",
                "new CommandRegistry("
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-command-builtin Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 command-defaults 依赖 command-kernel 内部注册/处理器，而不是通过 command-api factory 暴露默认命令模块：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void commandProductionPackagesMustRevealModuleOwnership() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanCommandMainForForbiddenText(
                repoRoot,
                offenders,
                "package yier.bubu.redis.command;"
        );
        Assert.assertTrue("architecture guard scanned no command production Java files", scanned > 0);
        Assert.assertTrue(
                "command production code must not remain in the shared root command package:\n"
                        + String.join("\n", offenders),
                offenders.isEmpty()
        );

        assertPackageDeclaration(
                repoRoot,
                commandApiMain(repoRoot).resolve("yier/bubu/redis/command/api/CommandSyntax.java"),
                "package yier.bubu.redis.command.api;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandKernelMain(repoRoot).resolve("yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"),
                "package yier.bubu.redis.command.kernel;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/DefaultCommandModules.java"),
                "package yier.bubu.redis.command.defaults;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/string/StringCommands.java"),
                "package yier.bubu.redis.command.defaults.string;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/hash/HashCommands.java"),
                "package yier.bubu.redis.command.defaults.hash;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/list/ListCommands.java"),
                "package yier.bubu.redis.command.defaults.list;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/set/SetCommands.java"),
                "package yier.bubu.redis.command.defaults.set;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/zset/ZSetCommands.java"),
                "package yier.bubu.redis.command.defaults.zset;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/hll/HllCommands.java"),
                "package yier.bubu.redis.command.defaults.hll;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/keyspace/KeyCommands.java"),
                "package yier.bubu.redis.command.defaults.keyspace;"
        );
        assertPackageDeclaration(
                repoRoot,
                commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/connection/CoreConnectionCommands.java"),
                "package yier.bubu.redis.command.defaults.connection;"
        );
    }

    @Test
    public void respProtocolModulesMustKeepTheirBoundaries() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path respRoot = workspaceRoot.resolve("yierdis-networking/yierdis-networking-resp").normalize();
        Path nettyRoot = workspaceRoot.resolve("yierdis-networking/yierdis-networking-netty").normalize();
        Assert.assertTrue("缺少 yierdis-networking-resp/pom.xml", Files.isRegularFile(respRoot.resolve("pom.xml")));
        Assert.assertTrue("缺少 yierdis-networking-netty/pom.xml", Files.isRegularFile(nettyRoot.resolve("pom.xml")));

        String respPom = Files.readString(respRoot.resolve("pom.xml"), StandardCharsets.UTF_8);
        Assert.assertTrue("resp module must depend on server-api for ReplyWriter integration", respPom.contains("<artifactId>yierdis-server-api</artifactId>"));
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-contract</artifactId>",
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-server-runtime-api</artifactId>",
                "<artifactId>yierdis-db-api</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse("resp module must not depend on " + forbiddenDependency, respPom.contains(forbiddenDependency));
        }
        String nettyPom = Files.readString(nettyRoot.resolve("pom.xml"), StandardCharsets.UTF_8);
        Assert.assertTrue("resp-netty must depend on resp module", nettyPom.contains("<artifactId>yierdis-networking-resp</artifactId>"));
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-server-runtime-api</artifactId>",
                "<artifactId>yierdis-db-api</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>"
        )) {
            Assert.assertFalse("resp-netty must not depend on " + forbiddenDependency, nettyPom.contains(forbiddenDependency));
        }

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForForbiddenText(
                repoRoot,
                respRoot.resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.storage.api.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.storage.api.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                nettyRoot.resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.storage.api.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.app.server.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.storage.api.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.app.server."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 resp Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 RESP wire/execution/netty 模块边界违规：\n"
                            + String.join("\n", offenders)
            );
        }

        Assert.assertFalse(
                "server production code must not own RespReplyWriter implementation",
                Files.isRegularFile(workspaceRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/protocol/resp/RespReplyWriter.java"))
        );
        Assert.assertFalse(
                "server production code must not own RespCommandAdapter implementation",
                Files.isRegularFile(workspaceRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RespCommandAdapter.java"))
        );
    }

    @Test
    public void executionApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-server",
                rootPomText.contains("<module>yierdis-server</module>")
        );

        Path executionPom = workspaceRoot.resolve("yierdis-server/pom.xml").normalize();
        Path apiPom = workspaceRoot.resolve("yierdis-server/yierdis-server-api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/pom.xml", Files.isRegularFile(executionPom));
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String executionPolicy = policySection(policy, "yierdis-server-api");
        Assert.assertTrue(
                "execution-api policy must forbid Netty imports from execution API",
                executionPolicy.contains("io.netty")
        );
        Assert.assertTrue(
                "execution-api policy must forbid protocol imports from execution API",
                executionPolicy.contains("yier.bubu.redis.protocol")
        );

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-networking-model</artifactId>",
                "<artifactId>yierdis-networking-codec</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-memory-ffm</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-server-api must not depend on forbidden implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-server/yierdis-server-api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-api Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-api 依赖协议、命令实现、存储实现、运行时实现、应用或 Netty：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void replyWriterMustBeDocumentedAsRedisReplyModelAndKeepProtocolOutOfCommand() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path apiPackage = repoRoot.resolve(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api"
        ).normalize();

        Path redisReplyWriterFile = apiPackage.resolve("RedisReplyWriter.java");
        Assert.assertTrue(
                "ReplyWriter's Redis/RESP-shaped contract must live behind an explicitly named RedisReplyWriter API",
                Files.isRegularFile(redisReplyWriterFile)
        );

        String redisReplyWriter = Files.readString(redisReplyWriterFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "RedisReplyWriter must be documented as a Redis command reply model, not a generic protocol abstraction",
                redisReplyWriter.contains("Redis command reply model")
        );
        Assert.assertTrue(
                "RedisReplyWriter must own RESP3/Redis-shaped aggregate reply methods",
                redisReplyWriter.contains("void mapHeader(int pairs)")
                        && redisReplyWriter.contains("void setHeader(int count)")
                        && redisReplyWriter.contains("void pushHeader(int count)")
                        && redisReplyWriter.contains("void attributeHeader(int pairs)")
        );
        Assert.assertTrue(
                "RedisReplyWriter must own RESP3/Redis-shaped scalar reply methods",
                redisReplyWriter.contains("void verbatimString(String format, byte[] data)")
                        && redisReplyWriter.contains("void blobError(String message)")
        );

        Path replyWriterFile = apiPackage.resolve("ReplyWriter.java");
        Assert.assertFalse(
                "ReplyWriter compatibility alias must be deleted; use RedisReplyWriter",
                Files.exists(replyWriterFile)
        );

        Path replyWriterFactoryFile = apiPackage.resolve("ReplyWriterFactory.java");
        Assert.assertFalse(
                "ReplyWriterFactory compatibility name must be deleted; use RedisReplyWriterFactory",
                Files.exists(replyWriterFactoryFile)
        );

        Path redisReplyWriterFactoryFile = apiPackage.resolve("RedisReplyWriterFactory.java");
        Assert.assertTrue("缺少 RedisReplyWriterFactory.java", Files.isRegularFile(redisReplyWriterFactoryFile));

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForForbiddenText(repoRoot, commandApiMain(repoRoot), offenders, "import yier.bubu.redis.protocol.");
        scanned += scanForForbiddenText(repoRoot, commandKernelMain(repoRoot), offenders, "import yier.bubu.redis.protocol.");
        scanned += scanForForbiddenText(repoRoot, commandDefaultsMain(repoRoot), offenders, "import yier.bubu.redis.protocol.");
        Assert.assertTrue("架构护栏扫描未扫描到任何 command Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 command 层直接 import protocol 包；Redis reply model 必须通过 server-api 边界暴露：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void byteViewAndKeyHandleMustNotExposeLegacyAliases() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path bytesView = repoRoot.resolve(
                "yierdis-common/yierdis-common-bytes/src/main/java/yier/bubu/redis/bytes/BytesView.java"
        ).normalize();
        Path storageKeyHandle = repoRoot.resolve(
                "yierdis-db/yierdis-db-api/src/main/java/yier/bubu/redis/storage/api/KeyHandle.java"
        ).normalize();
        Path memoryKeyHandle = repoRoot.resolve(
                "yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/internal/key/KeyHandle.java"
        ).normalize();

        for (Path file : List.of(bytesView, storageKeyHandle, memoryKeyHandle)) {
            Assert.assertTrue("缺少 byte view/key handle API 文件: " + relativePath(repoRoot, file), Files.isRegularFile(file));
            String source = Files.readString(file, StandardCharsets.UTF_8);
            Assert.assertFalse(
                    relativePath(repoRoot, file) + " must not expose legacy len()",
                    source.contains(" len()")
            );
            Assert.assertFalse(
                    relativePath(repoRoot, file) + " must not expose legacy byteAt(int)",
                    source.contains(" byteAt(int")
            );
        }
    }

    @Test
    public void internalApisMustNotExposeStringMaxmemoryPolicyOverloads() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path yierdisDb = repoRoot.resolve(
                "yierdis-db/yierdis-db-memory/src/main/java/yier/bubu/redis/storage/memory/YierdisDb.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisDb.java", Files.isRegularFile(yierdisDb));
        String dbSource = Files.readString(yierdisDb, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "YierdisDb must not keep String maxmemoryPolicy overloads",
                Pattern.compile("\\bcreateWith(?:Shared|Owned)FfmRuntime\\s*\\([^)]*\\b(?:java\\.lang\\.)?String\\b", Pattern.DOTALL)
                        .matcher(dbSource)
                        .find()
        );
        Assert.assertFalse(
                "YierdisDb must not keep compatibilityMaxmemoryPolicy",
                Pattern.compile("\\bcompatibilityMaxmemoryPolicy\\s*\\(")
                        .matcher(dbSource)
                        .find()
        );

        Path instanceConfig = repoRoot.resolve(
                "yierdis-server/yierdis-server-runtime-api/src/main/java/yier/bubu/redis/runtime/api/YierdisInstanceConfig.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisInstanceConfig.java", Files.isRegularFile(instanceConfig));
        String configSource = Files.readString(instanceConfig, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "YierdisInstanceConfig.Builder must not keep String maxmemoryPolicy overload",
                Pattern.compile("\\bmaxmemoryPolicy\\s*\\(\\s*(?:java\\.lang\\.)?String\\b", Pattern.DOTALL)
                        .matcher(configSource)
                        .find()
        );
    }

    @Test
    public void cliMustUseSharedInlineCommandParserDirectly() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path cliPackage = repoRoot.resolve(
                "yierdis-cli/src/main/java/yier/bubu/redis/app/client"
        ).normalize();
        Assert.assertTrue("缺少 CLI package，无法执行 inline parser 护栏", Files.isDirectory(cliPackage));

        Path cliWrapper = repoRoot.resolve(
                "yierdis-cli/src/main/java/yier/bubu/redis/app/client/InlineCommandParser.java"
        ).normalize();
        Assert.assertFalse(
                "CLI InlineCommandParser wrapper must be deleted; use protocol.resp.InlineCommandParser",
                Files.exists(cliWrapper)
        );

        Path cliMain = cliPackage.resolve("YierdisCli.java");
        Assert.assertTrue("缺少 YierdisCli.java，无法确认 CLI parser 边界", Files.isRegularFile(cliMain));
        String cliSource = Files.readString(cliMain, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "YierdisCli must import the shared RESP inline parser directly",
                cliSource.contains("import yier.bubu.redis.protocol.resp.InlineCommandParser;")
        );
        Assert.assertTrue(
                "YierdisCli must route REPL parsing through the shared inline parser",
                cliSource.contains("InlineCommandParser.splitUtf8(")
        );

        List<String> offenders = new ArrayList<>();
        scanFilesMatchingRegex(
                repoRoot,
                cliPackage,
                offenders,
                "\\b(?:class|interface|enum)\\s+\\w*Inline\\w*Parser\\b"
        );
        Assert.assertTrue(
                "CLI package must not reintroduce a local inline parser implementation:\n" + String.join("\n", offenders),
                offenders.isEmpty()
        );
    }

    @Test
    public void serverSessionProtocolNegotiationMustBeSplitFromGeneralSessionState() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path apiPackage = repoRoot.resolve(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api"
        ).normalize();

        Path serverSessionFile = apiPackage.resolve("ServerSession.java");
        Path markerSessionFile = apiPackage.resolve("Session.java");
        Path commandSessionFile = apiPackage.resolve("CommandSession.java");
        Path capabilityAdapterFile = apiPackage.resolve("Command" + "SessionCapabilities.java");
        Path dbIndexSessionFile = apiPackage.resolve("DbIndexSession.java");
        Path clientMetadataSessionFile = apiPackage.resolve("ClientMetadataSession.java");
        Path transactionSessionFile = apiPackage.resolve("TransactionSession.java");
        Path connectionStatsSessionFile = apiPackage.resolve("ConnectionStatsSession.java");
        Path protocolNegotiationSessionFile = apiPackage.resolve("ProtocolNegotiationSession.java");
        for (Path required : List.of(
                dbIndexSessionFile,
                clientMetadataSessionFile,
                transactionSessionFile,
                connectionStatsSessionFile,
                protocolNegotiationSessionFile,
                commandSessionFile
        )) {
            Assert.assertTrue("缺少拆分后的 session 能力接口: " + relativePath(repoRoot, required), Files.isRegularFile(required));
        }
        Assert.assertFalse(
                "ServerSession aggregate must be deleted; use CommandSession",
                Files.exists(serverSessionFile)
        );
        Assert.assertFalse("marker Session must be deleted", Files.exists(markerSessionFile));
        Assert.assertFalse("capability adapter must be deleted", Files.exists(capabilityAdapterFile));

        String protocolNegotiationSession = Files.readString(protocolNegotiationSessionFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "ProtocolNegotiationSession must own RESP version reads",
                protocolNegotiationSession.contains("int respVersion()")
        );
        Assert.assertTrue(
                "ProtocolNegotiationSession must own RESP version writes",
                protocolNegotiationSession.contains("void setRespVersion(int respVersion)")
        );
        for (String forbiddenState : List.of(
                "dbIndex()",
                "setDbIndex(",
                "clientName()",
                "setClientName(",
                "authenticated()",
                "setAuthenticated(",
                "transaction()",
                "connectionStats()"
        )) {
            Assert.assertFalse(
                    "ProtocolNegotiationSession must not mix ordinary session state method " + forbiddenState,
                    protocolNegotiationSession.contains(forbiddenState)
            );
        }

        String dbIndexSession = Files.readString(dbIndexSessionFile, StandardCharsets.UTF_8);
        Assert.assertTrue("DbIndexSession must own DB index reads", dbIndexSession.contains("int dbIndex()"));
        Assert.assertTrue("DbIndexSession must own DB index writes", dbIndexSession.contains("void setDbIndex(int dbIndex)"));
        Assert.assertFalse("DbIndexSession must not own RESP version", dbIndexSession.contains("respVersion("));

        String commandSession = Files.readString(commandSessionFile, StandardCharsets.UTF_8).replaceAll("\\s+", " ");
        Assert.assertTrue(
                "CommandSession must compose every connection capability",
                commandSession.contains(
                        "extends DbIndexSession, ClientMetadataSession, TransactionSession, ConnectionStatsSession, ProtocolNegotiationSession"
                )
        );

        Path commandContextFile = apiPackage.resolve("CommandContext.java");
        String commandContext = Files.readString(commandContextFile, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "CommandContext must not keep ServerSession constructors",
                commandContext.contains("CommandContext(ServerSession")
        );
        Assert.assertFalse(
                "CommandContext must not keep ServerSession reset overloads",
                commandContext.contains("reset(ServerSession")
        );

        Path engineSessionFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-core/src/main/java/yier/bubu/redis/execution/engine/EngineSession.java"
        ).normalize();
        String engineSession = Files.readString(engineSessionFile, StandardCharsets.UTF_8).replaceAll("\\s+", " ");
        Assert.assertTrue(
                "EngineSession must implement the complete command session",
                engineSession.contains("implements CommandSession")
        );

        Path respFactoryFile = repoRoot.resolve(
                "yierdis-networking/yierdis-networking-resp/src/main/java/yier/bubu/redis/protocol/resp/RespReplyWriterFactory.java"
        ).normalize();
        Assert.assertTrue("缺少 RespReplyWriterFactory.java", Files.isRegularFile(respFactoryFile));
        String respFactory = Files.readString(respFactoryFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "RESP writer factory must require the complete command session",
                respFactory.contains("newWriter(CommandSession session, BytesSink out)")
        );
        Assert.assertFalse(
                "RESP writer factory must not retain a session-free overload",
                respFactory.contains("newWriter(BytesSink")
        );
        Assert.assertFalse(
                "RESP writer factory must not retain the deleted marker-session overload",
                respFactory.contains("newWriter(" + "Session")
        );
        Assert.assertFalse(
                "RESP writer factory must not require full ServerSession just to read RESP version",
                respFactory.contains("import yier.bubu.redis.execution.api.ServerSession;")
        );

        List<String> offenders = new ArrayList<>();
        Path architectureTestFile = repoRoot.resolve(
                "yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java"
        ).normalize();
        int scanned = 0;
        for (Path sourceRoot : List.of(
                repoRoot.resolve("yierdis-server").normalize(),
                repoRoot.resolve("yierdis-command").normalize(),
                repoRoot.resolve("yierdis-tests").normalize()
        )) {
            scanned += scanForForbiddenTextExcluding(
                    repoRoot,
                    sourceRoot,
                    offenders,
                    List.of(architectureTestFile),
                    "import yier.bubu.redis.execution.api.ServerSession;",
                    "implements ServerSession",
                    "from(ServerSession",
                    "CommandContext(ServerSession",
                    "reset(ServerSession"
            );
        }
        Assert.assertTrue("ServerSession guard did not scan any Java files", scanned > 0);
        Assert.assertTrue(
                "ServerSession aggregate references remain:\n" + String.join("\n", offenders),
                offenders.isEmpty()
        );

        List<String> docOffenders = new ArrayList<>();
        Path docsRoot = repoRoot.resolve("docs/project-docs").normalize();
        if (Files.exists(docsRoot)) {
            try (Stream<Path> paths = Files.walk(docsRoot)) {
                paths.filter(p -> p != null && p.toString().endsWith(".md"))
                        .sorted()
                        .forEach(p -> {
                            try {
                                if (Files.readString(p, StandardCharsets.UTF_8).contains("ServerSession")) {
                                    docOffenders.add(relativePath(repoRoot, p));
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
        }
        Assert.assertTrue(
                "Project docs still describe ServerSession:\n" + String.join("\n", docOffenders),
                docOffenders.isEmpty()
        );
    }

    @Test
    public void memoryApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-memory",
                rootPomText.contains("<module>yierdis-memory</module>")
        );
        Assert.assertTrue(
                "root dependencyManagement must include yierdis-memory-api",
                rootPomText.contains("<artifactId>yierdis-memory-api</artifactId>")
        );

        Path memoryPom = workspaceRoot.resolve("yierdis-memory/pom.xml").normalize();
        Path apiPom = workspaceRoot.resolve("yierdis-memory/yierdis-memory-api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-memory/pom.xml", Files.isRegularFile(memoryPom));
        String memoryPomText = Files.readString(memoryPom, StandardCharsets.UTF_8);
        int apiModuleIndex = memoryPomText.indexOf("<module>yierdis-memory-api</module>");
        int foreignModuleIndex = memoryPomText.indexOf("<module>yierdis-memory-ffm</module>");
        Assert.assertTrue(
                "yierdis-memory parent must aggregate api before foreign",
                apiModuleIndex >= 0 && foreignModuleIndex >= 0 && apiModuleIndex < foreignModuleIndex
        );
        Assert.assertTrue("缺少 yierdis-memory/yierdis-memory-api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String memoryApiPolicy = policySection(policy, "yierdis-memory-api");
        Assert.assertTrue(
                "memory-api policy must allow only yierdis-common-bytes as production dependency",
                memoryApiPolicy.contains("yierdis-common-bytes")
        );
        Assert.assertTrue(
                "memory-api policy must name forbidden dependency section",
                memoryApiPolicy.contains("forbidden_dependencies:")
        );
        Assert.assertTrue(
                "memory-api policy must name forbidden import section",
                memoryApiPolicy.contains("forbidden_imports:")
        );
        for (String requiredForbidden : List.of(
                "yierdis-core-api",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-db-memory",
                "yierdis-core-runtime",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-memory-ffm",
                "yierdis-networking-netty",
                "netty-all",
                "yier.bubu.redis.command",
                "yier.bubu.redis.storage.memory",
                "yier.bubu.redis.storage",
                "yier.bubu.redis.runtime",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "yier.bubu.redis.memory.foreign",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "memory-api policy must forbid " + requiredForbidden,
                    memoryApiPolicy.contains(requiredForbidden)
            );
        }

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-memory-api must depend on yierdis-common-bytes",
                pom.contains("<artifactId>yierdis-common-bytes</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-memory-api tests must depend on JUnit",
                pom.contains("<artifactId>junit</artifactId>")
                        && pom.contains("<scope>test</scope>")
        );
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-api</artifactId>",
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-networking-model</artifactId>",
                "<artifactId>yierdis-networking-codec</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-memory-ffm</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-memory-api must not depend on forbidden implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-memory/yierdis-memory-api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.storage.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.storage.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-memory-api Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-memory-api 依赖命令、存储实现、协议、运行时实现、应用、Netty 或 memory-foreign：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void storageApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-db",
                rootPomText.contains("<module>yierdis-db</module>")
        );
        Assert.assertTrue(
                "root dependencyManagement must include yierdis-db-api",
                rootPomText.contains("<artifactId>yierdis-db-api</artifactId>")
        );

        Path storagePom = workspaceRoot.resolve("yierdis-db/pom.xml").normalize();
        Path apiPom = workspaceRoot.resolve("yierdis-db/yierdis-db-api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-db/pom.xml", Files.isRegularFile(storagePom));
        String storagePomText = Files.readString(storagePom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-db parent must aggregate yierdis-db-api",
                storagePomText.contains("<module>yierdis-db-api</module>")
        );
        Assert.assertTrue("缺少 yierdis-db/yierdis-db-api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String storageApiPolicy = policySection(policy, "yierdis-db-api");
        Assert.assertTrue(
                "storage-api policy must allow only yierdis-common-bytes as production dependency",
                storageApiPolicy.contains("yierdis-common-bytes")
        );
        Assert.assertTrue(
                "storage-api policy must name forbidden dependency section",
                storageApiPolicy.contains("forbidden_dependencies:")
        );
        Assert.assertTrue(
                "storage-api policy must name forbidden import section",
                storageApiPolicy.contains("forbidden_imports:")
        );
        for (String requiredForbidden : List.of(
                "yierdis-core-api",
                "yierdis-server-api",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-db-memory",
                "yierdis-core-runtime",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-memory-ffm",
                "yierdis-networking-netty",
                "netty-all",
                "yier.bubu.redis.command",
                "yier.bubu.redis.execution.api",
                "yier.bubu.redis.storage.memory",
                "yier.bubu.redis.runtime",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "yier.bubu.redis.memory.foreign",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "storage-api policy must forbid " + requiredForbidden,
                    storageApiPolicy.contains(requiredForbidden)
            );
        }

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-db-api must depend on yierdis-common-bytes",
                pom.contains("<artifactId>yierdis-common-bytes</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-db-api tests must depend on JUnit",
                pom.contains("<artifactId>junit</artifactId>")
                        && pom.contains("<scope>test</scope>")
        );
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-api</artifactId>",
                "<artifactId>yierdis-server-api</artifactId>",
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-networking-model</artifactId>",
                "<artifactId>yierdis-networking-codec</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-memory-ffm</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-db-api must not depend on forbidden implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-db/yierdis-db-api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.execution.api.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.execution.api.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-db-api Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-db-api 依赖命令、执行、存储实现、协议、运行时实现、应用、Netty 或 memory-foreign：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-server",
                rootPomText.contains("<module>yierdis-server</module>")
        );
        Assert.assertTrue(
                "root dependencyManagement must include yierdis-server-runtime-api",
                rootPomText.contains("<artifactId>yierdis-server-runtime-api</artifactId>")
        );

        Path runtimePom = workspaceRoot.resolve("yierdis-server/pom.xml").normalize();
        Path apiPom = workspaceRoot.resolve("yierdis-server/yierdis-server-runtime-api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/pom.xml", Files.isRegularFile(runtimePom));
        String runtimePomText = Files.readString(runtimePom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server parent must aggregate yierdis-server-runtime-api",
                runtimePomText.contains("<module>yierdis-server-runtime-api</module>")
        );
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-runtime-api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String runtimeApiPolicy = policySection(policy, "yierdis-server-runtime-api");
        Assert.assertTrue(
                "runtime-api policy must allow direct execution-api dependency",
                runtimeApiPolicy.contains("yierdis-server-api")
        );
        Assert.assertTrue(
                "runtime-api policy must allow direct storage-api dependency for embedded runtime config contracts",
                runtimeApiPolicy.contains("yierdis-db-api")
        );
        Assert.assertTrue(
                "runtime-api policy must name forbidden dependency section",
                runtimeApiPolicy.contains("forbidden_dependencies:")
        );
        Assert.assertTrue(
                "runtime-api policy must name forbidden import section",
                runtimeApiPolicy.contains("forbidden_imports:")
        );
        for (String requiredForbidden : List.of(
                "yierdis-core-api",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-db-memory",
                "yierdis-core-runtime",
                "yierdis-server-executor",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-memory-ffm",
                "yierdis-networking-netty",
                "netty-all",
                "yier.bubu.redis.command",
                "yier.bubu.redis.storage.memory",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "yier.bubu.redis.memory.foreign",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "runtime-api policy must forbid " + requiredForbidden,
                    runtimeApiPolicy.contains(requiredForbidden)
            );
        }

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server-runtime-api must depend on yierdis-server-api for change event records",
                pom.contains("<artifactId>yierdis-server-api</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-server-runtime-api must depend on yierdis-db-api for embedded runtime configuration contracts",
                pom.contains("<artifactId>yierdis-db-api</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-server-runtime-api tests must depend on JUnit",
                pom.contains("<artifactId>junit</artifactId>")
                        && pom.contains("<scope>test</scope>")
        );
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-api</artifactId>",
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-server-executor</artifactId>",
                "<artifactId>yierdis-networking-model</artifactId>",
                "<artifactId>yierdis-networking-codec</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-memory-ffm</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-server-runtime-api must not depend on forbidden implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-server/yierdis-server-runtime-api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-runtime-api Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-runtime-api 依赖命令实现、存储实现、协议、应用/server、Netty 或 memory-foreign：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeChangeTrackingSpiImportsMustBeRemoved() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        Assert.assertFalse(policy.contains("yier.bubu.redis.runtime.api.YierdisChangeTracking"));

        List<String> offenders = new ArrayList<>();
        Path importRoot = workspaceRoot.normalize();
        try (Stream<Path> paths = Files.walk(importRoot)) {
            paths.filter(p -> p != null
                            && p.toString().endsWith(".java")
                            && p.normalize().toString().contains("/src/main/java/"))
                    .sorted()
                    .forEach(file -> {
                        try {
                            String source = Files.readString(file, StandardCharsets.UTF_8);
                            if (!containsRuntimeChangeTrackingSpiReference(source)) {
                                return;
                            }
                            offenders.add(workspaceRoot.relativize(file.normalize()).toString().replace('\\', '/'));
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        Assert.assertTrue("YierdisChangeTracking must not remain in production code:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void storageMemoryAndTestkitMustReplaceCoreDbImplementationModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root dependencyManagement must expose yierdis-db-memory",
                rootPomText.contains("<artifactId>yierdis-db-memory</artifactId>")
        );
        Assert.assertTrue(
                "root dependencyManagement must expose yierdis-db-testkit",
                rootPomText.contains("<artifactId>yierdis-db-testkit</artifactId>")
        );
        Assert.assertFalse(
                "root dependencyManagement must not keep retired yierdis-core-db",
                rootPomText.contains("<artifactId>yierdis-core-db</artifactId>")
        );

        Path storagePom = workspaceRoot.resolve("yierdis-db/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-db/pom.xml", Files.isRegularFile(storagePom));
        String storagePomText = Files.readString(storagePom, StandardCharsets.UTF_8);
        for (String module : List.of(
                "yierdis-db-api",
                "yierdis-db-memory",
                "yierdis-db-testkit"
        )) {
            Assert.assertTrue(
                    "yierdis-db parent must aggregate " + module,
                    storagePomText.contains("<module>" + module + "</module>")
            );
        }

        Assert.assertFalse(
                "retired yierdis-core-db pom.xml must not remain in active source tree",
                Files.exists(repoRoot.resolve("yierdis-core/yierdis-core-db/pom.xml"))
        );
        Assert.assertFalse(
                "retired yierdis-core-db main sources must not remain in active source tree",
                Files.exists(repoRoot.resolve("yierdis-core/yierdis-core-db/src/main/java"))
        );

        Path storageMemoryPom = workspaceRoot.resolve("yierdis-db/yierdis-db-memory/pom.xml").normalize();
        Path storageTestkitPom = workspaceRoot.resolve("yierdis-db/yierdis-db-testkit/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-db-memory/pom.xml", Files.isRegularFile(storageMemoryPom));
        Assert.assertTrue("缺少 yierdis-db-testkit/pom.xml", Files.isRegularFile(storageTestkitPom));

        String storageMemoryPomText = Files.readString(storageMemoryPom, StandardCharsets.UTF_8);
        for (String dependency : List.of(
                "yierdis-db-api",
                "yierdis-common-bytes",
                "yierdis-memory-ffm",
                "yierdis-memory-api"
        )) {
            Assert.assertTrue(
                    "yierdis-db-memory must declare production dependency " + dependency,
                    storageMemoryPomText.contains("<artifactId>" + dependency + "</artifactId>")
            );
        }
        for (String forbiddenDependency : List.of(
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-core-runtime",
                "yierdis-server-runtime-api",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-networking-resp",
                "yierdis-networking-resp",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-server-executor",
                "yierdis-networking-netty",
                "netty-all"
        )) {
            Assert.assertFalse(
                    "yierdis-db-memory production pom must not depend on " + forbiddenDependency,
                    pomHasProductionDependency(storageMemoryPom, forbiddenDependency)
            );
        }
        Assert.assertFalse(
                "yierdis-db-memory must not declare yierdis-server-runtime-api in production",
                pomHasProductionDependency(storageMemoryPom, "yierdis-server-runtime-api")
        );

        String storageTestkitPomText = Files.readString(storageTestkitPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-db-testkit must depend on yierdis-db-api for reusable storage fixtures",
                storageTestkitPomText.contains("<artifactId>yierdis-db-api</artifactId>")
        );
        for (String forbiddenDependency : List.of(
                "yierdis-db-memory",
                "yierdis-core-db",
                "yierdis-core-runtime",
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-memory-ffm",
                "netty-all"
        )) {
            Assert.assertFalse(
                    "yierdis-db-testkit must stay reusable and not depend on " + forbiddenDependency,
                    storageTestkitPomText.contains("<artifactId>" + forbiddenDependency + "</artifactId>")
            );
        }

        Path storageMemoryMain = workspaceRoot.resolve("yierdis-db/yierdis-db-memory/src/main/java").normalize();
        Assert.assertTrue("缺少 yierdis-db-memory main source root", Files.isDirectory(storageMemoryMain));
        for (Path requiredStorageClass : List.of(
                storageMemoryMain.resolve("yier/bubu/redis/storage/memory/YierdisDb.java"),
                storageMemoryMain.resolve("yier/bubu/redis/storage/memory/YierdisDbEngineFactory.java"),
                storageMemoryMain.resolve("yier/bubu/redis/storage/memory/internal/expire/YierdisDbExpirationSupport.java"),
                storageMemoryMain.resolve("yier/bubu/redis/storage/memory/YierdisDbMaxmemorySupport.java")
        )) {
            Assert.assertTrue("storage-memory must own concrete storage class " + requiredStorageClass,
                    Files.isRegularFile(requiredStorageClass));
        }
        Path runtimePom = runtimeEmbeddedRoot(repoRoot).resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server-runtime/pom.xml", Files.isRegularFile(runtimePom));
        Assert.assertFalse(
                "yierdis-server-runtime production pom must not depend on yierdis-db-memory; defaults are assembled by server-main/tests",
                pomHasProductionDependency(runtimePom, "yierdis-db-memory")
        );
        Assert.assertFalse(
                "yierdis-server-runtime production pom must not depend on yierdis-memory-ffm; defaults are assembled by server-main/tests",
                pomHasProductionDependency(runtimePom, "yierdis-memory-ffm")
        );
        Assert.assertFalse(
                "yierdis-server-runtime must not depend on retired yierdis-core-db",
                pomHasProductionDependency(runtimePom, "yierdis-core-db")
        );

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String storageMemoryPolicy = policySection(policy, "yierdis-db-memory");
        String storageTestkitPolicy = policySection(policy, "yierdis-db-testkit");
        for (String dependency : List.of(
                "yierdis-db-api",
                "yierdis-common-bytes",
                "yierdis-memory-ffm",
                "yierdis-memory-api"
        )) {
            Assert.assertTrue(
                    "storage-memory policy must allow " + dependency,
                    storageMemoryPolicy.contains(dependency)
            );
        }
        for (String forbidden : List.of(
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-core-runtime",
                "yierdis-server-runtime-api",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-networking-resp",
                "yierdis-networking-resp",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-server-executor",
                "yierdis-networking-netty",
                "netty-all",
                "yier.bubu.redis.command",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "yier.bubu.redis.execution.executor",
                "yier.bubu.redis.runtime",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "storage-memory policy must forbid " + forbidden,
                    storageMemoryPolicy.contains(forbidden)
            );
        }
        List<String> storageMemoryOffenders = new ArrayList<>();
        int storageMemoryScanned = scanForForbiddenText(
                repoRoot,
                storageMemoryMain,
                storageMemoryOffenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import yier.bubu.redis.execution.executor.",
                "import yier.bubu.redis.runtime.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "yier.bubu.redis.execution.executor.",
                "yier.bubu.redis.runtime.",
                "DbChangeContext",
                "recordMutation(",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-db-memory Java 文件", storageMemoryScanned > 0);
        if (!storageMemoryOffenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-db-memory 依赖 command、protocol、server、runtime、executor 或 Netty：\n"
                            + String.join("\n", storageMemoryOffenders)
            );
        }
        Assert.assertTrue(
                "storage-testkit policy must document fixture ownership",
                storageTestkitPolicy.contains("storage_fixtures_only")
        );
        for (String forbidden : List.of(
                "yierdis-db-memory",
                "yierdis-core-db",
                "yierdis-core-runtime",
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-memory-ffm",
                "netty-all",
                "yier.bubu.redis.storage.memory",
                "yier.bubu.redis.command",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "yier.bubu.redis.memory.foreign",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "storage-testkit policy must forbid " + forbidden,
                    storageTestkitPolicy.contains(forbidden)
            );
        }
        List<String> storageTestkitOffenders = new ArrayList<>();
        int storageTestkitScanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-db/yierdis-db-testkit/src/main/java").normalize(),
                storageTestkitOffenders,
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import yier.bubu.redis.memory.foreign.",
                "import io.netty.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "yier.bubu.redis.memory.foreign.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-db-testkit Java 文件", storageTestkitScanned > 0);
        if (!storageTestkitOffenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-db-testkit 依赖 concrete storage、command、protocol、server、memory-foreign 或 Netty：\n"
                            + String.join("\n", storageTestkitOffenders)
            );
        }
    }

    @Test
    public void retiredCoreArtifactsMustLeaveActiveMavenGraph() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "root pom.xml must not aggregate retired yierdis-core module family",
                rootPomText.contains("<module>yierdis-core</module>")
        );

        for (String retiredArtifact : retiredCoreArtifacts()) {
            Assert.assertFalse(
                    "root dependencyManagement must not expose retired artifact " + retiredArtifact,
                    rootPomText.contains("<artifactId>" + retiredArtifact + "</artifactId>")
            );
        }

        Path executionPom = workspaceRoot.resolve("yierdis-server/pom.xml").normalize();
        Path runtimePom = workspaceRoot.resolve("yierdis-server/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/pom.xml", Files.isRegularFile(executionPom));
        Assert.assertTrue("缺少 yierdis-server/pom.xml", Files.isRegularFile(runtimePom));
        String executionPomText = Files.readString(executionPom, StandardCharsets.UTF_8);
        String runtimePomText = Files.readString(runtimePom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server parent must aggregate yierdis-server-core",
                executionPomText.contains("<module>yierdis-server-core</module>")
        );
        Assert.assertTrue(
                "yierdis-server parent must aggregate yierdis-server-runtime",
                runtimePomText.contains("<module>yierdis-server-runtime</module>")
        );

        Assert.assertFalse(
                "retired yierdis-core parent pom.xml must not remain in active source tree",
                Files.exists(workspaceRoot.resolve("yierdis-core/pom.xml"))
        );
        for (String retiredModule : List.of(
                "yierdis-core-api",
                "yierdis-core-contract",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-core-engine",
                "yierdis-core-runtime"
        )) {
            Assert.assertFalse(
                    "retired " + retiredModule + " pom.xml must not remain in active source tree",
                    Files.exists(workspaceRoot.resolve("yierdis-core/" + retiredModule + "/pom.xml"))
            );
            Assert.assertFalse(
                    "retired " + retiredModule + " main sources must not remain in active source tree",
                    Files.exists(workspaceRoot.resolve("yierdis-core/" + retiredModule + "/src/main/java"))
            );
        }

        List<String> offenders = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(workspaceRoot)) {
            paths.filter(path -> path != null
                            && Files.isRegularFile(path)
                            && path.getFileName().toString().equals("pom.xml")
                            && !path.normalize().toString().contains("/target/"))
                    .sorted()
                    .forEach(pom -> {
                        try {
                            List<String> productionDependencies = pomProductionDependencyArtifactIds(pom);
                            for (String retiredArtifact : retiredCoreArtifacts()) {
                                if (productionDependencies.contains(retiredArtifact)) {
                                    offenders.add(relativePath(workspaceRoot, pom) + " -> " + retiredArtifact);
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 active POM 仍有 production 依赖指向 retired core artifacts：\n"
                            + String.join("\n", offenders)
            );
        }

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        Assert.assertTrue("architecture policy must name yierdis-server-core", policy.contains("  yierdis-server-core:"));
        Assert.assertTrue("architecture policy must name yierdis-server-runtime", policy.contains("  yierdis-server-runtime:"));
        Assert.assertFalse("architecture policy must not keep yierdis-core-api section", policy.contains("  yierdis-core-api:"));
        Assert.assertFalse("architecture policy must not keep yierdis-core-engine section", policy.contains("  yierdis-core-engine:"));
        Assert.assertFalse("architecture policy must not keep yierdis-core-runtime section", policy.contains("  yierdis-core-runtime:"));
    }

    @Test
    public void runtimeEmbeddedMustDeclareRuntimeApiBoundary() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String coreRuntimePolicy = policySection(policy, "yierdis-server-runtime");
        Assert.assertTrue(
                "runtime-embedded policy must declare allowed production dependencies",
                coreRuntimePolicy.contains("allowed_dependencies:")
        );
        for (String allowedDependency : List.of(
                "yierdis-db-api",
                "yierdis-server-runtime-api"
        )) {
            Assert.assertTrue(
                    "runtime-embedded policy must allow direct dependency " + allowedDependency,
                    coreRuntimePolicy.contains(allowedDependency)
            );
        }
        Assert.assertTrue(
                "runtime-embedded policy must declare forbidden production dependencies",
                coreRuntimePolicy.contains("forbidden_dependencies:")
        );
        for (String forbiddenDependency : List.of(
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-server-api",
                "yierdis-core-api",
                "yierdis-core-contract",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-core-engine",
                "yierdis-server-executor",
                "yierdis-db-memory",
                "yierdis-memory-ffm",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-networking-resp",
                "yierdis-networking-resp",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-networking-netty",
                "netty-all"
        )) {
            Assert.assertTrue(
                    "runtime-embedded policy must forbid production dependency " + forbiddenDependency,
                    coreRuntimePolicy.contains(forbiddenDependency)
            );
        }
        Assert.assertTrue(
                "runtime-embedded policy must declare forbidden production imports",
                coreRuntimePolicy.contains("forbidden_imports:")
        );
        for (String forbiddenImport : List.of(
                "yier.bubu.redis.command",
                "yier.bubu.redis.execution.engine",
                "yier.bubu.redis.execution.executor",
                "yier.bubu.redis.storage.memory",
                "yier.bubu.redis.memory.foreign",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.app.server",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "runtime-embedded policy must forbid production import " + forbiddenImport,
                    coreRuntimePolicy.contains(forbiddenImport)
            );
        }

        Path runtimePom = runtimeEmbeddedRoot(repoRoot).resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server-runtime/pom.xml", Files.isRegularFile(runtimePom));
        String pom = Files.readString(runtimePom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server-runtime must declare yierdis-db-api directly for runtime lifecycle/storage boundaries",
                pom.contains("<artifactId>yierdis-db-api</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-server-runtime must declare yierdis-server-runtime-api directly for embedded runtime config contracts",
                pom.contains("<artifactId>yierdis-server-runtime-api</artifactId>")
        );
        for (String forbiddenDependency : List.of(
                "yierdis-command-api",
                "yierdis-command-core",
                "yierdis-command-builtin",
                "yierdis-server-api",
                "yierdis-core-api",
                "yierdis-core-contract",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-core-engine",
                "yierdis-server-executor",
                "yierdis-db-memory",
                "yierdis-memory-ffm",
                "yierdis-networking-model",
                "yierdis-networking-codec",
                "yierdis-networking-netty",
                "yierdis-networking-resp",
                "yierdis-networking-resp",
                "yierdis-networking-netty",
                "yierdis-server-main",
                "yierdis-networking-netty",
                "netty-all"
        )) {
            Assert.assertFalse(
                    "yierdis-server-runtime production pom must not depend on " + forbiddenDependency,
                    pomHasProductionDependency(runtimePom, forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                runtimeEmbeddedMain(repoRoot),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.execution.engine.",
                "import yier.bubu.redis.execution.executor.",
                "import yier.bubu.redis.storage.memory.",
                "import yier.bubu.redis.memory.foreign.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.app.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.execution.engine.",
                "yier.bubu.redis.execution.executor.",
                "yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.memory.foreign.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.app.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-runtime Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-runtime 依赖 command、engine、executor、storage implementation、memory-foreign、protocol、server 或 Netty：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void engineMustAvoidFutureProhibitedImplementationFamilies() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String enginePolicy = policySection(policy, "yierdis-server-core");
        Assert.assertTrue(
                "engine policy must allow command-api dependency",
                enginePolicy.contains("yierdis-command-api")
        );
        Assert.assertTrue(
                "engine policy must allow command-kernel dependency",
                enginePolicy.contains("yierdis-command-core")
        );
        Assert.assertTrue(
                "engine policy must forbid command-defaults dependency",
                enginePolicy.contains("yierdis-command-builtin")
        );
        Assert.assertTrue(
                "engine policy must forbid server imports",
                enginePolicy.contains("yier.bubu.redis.app.server")
        );
        Assert.assertTrue(
                "engine policy must forbid Netty imports",
                enginePolicy.contains("io.netty")
        );
        Assert.assertTrue(
                "engine policy must forbid concrete DB/runtime dependency",
                enginePolicy.contains("yierdis-core-db")
                        && enginePolicy.contains("yierdis-db-memory")
        );

        Path enginePom = engineRoot(repoRoot).resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server-core/pom.xml", Files.isRegularFile(enginePom));
        String pom = Files.readString(enginePom, StandardCharsets.UTF_8);
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-networking-model</artifactId>",
                "<artifactId>yierdis-networking-codec</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-db-memory</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-server-main</artifactId>",
                "<artifactId>yierdis-memory-ffm</artifactId>",
                "<artifactId>yierdis-networking-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-server-core must not depend on future-prohibited implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                engineRoot(repoRoot).resolve("src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.defaults.",
                "yier.bubu.redis.command.defaults.",
                "import yier.bubu.redis.storage.",
                "yier.bubu.redis.storage.",
                "import yier.bubu.redis.protocol.",
                "yier.bubu.redis.protocol.",
                "import yier.bubu.redis.runtime.",
                "yier.bubu.redis.runtime.",
                "import yier.bubu.redis.app.server.",
                "yier.bubu.redis.app.server.",
                "import yier.bubu.redis.storage.memory.",
                "yier.bubu.redis.storage.memory.",
                "import io.netty.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-core Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-core 依赖 command-defaults、storage-memory、protocol adapter、runtime implementation、application/server、Netty 或 concrete DB runtime：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void executionRequestAndServerReplyBoundariesMustStayDocumented() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path requestFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/ExecutionRequest.java"
        );
        Assert.assertTrue("缺少 ExecutionRequest.java", Files.isRegularFile(requestFile));
        String requestSource = Files.readString(requestFile, StandardCharsets.UTF_8);
        Assert.assertTrue("request model must stay in execution API package", requestSource.contains("package yier.bubu.redis.execution.api;"));
        Assert.assertTrue("request model must expose direct execution contract", requestSource.contains("interface ExecutionRequest"));
        Assert.assertTrue("request model must expose retained byte accounting", requestSource.contains("retainedBytes()"));
        Assert.assertTrue("request model must expose admission accounting", requestSource.contains("admittedMemoryBytes()"));

    }

    @Test
    public void serverSourceMustNotConstructProtocolReplyModelsDirectly() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java"),
                offenders,
                "ReplyValue.",
                "ReplyArray(",
                "ReplyMap("
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server 生产代码直接构造 protocol reply model（应保持 ReplyWriter 为唯一语义 authority）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void productionReplyWritesAndBuffersMustStayInsideTheOrderedEgressOwners() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("cannot locate repository root", repoRoot);

        Path serverMain = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize();
        Path sequencer = serverMain.resolve(
                "yier/bubu/redis/app/server/ConnectionReplySequencer.java"
        ).normalize();
        Path chunkSink = serverMain.resolve(
                "yier/bubu/redis/app/server/BoundedChunkedReplySink.java"
        ).normalize();
        Assert.assertTrue("missing ordered reply sequencer", Files.isRegularFile(sequencer));
        Assert.assertTrue("missing bounded reply chunk sink", Files.isRegularFile(chunkSink));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenTextExcluding(
                repoRoot,
                serverMain,
                offenders,
                List.of(sequencer, chunkSink),
                "channel.write(",
                "channel.writeAndFlush(",
                "channel.alloc().buffer(",
                "ctx.write(",
                "ctx.writeAndFlush(",
                "ctx.channel().write",
                "ctx.channel().alloc().buffer("
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-networking/yierdis-networking-netty/src/main/java").normalize(),
                offenders,
                "channel.write(",
                "channel.writeAndFlush(",
                "channel.alloc().buffer(",
                "ctx.write(",
                "ctx.writeAndFlush(",
                "ctx.channel().write",
                "ctx.channel().alloc().buffer("
        );
        Assert.assertTrue("ordered egress guard scanned no production Java files", scanned > 0);
        Assert.assertTrue(
                "direct production reply writes or growable buffers remain outside ordered egress owners:\n"
                        + String.join("\n", offenders),
                offenders.isEmpty()
        );
    }

    @Test
    public void yierdisDbMustNotRetainLegacyReservationHelpers() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path dbFile = storageMemoryMain(repoRoot).resolve("yier/bubu/redis/storage/memory/YierdisDb.java");
        Assert.assertTrue("缺少 YierdisDb.java", Files.exists(dbFile));

        String source = Files.readString(dbFile);
        List<String> offenders = new ArrayList<>();
        if (source.contains("MemoryReservation activeReservation")) {
            offenders.add("activeReservation");
        }
        if (source.contains("public void ensureWriteAllowed(")) {
            offenders.add("ensureWriteAllowed(");
        }
        if (source.contains("public void prepareWrite(")) {
            offenders.add("prepareWrite(");
        }
        if (source.contains("public void rollbackWriteReservationIfAny(")) {
            offenders.add("rollbackWriteReservationIfAny(");
        }
        if (source.contains("void commitWrite(")) {
            offenders.add("commitWrite(");
        }
        if (source.contains("void rollbackWrite(")) {
            offenders.add("rollbackWrite(");
        }

        if (!offenders.isEmpty()) {
            Assert.fail("检测到 YierdisDb 仍保留 legacy reservation helper：\n" + String.join("\n", offenders));
        }
    }

    @Test
    public void runtimeMaintenanceMustNotCastPublicDbViewsBackToRuntime() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path runtimeAccessFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，说明 runtime 显式访问 seam 未建立", Files.isRegularFile(runtimeAccessFile));

        Path maintenanceFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceMaintenance.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceMaintenance.java", Files.isRegularFile(maintenanceFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                maintenanceFile,
                offenders,
                "instanceof RuntimeDbEngine",
                "requireRuntimeEngine(",
                "DbEngine publicEngine = inst.engine("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 runtime maintenance 仍通过公开 DbEngine 视图回退到 RuntimeDbEngine：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeAssemblyMustNotUseRttiOrFirstEngineGlobalMaintenance() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path instanceFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java", Files.isRegularFile(instanceFile));

        Path runtimeAccessFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java", Files.isRegularFile(runtimeAccessFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                instanceFile,
                offenders,
                "instanceof MaxmemoryParticipant",
                "instanceof MaxmemoryCoordinatorAware"
        );
        scanFileForForbiddenText(
                repoRoot,
                runtimeAccessFile,
                offenders,
                "RuntimeDbEngine firstEngine",
                "firstEngine.enforceMaxmemoryMaintenance()"
        );
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 runtime 仍使用 RTTI 或 first-engine 全局维护约定：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeStrictCreateMustNotAssembleDefaultMemoryBackend() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path instanceFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java", Files.isRegularFile(instanceFile));

        List<String> offenders = new ArrayList<>();
        scanMethodForForbiddenText(
                repoRoot,
                instanceFile,
                "public static YierdisInstance create(YierdisInstanceConfig config)",
                offenders,
                "new YierdisDbEngineFactory(",
                "new YierdisFfmMemoryRuntime("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 runtime strict create(config) 仍承担默认 DB/backend 组装职责（生产默认组装应在 server-main 或显式 factory）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverBootstrapMustNotInlineOwnerThreadLifecycleAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path runtimeAccessFile = runtimeEmbeddedRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/runtime/embedded/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，无法约束 bootstrap 生命周期边界", Files.isRegularFile(runtimeAccessFile));

        Path bootstrapFile = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java").normalize();
        Assert.assertTrue("缺少 YierdisServerBootstrap.java", Files.isRegularFile(bootstrapFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                bootstrapFile,
                offenders,
                "inst.bindToCurrentThread()",
                "inst.close()",
                "runtimeAccess.maintenanceTick()",
                "engines = instance.engines()",
                "bindEngines("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 bootstrap 重新内联 owner-thread 生命周期逻辑，而不是通过 runtime seam 协作：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverInfoProviderMustNotOwnGlobalMemoryAggregationAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path infoProviderFile = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/NettyServerInfoProvider.java").normalize();
        Assert.assertTrue("缺少 NettyServerInfoProvider.java", Files.isRegularFile(infoProviderFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                infoProviderFile,
                offenders,
                "private MemorySummary memorySummary()",
                "long totalEstimatedBytes = heap + offHeap + keyspaceOverhead + expireOverhead + expireValueObjects;",
                "offHeap = Math.max(offHeap, s.offHeapUsedBytes());",
                "db.memory().memoryStats()",
                "appendKeyspace("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 NettyServerInfoProvider 重新承担 instance/global memory 聚合职责，而不是通过 runtime observability seam 协作：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void executorCoreMustNotDependOnCoreCommand() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path executorRoot = repoRoot.resolve("yierdis-server/yierdis-server-executor").normalize();
        Path executorPom = executorRoot.resolve("pom.xml");
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-executor/pom.xml", Files.isRegularFile(executorPom));

        String pom = Files.readString(executorPom, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "yierdis-server-executor must not depend on yierdis-core-command",
                pom.contains("<artifactId>yierdis-core-command</artifactId>")
        );

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                executorRoot.resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.command.",
                "yier.bubu.redis.command."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-executor Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-executor 依赖 core-command（executor-core 应只依赖执行契约）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverBootstrapMustWireCommandExecutionThroughYierdisEngine() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path engineFile = engineRoot(repoRoot).resolve(
                "src/main/java/yier/bubu/redis/execution/engine/YierdisEngine.java"
        );
        Assert.assertTrue(
                "缺少 YierdisEngine facade，server bootstrap 不应继续直接接线 command processor",
                Files.isRegularFile(engineFile)
        );

        Path bootstrapFile = repoRoot.resolve(
                "yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisServerBootstrap.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisServerBootstrap.java", Files.isRegularFile(bootstrapFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                bootstrapFile,
                offenders,
                "new YierdisFastCommandProcessor(",
                "processor::execute",
                "maintenance.maintenanceTick()"
        );
        assertFileContainsAllText(
                repoRoot,
                bootstrapFile,
                offenders,
                "ServerCommandComposition.createProcessor("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server bootstrap 仍绕过 YierdisEngine 直接接线 command processor 或 maintenance：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustNotBypassEngineForCommandExecution() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        Path serverMainRoot = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize();
        int scanned = scanForForbiddenTextExcluding(
                repoRoot,
                serverMainRoot,
                offenders,
                List.of(
                        serverMainRoot.resolve("yier/bubu/redis/app/server/ServerCommandComposition.java").normalize()
                ),
                "new YierdisFastCommandProcessor(",
                "new CommandContext(",
                ".execute(request, new CommandContext"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server 生产代码绕过 YierdisEngine 构造命令处理器或命令上下文：\n"
                    + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void commandAssemblyMustStayInNamedCompositionRoots() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                commandKernelMain(repoRoot).resolve("yier/bubu/redis/command/kernel/YierdisFastCommandProcessor.java"),
                offenders,
                "new TransactionCommands(this)",
                "registerExtraModules(",
                "CommandModule..."
        );
        scanFileForForbiddenText(
                repoRoot,
                engineRoot(repoRoot).resolve("src/main/java/yier/bubu/redis/execution/engine/DefaultYierdisEngine.java"),
                offenders,
                "new YierdisFastCommandProcessor("
        );

        assertFileContainsAllText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java"
                ).normalize(),
                offenders,
                "TestCommandComposition"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/integration/command/TestCommandProcessors.java"
                ).normalize(),
                offenders,
                "new YierdisFastCommandProcessor(",
                "new DefaultYierdisEngine("
        );

        assertFileContainsAllText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java"
                ).normalize(),
                offenders,
                "ServerCommandComposition"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-server/yierdis-server-main/src/test/java/yier/bubu/redis/app/server/TestYierdisEngines.java"
                ).normalize(),
                offenders,
                "new YierdisFastCommandProcessor("
        );

        assertFileContainsAllText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java"
                ).normalize(),
                offenders,
                "EmbeddedCommandComposition"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve(
                        "yierdis-tests/yierdis-integration-tests/src/test/java/yier/bubu/redis/runtime/embedded/TestCommandProcessors.java"
                ).normalize(),
                offenders,
                "new YierdisFastCommandProcessor(",
                "new DefaultYierdisEngine("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到命令装配逃离命名 composition root（steady state 必须经由 *CommandComposition 入口）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverAppMustReplaceLegacyServerArtifactAndAvoidStorageInternals() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-server parent",
                rootPomText.contains("<module>yierdis-server</module>")
        );
        Assert.assertFalse(
                "root dependencyManagement must not keep legacy yierdis-server artifact",
                rootPomText.contains("<artifactId>yierdis-server</artifactId>")
        );
        Assert.assertTrue(
                "root dependencyManagement must expose yierdis-server-main artifact",
                rootPomText.contains("<artifactId>yierdis-server-main</artifactId>")
        );

        Path serverParentPom = workspaceRoot.resolve("yierdis-server/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/pom.xml", Files.isRegularFile(serverParentPom));
        String serverParentPomText = Files.readString(serverParentPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server parent must aggregate yierdis-server-main",
                serverParentPomText.contains("<module>yierdis-server-main</module>")
        );

        Path serverAppPom = workspaceRoot.resolve("yierdis-server/yierdis-server-main/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/pom.xml", Files.isRegularFile(serverAppPom));
        String serverAppPomText = Files.readString(serverAppPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "server-app pom must declare yierdis-server-main artifactId",
                serverAppPomText.contains("<artifactId>yierdis-server-main</artifactId>")
        );
        Assert.assertTrue(
                "server-app pom must keep the runnable server main class",
                serverAppPomText.contains("<mainClass>yier.bubu.redis.app.server.YierdisServer</mainClass>")
        );

        Path clientPom = workspaceRoot.resolve("yierdis-cli/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-cli/pom.xml", Files.isRegularFile(clientPom));
        String clientPomText = Files.readString(clientPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "client tests must depend on yierdis-server-main for process-boundary smoke coverage",
                clientPomText.contains("<artifactId>yierdis-server-main</artifactId>")
        );
        Assert.assertFalse(
                "client pom must not depend on legacy yierdis-server artifact",
                clientPomText.contains("<artifactId>yierdis-server</artifactId>")
        );

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.storage.memory.internal.",
                "import yier.bubu.redis.storage.memory.YierdisDb;",
                "import yier.bubu.redis.storage.internal.",
                "import yier.bubu.redis.storage.memory.memory.",
                "new YierdisDb("
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server-app 直接依赖 storage implementation/internal，而不是只做应用组装：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverStorageApiImportsMustHaveDirectDependency() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String serverPolicy = policySection(policy, "yierdis-server-main");
        Assert.assertTrue(
                "server policy must allow direct storage-api dependency when server imports storage API types",
                serverPolicy.contains("yierdis-db-api")
        );

        Path serverPom = workspaceRoot.resolve("yierdis-server/yierdis-server-main/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/pom.xml", Files.isRegularFile(serverPom));
        String pom = Files.readString(serverPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server-main must declare yierdis-db-api directly because production server code imports storage API types",
                pom.contains("<artifactId>yierdis-db-api</artifactId>")
        );

        List<String> storageApiUsers = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize(),
                storageApiUsers,
                "import yier.bubu.redis.storage.api."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned > 0);
        Assert.assertFalse(
                "server guard expected at least one production storage-api import; remove this direct-dependency guard if server stops using storage API",
                storageApiUsers.isEmpty()
        );
    }

    @Test
    public void serverRuntimeApiImportsMustHaveDirectDependency() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);
        Path workspaceRoot = repoRoot;
        assertRuntimeApiDetectorCoversPackageWideReviewCases();

        Path policyFile = workspaceRoot.resolve("yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String serverPolicy = policySection(policy, "yierdis-server-main");
        Assert.assertTrue(
                "server policy must allow direct runtime-api dependency when server imports runtime API types",
                serverPolicy.contains("yierdis-server-runtime-api")
        );

        Path serverPom = workspaceRoot.resolve("yierdis-server/yierdis-server-main/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/pom.xml", Files.isRegularFile(serverPom));
        String pom = Files.readString(serverPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-server-main must declare yierdis-server-runtime-api directly because production server code imports runtime API types",
                pom.contains("<artifactId>yierdis-server-runtime-api</artifactId>")
        );

        List<String> runtimeApiUsers = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-server/yierdis-server-main/src/main/java").normalize(),
                runtimeApiUsers,
                "yier.bubu.redis.runtime.api.YierdisInstanceConfig",
                "import yier.bubu.redis.runtime.*;",
                "yier.bubu.redis.runtime.api."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned > 0);
        Assert.assertFalse(
                "server guard expected at least one production runtime-api import/reference; remove this direct-dependency guard if server stops using runtime API",
                runtimeApiUsers.isEmpty()
        );
    }

    @Test
    public void executorCoreMustNotDependOnNetty() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-server/yierdis-server-executor/src/main/java").normalize(),
                offenders,
                "import io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-executor Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-executor 重新依赖 Netty（该模块必须保持 transport-neutral）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustNotContainLegacyExecutorRuntimeFiles() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path serverRoot = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server", Files.isDirectory(serverRoot));

        List<String> offenders = new ArrayList<>();
        for (String name : List.of(
                "NettyCommandExecutor.java",
                "NettyCommandSubmitter.java",
                "NettyCommandDrainLoop.java",
                "NettyCommandExecutionSupport.java",
                "NettyExecutorTask.java",
                "NettyCommandExecutorConfig.java",
                "ServerConnectionContext.java",
                "ServerSessionState.java",
                "ServerRuntimeState.java",
                "NettyExecutorChannelState.java"
        )) {
            Path file = serverRoot.resolve(name);
            if (Files.exists(file)) {
                offenders.add(relativePath(repoRoot, file));
            }
        }

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到已经废弃的 server-owned executor/runtime 文件仍然存在：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustOwnChannelAttrOnlyInNettyExecutionAdapters() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path serverRoot = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server", Files.isDirectory(serverRoot));

        Path allowedConnectionOwner = serverRoot.resolve("NettyExecutionConnection.java");
        Path allowedIoOwner = serverRoot.resolve("NettyExecutionIoAdapter.java");
        Assert.assertTrue("缺少 NettyExecutionConnection.java，无法执行 Channel.attr 归一化护栏", Files.isRegularFile(allowedConnectionOwner));
        Assert.assertTrue("缺少 NettyExecutionIoAdapter.java，无法执行 Channel.attr 归一化护栏", Files.isRegularFile(allowedIoOwner));

        List<String> offenders = new ArrayList<>();
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(serverRoot)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(file -> {
                        if (allowedConnectionOwner.equals(file) || allowedIoOwner.equals(file)) {
                            return;
                        }
                        scanned[0]++;
                        try {
                            scanFileForForbiddenText(
                                    repoRoot,
                                    file,
                                    offenders,
                                    ".attr(",
                                    "AttributeKey.valueOf("
                            );
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned[0] > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server-main 存在分散的 Channel.attr 所有权（仅允许 NettyExecutionConnection/NettyExecutionIoAdapter 持有）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustNotReachThroughLegacyConnectionSlices() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-server/yierdis-db-memory 模块）", repoRoot);

        Path serverRoot = repoRoot.resolve("yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server").normalize();
        Assert.assertTrue("缺少 yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server", Files.isDirectory(serverRoot));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                serverRoot,
                offenders,
                ".runtime()",
                ".scheduling()"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server-main Java 文件", scanned > 0);
        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server 代码仍访问已经废弃的 legacy 连接切片 API：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    private static int scanForForbiddenText(Path repoRoot, Path root, List<String> offenders, String... forbiddenSnippets) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            scanFileForForbiddenText(repoRoot, p, offenders, forbiddenSnippets);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanForForbiddenTextExcluding(
            Path repoRoot,
            Path root,
            List<String> offenders,
            List<Path> allowedFiles,
            String... forbiddenSnippets
    ) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0;
        }
        List<Path> normalizedAllowed = allowedFiles == null
                ? List.of()
                : allowedFiles.stream().map(Path::normalize).toList();
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(p -> {
                        Path normalized = p.normalize();
                        if (normalizedAllowed.contains(normalized)) {
                            return;
                        }
                        try {
                            scanned[0]++;
                            scanFileForForbiddenText(repoRoot, p, offenders, forbiddenSnippets);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static int scanFilesMatchingRegex(Path repoRoot, Path root, List<String> offenders, String... forbiddenPatterns) throws IOException {
        if (root == null || !Files.exists(root)) {
            return 0;
        }
        int[] scanned = new int[]{0};
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(p -> p != null && p.toString().endsWith(".java"))
                    .sorted()
                    .forEach(p -> {
                        try {
                            scanned[0]++;
                            String source = Files.readString(p, StandardCharsets.UTF_8);
                            for (String forbiddenPattern : forbiddenPatterns) {
                                if (source.matches("(?s).*" + forbiddenPattern + ".*")) {
                                    offenders.add(relativePath(repoRoot, p) + " -> /" + forbiddenPattern + "/");
                                }
                            }
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }
        return scanned[0];
    }

    private static void scanFileForForbiddenText(Path repoRoot, Path file, List<String> offenders, String... forbiddenSnippets)
            throws IOException {
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            for (String snippet : forbiddenSnippets) {
                if (line.contains(snippet)) {
                    offenders.add(relativePath(repoRoot, file) + ":" + (i + 1) + " -> " + snippet);
                }
            }
        }
    }

    private static void assertFileContainsAllText(Path repoRoot, Path file, List<String> offenders, String... requiredSnippets)
            throws IOException {
        Assert.assertTrue("missing expected file: " + relativePath(repoRoot, file), Files.isRegularFile(file));
        String source = Files.readString(file, StandardCharsets.UTF_8);
        for (String snippet : requiredSnippets) {
            if (!source.contains(snippet)) {
                offenders.add(relativePath(repoRoot, file) + " -> missing required text: " + snippet);
            }
        }
    }

    private static int scanCommandMainForForbiddenText(Path repoRoot, List<String> offenders, String... forbiddenSnippets)
            throws IOException {
        int scanned = 0;
        scanned += scanForForbiddenText(repoRoot, commandApiMain(repoRoot), offenders, forbiddenSnippets);
        scanned += scanForForbiddenText(repoRoot, commandKernelMain(repoRoot), offenders, forbiddenSnippets);
        scanned += scanForForbiddenText(repoRoot, commandDefaultsMain(repoRoot), offenders, forbiddenSnippets);
        return scanned;
    }

    private static void assertPackageDeclaration(Path repoRoot, Path file, String expectedDeclaration) throws IOException {
        Assert.assertTrue("missing expected command package file: " + relativePath(repoRoot, file), Files.isRegularFile(file));
        String source = Files.readString(file, StandardCharsets.UTF_8);
        Assert.assertTrue(
                relativePath(repoRoot, file) + " must declare " + expectedDeclaration,
                source.contains(expectedDeclaration)
        );
    }

    private static Path commandApiMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-command/yierdis-command-api/src/main/java").normalize();
    }

    private static Path commandKernelMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-command/yierdis-command-core/src/main/java").normalize();
    }

    private static Path commandDefaultsMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-command/yierdis-command-builtin/src/main/java").normalize();
    }

    private static Path storageMemoryMain(Path repoRoot) {
        return repoRoot.resolve("yierdis-db/yierdis-db-memory/src/main/java").normalize();
    }

    private static Path engineRoot(Path repoRoot) {
        return repoRoot.resolve("yierdis-server/yierdis-server-core").normalize();
    }

    private static Path runtimeEmbeddedRoot(Path repoRoot) {
        return repoRoot.resolve("yierdis-server/yierdis-server-runtime").normalize();
    }

    private static Path runtimeEmbeddedMain(Path repoRoot) {
        return runtimeEmbeddedRoot(repoRoot).resolve("src/main/java").normalize();
    }

    private static Path commandKernelFile(Path repoRoot, String fileName) {
        return switch (fileName) {
            case "TransactionCommands.java", "CommandRegistry.java", "YierdisFastCommandProcessor.java" ->
                    commandKernelMain(repoRoot).resolve("yier/bubu/redis/command/kernel").resolve(fileName).normalize();
            default -> commandKernelMain(repoRoot).resolve("yier/bubu/redis/command").resolve(fileName).normalize();
        };
    }

    private static Path commandDefaultsFile(Path repoRoot, String fileName) {
        return switch (fileName) {
            case "StringCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/string").resolve(fileName).normalize();
            case "HashCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/hash").resolve(fileName).normalize();
            case "ListCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/list").resolve(fileName).normalize();
            case "SetCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/set").resolve(fileName).normalize();
            case "ZSetCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/zset").resolve(fileName).normalize();
            case "HllCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/hll").resolve(fileName).normalize();
            case "KeyCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/keyspace").resolve(fileName).normalize();
            case "CoreConnectionCommands.java" -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults/connection").resolve(fileName).normalize();
            default -> commandDefaultsMain(repoRoot).resolve("yier/bubu/redis/command/defaults").resolve(fileName).normalize();
        };
    }

    private static void assertRuntimeChangeTrackingSpiDetectorCoversReviewCases() {
        Assert.assertTrue(
                "runtime change tracking SPI guard must catch exact imports",
                containsRuntimeChangeTrackingSpiReference("import yier.bubu.redis.runtime.api.YierdisChangeTracking;")
        );
        Assert.assertTrue(
                "runtime change tracking SPI guard must catch wildcard runtime-api imports",
                containsRuntimeChangeTrackingSpiReference("import yier.bubu.redis.runtime.api.*;")
        );
        Assert.assertTrue(
                "runtime change tracking SPI guard must catch fully-qualified references",
                containsRuntimeChangeTrackingSpiReference(
                        "yier.bubu.redis.runtime.api.YierdisChangeTracking.markValueChanged();"
                )
        );
        Assert.assertFalse(
                "runtime change tracking SPI guard must not catch unrelated runtime-api types",
                containsRuntimeChangeTrackingSpiReference("import yier.bubu.redis.runtime.api.YierdisChangeEvent;")
        );
    }

    private static boolean containsRuntimeChangeTrackingSpiReference(String source) {
        return source.contains("yier.bubu.redis.runtime.api.YierdisChangeTracking")
                || source.contains("import yier.bubu.redis.runtime.api.*;");
    }

    private static void assertRuntimeApiDetectorCoversPackageWideReviewCases() {
        Assert.assertTrue(
                "server runtime-api guard must catch legacy-package runtime API imports",
                containsRuntimeApiReference("import yier.bubu.redis.runtime.api.YierdisInstanceConfig;")
        );
        Assert.assertTrue(
                "server runtime-api guard must catch legacy-package runtime wildcard imports",
                containsRuntimeApiReference("import yier.bubu.redis.runtime.*;")
        );
        Assert.assertTrue(
                "server runtime-api guard must catch fully-qualified legacy-package runtime API references",
                containsRuntimeApiReference("yier.bubu.redis.runtime.api.YierdisInstanceConfig.builder();")
        );
        Assert.assertTrue(
                "server runtime-api guard must catch runtime-api package imports",
                containsRuntimeApiReference("import yier.bubu.redis.runtime.api.YierdisChangeEvent;")
        );
        Assert.assertTrue(
                "server runtime-api guard must catch fully-qualified runtime-api package references",
                containsRuntimeApiReference("yier.bubu.redis.runtime.api.YierdisChangeSink.NOOP.publish(null);")
        );
        Assert.assertFalse(
                "server runtime-api guard must not catch runtime implementation imports",
                containsRuntimeApiReference("import yier.bubu.redis.runtime.embedded.YierdisInstance;")
        );
    }

    private static boolean containsRuntimeApiReference(String source) {
        return source.contains("yier.bubu.redis.runtime.api.YierdisInstanceConfig")
                || source.contains("import yier.bubu.redis.runtime.*;")
                || source.contains("yier.bubu.redis.runtime.api.");
    }

    private static void scanMethodForForbiddenText(
            Path repoRoot,
            Path file,
            String methodSignature,
            List<String> offenders,
            String... forbiddenSnippets
    ) throws IOException {
        Assert.assertTrue("缺少文件：" + file, Files.isRegularFile(file));
        List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        int start = -1;
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains(methodSignature)) {
                start = i;
                break;
            }
        }
        Assert.assertTrue("缺少方法：" + relativePath(repoRoot, file) + "#" + methodSignature, start >= 0);

        int depth = 0;
        boolean inBody = false;
        for (int i = start; i < lines.size(); i++) {
            String line = lines.get(i);
            if (inBody) {
                for (String snippet : forbiddenSnippets) {
                    if (line.contains(snippet)) {
                        offenders.add(relativePath(repoRoot, file) + ":" + (i + 1) + " -> " + snippet);
                    }
                }
            }
            for (int j = 0; j < line.length(); j++) {
                char c = line.charAt(j);
                if (c == '{') {
                    depth++;
                    inBody = true;
                } else if (c == '}') {
                    depth--;
                }
            }
            if (inBody && depth == 0) {
                return;
            }
        }
    }

    private static String policySection(String policy, String moduleName) {
        String[] lines = policy.split("\\R", -1);
        StringBuilder section = new StringBuilder();
        boolean found = false;
        for (String line : lines) {
            boolean moduleHeader = line.startsWith("  ")
                    && !line.startsWith("    ")
                    && line.endsWith(":");
            if (moduleHeader) {
                if (found) {
                    break;
                }
                found = line.equals("  " + moduleName + ":");
            }
            if (found) {
                section.append(line).append('\n');
            }
        }
        Assert.assertTrue("architecture policy must name " + moduleName, found);
        return section.toString();
    }

    private static boolean pomHasProductionDependency(Path pom, String artifactId) throws IOException {
        return pomProductionDependencyArtifactIds(pom).contains(artifactId);
    }

    private static List<String> retiredCoreArtifacts() {
        return List.of(
                "yierdis-core-api",
                "yierdis-core-contract",
                "yierdis-core-command",
                "yierdis-core-db",
                "yierdis-core-engine",
                "yierdis-core-runtime"
        );
    }

    private static List<String> pomProductionDependencyArtifactIds(Path pom) throws IOException {
        Assert.assertTrue("缺少 pom.xml: " + pom, Files.isRegularFile(pom));
        Document document;
        try (InputStream in = Files.newInputStream(pom)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            document = factory.newDocumentBuilder().parse(in);
        } catch (Exception e) {
            throw new IOException("failed to parse pom.xml: " + pom, e);
        }

        List<String> artifactIds = new ArrayList<>();
        for (Element dependencies : childElements(document.getDocumentElement(), "dependencies")) {
            for (Element dependency : childElements(dependencies, "dependency")) {
                String artifactId = directChildText(dependency, "artifactId");
                String scope = directChildText(dependency, "scope");
                if (artifactId != null && !artifactId.isBlank() && !"test".equals(scope)) {
                    artifactIds.add(artifactId);
                }
            }
        }
        return artifactIds;
    }

    private static List<Element> childElements(Element parent, String localName) {
        List<Element> matches = new ArrayList<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            if (child instanceof Element element && nodeNameEquals(element, localName)) {
                matches.add(element);
            }
        }
        return matches;
    }

    private static String directChildText(Element parent, String localName) {
        for (Element child : childElements(parent, localName)) {
            return child.getTextContent().trim();
        }
        return null;
    }

    private static boolean nodeNameEquals(Node node, String localName) {
        String actual = node.getLocalName();
        if (actual == null || actual.isBlank()) {
            actual = node.getNodeName();
        }
        return localName.equals(actual);
    }

    private static void retainOnly(List<String> offenders, String allowedRelativeFile) {
        if (offenders == null || offenders.isEmpty()) {
            return;
        }
        offenders.removeIf(line -> line != null && line.startsWith(allowedRelativeFile + ":"));
    }

    private static String relativePath(Path repoRoot, Path file) {
        if (repoRoot == null || file == null) {
            return String.valueOf(file);
        }
        return repoRoot.relativize(file).toString().replace('\\', '/');
    }

    private static boolean isUnder(Path file, Path root) {
        return file != null && root != null && file.normalize().startsWith(root.normalize());
    }

    private static Path resolveRepoRoot() {
        // Maven surefire 下通常为 yierdis-tests/yierdis-architecture-tests 模块根目录；IDE/自定义运行环境下可能是仓库根目录。
        Path cwd = Paths.get("").toAbsolutePath().normalize();
        Path direct = tryResolveRepoRoot(cwd);
        if (direct != null) {
            return direct;
        }

        String basedir = System.getProperty("basedir");
        if (basedir != null && !basedir.isBlank()) {
            Path byBasedir = tryResolveRepoRoot(Paths.get(basedir));
            if (byBasedir != null) {
                return byBasedir;
            }
        }

        Path p = cwd;
        for (int i = 0; i < 6 && p != null; i++) {
            Path candidate = tryResolveRepoRoot(p);
            if (candidate != null) {
                return candidate;
            }
            p = p.getParent();
        }
        return null;
    }

    private static Path tryResolveRepoRoot(Path base) {
        if (base == null) {
            return null;
        }

        if (Files.isRegularFile(base.resolve("pom.xml"))
                && Files.isRegularFile(base.resolve("yierdis-server/pom.xml"))
                && Files.isRegularFile(base.resolve("yierdis-server/pom.xml"))
                && Files.isRegularFile(base.resolve("yierdis-networking/pom.xml"))
                && Files.isDirectory(base.resolve("yierdis-db/yierdis-db-memory/src/main/java"))
                && Files.isDirectory(base.resolve("yierdis-server/yierdis-server-main/src/main/java"))) {
            return base.normalize();
        }
        return null;
    }
}
