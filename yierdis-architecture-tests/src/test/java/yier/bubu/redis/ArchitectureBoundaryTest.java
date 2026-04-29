package yier.bubu.redis;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ArchitectureBoundaryTest {
    @Test
    public void dbOpsAndCoreCommandMustNotImportProtocolModel() throws IOException {
        List<String> offenders = new ArrayList<>();
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        int scanned = 0;
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-api/src/main/java/yier/bubu/redis/ops"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "import yier.bubu.redis.protocol."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到协议模型依赖泄漏（core-db/core-api/core-command 禁止 import yier.bubu.redis.protocol.*）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void runtimeMustNotOwnCommandAssemblyAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path instanceFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java，无法执行 runtime 边界护栏", Files.isRegularFile(instanceFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                instanceFile,
                offenders,
                "import yier.bubu.redis.command.ServerInfoProvider;",
                "import yier.bubu.redis.command.SlowCommandGovernor;",
                "import yier.bubu.redis.command.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "newCommandProcessor("
        );

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 core-runtime 重新承担命令处理器组装/装配职责（YierdisInstance 应只负责 DB 生命周期与路由）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreDefaultsMustNotOwnServerFacingCommands() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        Path serverCommands = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/ServerCommands.java"
        );
        if (Files.exists(serverCommands)) {
            offenders.add(relativePath(repoRoot, serverCommands) + " (server-facing commands should live in yierdis-server)");
        }

        Path processorFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java"
        );
        Assert.assertTrue("缺少 YierdisFastCommandProcessor.java，无法执行 core-command 默认装配护栏", Files.isRegularFile(processorFile));
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
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "register(\"HELLO\"",
                "register(\"INFO\"",
                "register(\"STATS\""
        );
        Path coreConnectionFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/CoreConnectionCommands.java"
        );
        Assert.assertTrue("缺少 CoreConnectionCommands.java，无法执行 COMMAND metadata 护栏", Files.isRegularFile(coreConnectionFile));
        scanFileForForbiddenText(
                repoRoot,
                coreConnectionFile,
                offenders,
                "case \"HELLO\":",
                "case \"INFO\":",
                "case \"STATS\":"
        );
        Path descriptorFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandDescriptor.java"
        );
        Assert.assertTrue("缺少 CommandDescriptor.java，无法执行 COMMAND descriptor 护栏", Files.isRegularFile(descriptorFile));
        scanFileForForbiddenText(
                repoRoot,
                descriptorFile,
                offenders,
                "case \"HELLO\":",
                "case \"INFO\":",
                "case \"STATS\":"
        );
        Path registryFile = repoRoot.resolve(
                "yierdis-core-command/src/main/java/yier/bubu/redis/command/CommandRegistry.java"
        );
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
                    "检测到 server-facing commands 回流到 core 默认装配（这些命令应由 yierdis-server 组装）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreCommandMustNotReferenceLegacyWriteReservationApis() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
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
                    "检测到 core-command 仍依赖 legacy 写预留/混合 DB API：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreCommandMustStayIndependentFromMemoryApi() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);
        Path workspaceRoot = repoRoot.getParent();

        Path policyFile = workspaceRoot.resolve("yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String coreCommandPolicy = policySection(policy, "yierdis-core-command");
        Assert.assertTrue(
                "core-command policy must forbid direct memory-api dependency",
                coreCommandPolicy.contains("yierdis-memory-api")
        );
        Assert.assertTrue(
                "core-command policy must forbid offheap API imports",
                coreCommandPolicy.contains("yier.bubu.redis.offheap.api")
        );

        Path commandPom = repoRoot.resolve("yierdis-core-command/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-core-command/pom.xml", Files.isRegularFile(commandPom));
        String pom = Files.readString(commandPom, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "yierdis-core-command must not depend on yierdis-memory-api",
                pom.contains("<artifactId>yierdis-memory-api</artifactId>")
        );

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "import yier.bubu.redis.offheap.api.",
                "yier.bubu.redis.offheap.api."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-core-command Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 core-command 依赖 memory-api/offheap API（命令层只能接收 DB/API 层转换后的错误）：\n"
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
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java"),
                "parseSet(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/StringCommands.java"),
                "set(SetArgs args, CommandContext ctx)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java"),
                "parseScan(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/KeyCommands.java"),
                "scan(ScanArgs args, CommandContext ctx)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java"),
                "parseZRange(ArgReader args)",
                offenders,
                "out.error(\"ERR syntax error\")"
        );
        scanMethodForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/ZSetCommands.java"),
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
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisHashOps.java"),
                offenders,
                "wrong number of arguments for 'hset' command"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisZSetOps.java"),
                offenders,
                "wrong number of arguments for 'zadd' command"
        );
        Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void replaySurfacesMustUseExecutionContracts() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-api/src/main/java/yier/bubu/redis/runtime/api/YierdisChangeEvent.java"),
                offenders,
                "byte[][] argv"
        );
        Path transactionStateFile = repoRoot.getParent().resolve(
                "yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/TransactionState.java"
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
                repoRoot.resolve("yierdis-core-engine/src/main/java/yier/bubu/redis/engine/EngineSession.java"),
                offenders,
                "ArrayList<byte[][]>",
                "List<byte[][]>",
                "tryEnqueue(byte[][]"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/TransactionCommands.java"),
                offenders,
                "drainRequests(",
                "new QueuedCommand(",
                "new QueuedExecutionRequest("
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command/YierdisFastCommandProcessor.java"),
                offenders,
                "tx.tryEnqueue(ByteArrayExecutionRequest.copyOf(request))"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis/ProtocolCommandAdapter.java").normalize(),
                offenders,
                "new AdaptedCommand("
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis/YierdisFastCommandHandler.java").normalize(),
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command"),
                offenders,
                "CommandModule.Handler",
                "new CommandSpec(",
                "register(String name, Handler",
                "registerDisallowedInMulti(String name, Handler"
        );
        scanFilesMatchingRegex(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java/yier/bubu/redis/command"),
                offenders,
                "registration\\.register\\(\\s*\"[A-Z0-9_]+\"\\s*,\\s*this::"
        );
        scanFilesMatchingRegex(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis").normalize(),
                offenders,
                "registration\\.register\\(\\s*\"[A-Z0-9_]+\"\\s*,\\s*this::",
                "registration\\.registerDisallowedInMulti\\("
        );

        Assert.assertTrue("legacy command registration remains:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void productionCodeMustNotUseDeprecatedCommandRequestCompatibility() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-command/src/main/java"),
                offenders,
                "import yier.bubu.redis.contract.Command;",
                "instanceof yier.bubu.redis.contract.Command",
                "execute(Command"
        );
        scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.contract.Command;",
                "SimpleChannelInboundHandler<Command>",
                "instanceof Command"
        );

        Assert.assertTrue("deprecated Command compatibility remains:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void storagePressurePathsMustUseKeyHandles() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-api/src/main/java/yier/bubu/redis/ops/MaxmemoryCandidate.java"),
                offenders,
                "byte[] key"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbExpirationSupport.java"),
                offenders,
                ".randomKey()"
        );
        scanFileForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDbMaxmemorySupport.java"),
                offenders,
                ".randomKey()",
                ".forEach("
        );

        Assert.assertTrue("storage pressure paths must use KeyHandle identities:\n" + String.join("\n", offenders), offenders.isEmpty());
    }

    @Test
    public void engineAndExecutorMustExposeSessionRequestReplyBoundary() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path engineFile = repoRoot.resolve(
                "yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java"
        );
        Assert.assertTrue("缺少 YierdisEngine.java，无法约束 engine 执行边界", Files.isRegularFile(engineFile));
        String engineSource = Files.readString(engineFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "YierdisEngine public execution boundary must expose Session + ExecutionRequest + ReplyWriter",
                engineSource.contains("void execute(Session session, ExecutionRequest request, ReplyWriter out);")
        );
        Assert.assertFalse(
                "YierdisEngine public API must not expose CommandContext compatibility overloads",
                engineSource.contains("CommandContext")
        );

        Path executorEngineFile = repoRoot.getParent().resolve(
                "yierdis-executor-core/src/main/java/yier/bubu/redis/executor/CommandExecutionEngine.java"
        ).normalize();
        Assert.assertTrue("缺少 CommandExecutionEngine.java，无法约束 executor 执行边界", Files.isRegularFile(executorEngineFile));
        String executorEngineSource = Files.readString(executorEngineFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "CommandExecutionEngine must accept Session + ExecutionRequest + ReplyWriter",
                executorEngineSource.contains("void execute(Session session, ExecutionRequest request, ReplyWriter out);")
        );
        Assert.assertFalse(
                "executor-core execution seam must not expose CommandContext",
                executorEngineSource.contains("CommandContext")
        );
    }

    @Test
    public void executorCoreMustNotOwnCommandSessionSemantics() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-executor-core/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.contract.CommandContext;",
                "new CommandContext(",
                "import yier.bubu.redis.contract.ServerSession;",
                "import yier.bubu.redis.contract.TransactionState;",
                "DefaultExecutionSession",
                "DefaultTransactionState",
                "setDbIndex(",
                "dbIndex()",
                "clientName",
                "authenticated"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-executor-core Java 文件", scanned > 0);

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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path serverRoot = repoRoot.getParent().resolve("yierdis-server/src/main/java").normalize();
        Path serverCommandModule = serverRoot.resolve("yier/bubu/redis/ServerCommandModule.java").normalize();
        List<Path> allowedServerFiles = List.of(serverCommandModule);

        List<String> offenders = new ArrayList<>();
        int scanned = 0;
        scanned += scanForForbiddenTextExcluding(
                repoRoot,
                serverRoot,
                offenders,
                allowedServerFiles,
                "import yier.bubu.redis.command.CommandParsers;",
                "import yier.bubu.redis.command.CommandSpec;",
                "import yier.bubu.redis.command.ArgReader;",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-executor-core/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        scanned += scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-runtime/src/main/java"),
                offenders,
                "import yier.bubu.redis.command.CommandParsers;",
                "import yier.bubu.redis.command.CommandSpec;",
                "import yier.bubu.redis.command.ArgReader;",
                "CommandParseResult",
                "wrong number of arguments for"
        );
        for (Path protocolMain : List.of(
                repoRoot.getParent().resolve("yierdis-protocol/yierdis-protocol-model/src/main/java").normalize(),
                repoRoot.getParent().resolve("yierdis-protocol/yierdis-protocol-codec/src/main/java").normalize(),
                repoRoot.getParent().resolve("yierdis-protocol/yierdis-protocol-netty/src/main/java").normalize()
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
                repoRoot.resolve("yierdis-core-db/src/main/java"),
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
    public void protocolCodecMustNotDependOnCoreContract() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path codecPom = repoRoot.resolve("yierdis-protocol/yierdis-protocol-codec/pom.xml");
        if (!Files.isRegularFile(codecPom) && repoRoot.getParent() != null) {
            codecPom = repoRoot.getParent().resolve("yierdis-protocol/yierdis-protocol-codec/pom.xml");
        }
        Assert.assertTrue("缺少 yierdis-protocol-codec/pom.xml", Files.isRegularFile(codecPom));

        String pom = Files.readString(codecPom, StandardCharsets.UTF_8);
        Assert.assertFalse("protocol-codec 不应再依赖 yierdis-core-contract", pom.contains("<artifactId>yierdis-core-contract</artifactId>"));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                codecPom.getParent().resolve("src/main/java"),
                offenders,
                "import yier.bubu.redis.contract.",
                "yier.bubu.redis.contract."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 Java 文件（请检查测试工作目录/构建配置）", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 protocol-codec 仍依赖 core-contract：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void executionApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);
        Path workspaceRoot = repoRoot.getParent();

        Path rootPom = workspaceRoot.resolve("pom.xml").normalize();
        Assert.assertTrue("缺少 root pom.xml", Files.isRegularFile(rootPom));
        String rootPomText = Files.readString(rootPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "root pom.xml must aggregate yierdis-execution",
                rootPomText.contains("<module>yierdis-execution</module>")
        );

        Path executionPom = workspaceRoot.resolve("yierdis-execution/pom.xml").normalize();
        Path apiPom = workspaceRoot.resolve("yierdis-execution/yierdis-execution-api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-execution/pom.xml", Files.isRegularFile(executionPom));
        Assert.assertTrue("缺少 yierdis-execution/yierdis-execution-api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String executionPolicy = policySection(policy, "yierdis-execution-api");
        Assert.assertTrue(
                "execution-api policy must forbid Netty imports from execution API",
                executionPolicy.contains("io.netty")
        );
        Assert.assertTrue(
                "execution-api policy must forbid protocol imports from execution API",
                executionPolicy.contains("yier.bubu.redis.protocol")
        );

        Path packageInfo = workspaceRoot.resolve(
                "yierdis-execution/yierdis-execution-api/src/main/java/yier/bubu/redis/contract/package-info.java"
        ).normalize();
        Assert.assertTrue("execution API contracts must document API/SPI audience in package-info.java", Files.isRegularFile(packageInfo));
        String packageInfoText = Files.readString(packageInfo, StandardCharsets.UTF_8);
        for (String requiredClassification : List.of(
                "ExecutionRequest - API",
                "ByteArrayExecutionRequest - API",
                "ExecutionRecord - API",
                "ReplySink - API",
                "ReplyWriter - API",
                "ReplyWriterFactory - API",
                "Session - API",
                "ServerSession - API",
                "DbIndexProvider - API",
                "ConnectionStatsProvider - API",
                "ConnectionStatsView - API",
                "TransactionState - API",
                "CommandContext - API",
                "Command - compatibility/deprecated"
        )) {
            Assert.assertTrue(
                    "execution API package-info.java must classify " + requiredClassification,
                    packageInfoText.contains(requiredClassification)
            );
        }

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-core-command</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-protocol-model</artifactId>",
                "<artifactId>yierdis-protocol-codec</artifactId>",
                "<artifactId>yierdis-protocol-netty</artifactId>",
                "<artifactId>yierdis-server</artifactId>",
                "<artifactId>yierdis-memory-foreign</artifactId>",
                "<artifactId>yierdis-bytes-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-execution-api must not depend on forbidden implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                workspaceRoot.resolve("yierdis-execution/yierdis-execution-api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.db.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.server.",
                "import io.netty.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.db.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.server.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-execution-api Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-execution-api 依赖协议、命令实现、存储实现、运行时实现、应用或 Netty：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void memoryApiMustRemainNeutralContractModule() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);
        Path workspaceRoot = repoRoot.getParent();

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
        Path apiPom = workspaceRoot.resolve("yierdis-memory/api/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-memory/pom.xml", Files.isRegularFile(memoryPom));
        String memoryPomText = Files.readString(memoryPom, StandardCharsets.UTF_8);
        int apiModuleIndex = memoryPomText.indexOf("<module>api</module>");
        int foreignModuleIndex = memoryPomText.indexOf("<module>foreign</module>");
        Assert.assertTrue(
                "yierdis-memory parent must aggregate api before foreign",
                apiModuleIndex >= 0 && foreignModuleIndex >= 0 && apiModuleIndex < foreignModuleIndex
        );
        Assert.assertTrue("缺少 yierdis-memory/api/pom.xml", Files.isRegularFile(apiPom));

        Path policyFile = workspaceRoot.resolve("yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String memoryApiPolicy = policySection(policy, "yierdis-memory-api");
        Assert.assertTrue(
                "memory-api policy must allow only yierdis-bytes-lib as production dependency",
                memoryApiPolicy.contains("yierdis-bytes-lib")
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
                "yierdis-core-runtime",
                "yierdis-protocol-model",
                "yierdis-protocol-codec",
                "yierdis-protocol-netty",
                "yierdis-server",
                "yierdis-memory-foreign",
                "yierdis-bytes-netty",
                "netty-all",
                "yier.bubu.redis.command",
                "yier.bubu.redis.db",
                "yier.bubu.redis.storage",
                "yier.bubu.redis.runtime",
                "yier.bubu.redis.protocol",
                "yier.bubu.redis.server",
                "yier.bubu.redis.db.memory.foreign",
                "io.netty"
        )) {
            Assert.assertTrue(
                    "memory-api policy must forbid " + requiredForbidden,
                    memoryApiPolicy.contains(requiredForbidden)
            );
        }

        String pom = Files.readString(apiPom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-memory-api must depend on yierdis-bytes-lib",
                pom.contains("<artifactId>yierdis-bytes-lib</artifactId>")
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
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-protocol-model</artifactId>",
                "<artifactId>yierdis-protocol-codec</artifactId>",
                "<artifactId>yierdis-protocol-netty</artifactId>",
                "<artifactId>yierdis-server</artifactId>",
                "<artifactId>yierdis-memory-foreign</artifactId>",
                "<artifactId>yierdis-bytes-netty</artifactId>",
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
                workspaceRoot.resolve("yierdis-memory/api/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.",
                "import yier.bubu.redis.db.",
                "import yier.bubu.redis.storage.",
                "import yier.bubu.redis.runtime.",
                "import yier.bubu.redis.protocol.",
                "import yier.bubu.redis.server.",
                "import io.netty.",
                "yier.bubu.redis.command.",
                "yier.bubu.redis.db.",
                "yier.bubu.redis.storage.",
                "yier.bubu.redis.runtime.",
                "yier.bubu.redis.protocol.",
                "yier.bubu.redis.server.",
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
    public void coreContractMustRemainCompatibilityBridge() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path bridgePom = repoRoot.resolve("yierdis-core-contract/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-core-contract/pom.xml", Files.isRegularFile(bridgePom));
        String pom = Files.readString(bridgePom, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "yierdis-core-contract must depend on yierdis-execution-api as a compatibility bridge",
                pom.contains("<artifactId>yierdis-execution-api</artifactId>")
        );
        Assert.assertTrue(
                "yierdis-core-contract description must identify it as a compatibility bridge",
                pom.contains("Temporary compatibility bridge")
        );

        Path sourceRoot = repoRoot.resolve("yierdis-core-contract").resolve("src/main/java").normalize();
        List<String> offenders = new ArrayList<>();
        if (Files.exists(sourceRoot)) {
            try (Stream<Path> paths = Files.walk(sourceRoot)) {
                paths.filter(p -> p != null && p.toString().endsWith(".java"))
                        .sorted()
                        .forEach(file -> offenders.add(relativePath(repoRoot, file)));
            }
        }

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "yierdis-core-contract must not reintroduce main Java contract sources; use yierdis-execution-api:\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void coreEngineMustAvoidFutureProhibitedImplementationFamilies() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);
        Path workspaceRoot = repoRoot.getParent();

        Path policyFile = workspaceRoot.resolve("yierdis-architecture-tests/src/test/resources/architecture-policy.yml").normalize();
        Assert.assertTrue("缺少 architecture-policy.yml", Files.isRegularFile(policyFile));
        String policy = Files.readString(policyFile, StandardCharsets.UTF_8);
        String enginePolicy = policySection(policy, "yierdis-core-engine");
        Assert.assertTrue(
                "core-engine policy must allow current core-command dependency until the command split",
                enginePolicy.contains("yierdis-core-command")
        );
        Assert.assertTrue(
                "core-engine policy must forbid server imports",
                enginePolicy.contains("yier.bubu.redis.server")
        );
        Assert.assertTrue(
                "core-engine policy must forbid Netty imports",
                enginePolicy.contains("io.netty")
        );
        Assert.assertTrue(
                "core-engine policy must forbid concrete DB/runtime dependency",
                enginePolicy.contains("yierdis-core-db")
        );

        Path enginePom = repoRoot.resolve("yierdis-core-engine/pom.xml").normalize();
        Assert.assertTrue("缺少 yierdis-core-engine/pom.xml", Files.isRegularFile(enginePom));
        String pom = Files.readString(enginePom, StandardCharsets.UTF_8);
        for (String forbiddenDependency : List.of(
                "<artifactId>yierdis-protocol-model</artifactId>",
                "<artifactId>yierdis-protocol-codec</artifactId>",
                "<artifactId>yierdis-protocol-netty</artifactId>",
                "<artifactId>yierdis-core-db</artifactId>",
                "<artifactId>yierdis-core-runtime</artifactId>",
                "<artifactId>yierdis-server</artifactId>",
                "<artifactId>yierdis-memory-foreign</artifactId>",
                "<artifactId>yierdis-bytes-netty</artifactId>",
                "<artifactId>netty-all</artifactId>"
        )) {
            Assert.assertFalse(
                    "yierdis-core-engine must not depend on future-prohibited implementation/module dependency "
                            + forbiddenDependency,
                    pom.contains(forbiddenDependency)
            );
        }

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.resolve("yierdis-core-engine/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.defaults.",
                "yier.bubu.redis.command.defaults.",
                "import yier.bubu.redis.storage.",
                "yier.bubu.redis.storage.",
                "import yier.bubu.redis.protocol.",
                "yier.bubu.redis.protocol.",
                "import yier.bubu.redis.runtime.",
                "yier.bubu.redis.runtime.",
                "import yier.bubu.redis.server.",
                "yier.bubu.redis.server.",
                "import yier.bubu.redis.db.",
                "yier.bubu.redis.db.",
                "import io.netty.",
                "io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-core-engine Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-core-engine 依赖 command-defaults、storage-memory、protocol adapter、runtime implementation、application/server、Netty 或 concrete DB runtime：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void protocolRequestAndServerReplyBoundariesMustStayDocumented() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path requestFile = repoRoot.getParent().resolve(
                "yierdis-protocol/yierdis-protocol-model/src/main/java/yier/bubu/redis/protocol/v1/CustomProtocolV1Request.java"
        );
        Assert.assertTrue("缺少 CustomProtocolV1Request.java", Files.isRegularFile(requestFile));
        String requestSource = Files.readString(requestFile, StandardCharsets.UTF_8);
        Assert.assertTrue("request model 必须声明 protocol DTO 边界", requestSource.contains("This is a protocol-layer DTO only"));

        Path readmeFile = repoRoot.getParent().resolve("README.md");
        Assert.assertTrue("缺少 README.md", Files.isRegularFile(readmeFile));
        String readmeSource = Files.readString(readmeFile, StandardCharsets.UTF_8);
        Assert.assertTrue(
                "README 必须声明 ReplyWriter 仍是 server write-back 语义 authority",
                readmeSource.contains("server command execution write-back still uses ReplyWriter")
        );
    }

    @Test
    public void serverSourceMustNotConstructProtocolReplyModelsDirectly() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java"),
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
    public void yierdisDbMustNotRetainLegacyReservationHelpers() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path dbFile = repoRoot.resolve("yierdis-core-db/src/main/java/yier/bubu/redis/db/YierdisDb.java");
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path runtimeAccessFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，说明 runtime 显式访问 seam 未建立", Files.isRegularFile(runtimeAccessFile));

        Path maintenanceFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceMaintenance.java"
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path instanceFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstance.java"
        );
        Assert.assertTrue("缺少 YierdisInstance.java", Files.isRegularFile(instanceFile));

        Path runtimeAccessFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java"
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
    public void serverBootstrapMustNotInlineOwnerThreadLifecycleAgain() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path runtimeAccessFile = repoRoot.resolve(
                "yierdis-core-runtime/src/main/java/yier/bubu/redis/runtime/YierdisInstanceRuntimeAccess.java"
        );
        Assert.assertTrue("缺少 YierdisInstanceRuntimeAccess.java，无法约束 bootstrap 生命周期边界", Files.isRegularFile(runtimeAccessFile));

        Path bootstrapFile = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java").normalize();
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path infoProviderFile = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis/NettyServerInfoProvider.java").normalize();
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path executorRoot = repoRoot.getParent().resolve("yierdis-executor-core").normalize();
        Path executorPom = executorRoot.resolve("pom.xml");
        Assert.assertTrue("缺少 yierdis-executor-core/pom.xml", Files.isRegularFile(executorPom));

        String pom = Files.readString(executorPom, StandardCharsets.UTF_8);
        Assert.assertFalse(
                "yierdis-executor-core must not depend on yierdis-core-command",
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
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-executor-core Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-executor-core 依赖 core-command（executor-core 应只依赖执行契约）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverBootstrapMustWireCommandExecutionThroughYierdisEngine() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path engineFile = repoRoot.resolve(
                "yierdis-core-engine/src/main/java/yier/bubu/redis/engine/YierdisEngine.java"
        );
        Assert.assertTrue(
                "缺少 YierdisEngine facade，server bootstrap 不应继续直接接线 command processor",
                Files.isRegularFile(engineFile)
        );

        Path bootstrapFile = repoRoot.getParent().resolve(
                "yierdis-server/src/main/java/yier/bubu/redis/YierdisServerBootstrap.java"
        ).normalize();
        Assert.assertTrue("缺少 YierdisServerBootstrap.java", Files.isRegularFile(bootstrapFile));

        List<String> offenders = new ArrayList<>();
        scanFileForForbiddenText(
                repoRoot,
                bootstrapFile,
                offenders,
                "import yier.bubu.redis.command.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "processor::execute",
                "maintenance.maintenanceTick()"
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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-server/src/main/java").normalize(),
                offenders,
                "import yier.bubu.redis.command.YierdisFastCommandProcessor;",
                "new YierdisFastCommandProcessor(",
                "new CommandContext(",
                ".execute(request, new CommandContext"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 server 生产代码绕过 YierdisEngine 构造命令处理器或命令上下文：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void executorCoreMustNotDependOnNetty() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                repoRoot.getParent().resolve("yierdis-executor-core/src/main/java").normalize(),
                offenders,
                "import io.netty."
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-executor-core Java 文件", scanned > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-executor-core 重新依赖 Netty（该模块必须保持 transport-neutral）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustNotContainLegacyExecutorRuntimeFiles() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path serverRoot = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis").normalize();
        Assert.assertTrue("缺少 yierdis-server/src/main/java/yier/bubu/redis", Files.isDirectory(serverRoot));

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
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path serverRoot = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis").normalize();
        Assert.assertTrue("缺少 yierdis-server/src/main/java/yier/bubu/redis", Files.isDirectory(serverRoot));

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
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server Java 文件", scanned[0] > 0);

        if (!offenders.isEmpty()) {
            Assert.fail(
                    "检测到 yierdis-server 存在分散的 Channel.attr 所有权（仅允许 NettyExecutionConnection/NettyExecutionIoAdapter 持有）：\n"
                            + String.join("\n", offenders)
            );
        }
    }

    @Test
    public void serverMustNotReachThroughLegacyConnectionSlices() throws IOException {
        Path repoRoot = resolveRepoRoot();
        Assert.assertNotNull("无法定位仓库根目录（未找到 yierdis-core-api/yierdis-core-db 模块）", repoRoot);

        Path serverRoot = repoRoot.getParent().resolve("yierdis-server/src/main/java/yier/bubu/redis").normalize();
        Assert.assertTrue("缺少 yierdis-server/src/main/java/yier/bubu/redis", Files.isDirectory(serverRoot));

        List<String> offenders = new ArrayList<>();
        int scanned = scanForForbiddenText(
                repoRoot,
                serverRoot,
                offenders,
                ".runtime()",
                ".session()",
                ".scheduling()"
        );
        Assert.assertTrue("架构护栏扫描未扫描到任何 yierdis-server Java 文件", scanned > 0);
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

    private static Path resolveRepoRoot() {
        // Maven surefire 下通常为 yierdis-core 模块根目录；IDE/自定义运行环境下可能是仓库根目录。
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

        Path workspaceCore = base.resolve("yierdis-core");
        if (Files.isDirectory(workspaceCore.resolve("yierdis-core-api/src/main/java"))
                && Files.isDirectory(workspaceCore.resolve("yierdis-core-db/src/main/java"))
                && Files.isRegularFile(workspaceCore.resolve("pom.xml"))) {
            return workspaceCore.normalize();
        }

        if (Files.isDirectory(base.resolve("yierdis-core-api/src/main/java"))
                && Files.isDirectory(base.resolve("yierdis-core-db/src/main/java"))
                && Files.isRegularFile(base.resolve("pom.xml"))) {
            return base.normalize();
        }
        return null;
    }
}
