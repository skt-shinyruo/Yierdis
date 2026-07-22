# Backend Architecture Contract Rewrite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Coordinate the breaking backend contract rewrite across command/runtime, storage/memory, and network/executor, then make the Maven reactor graph, source boundaries, and current architecture documentation executable acceptance gates.

**Architecture:** Three detailed subsystem plans own production migration. This master plan freezes their shared types and execution order so every task ends on a compilable checkpoint, then replaces duplicated dependency allow-lists with a POM-derived graph test and adds final cross-module guards. The server retains one serial owner for the entire instance; no compatibility lane or alternate execution model is introduced.

**Tech Stack:** Java 25, Maven multi-module reactor, JUnit 4, ArchUnit 1.4, SnakeYAML 2.6, Netty 4.1, JDK 25 FFM, RESP2/RESP3, Bash smoke checks.

## Global Constraints

- Implement the approved design in `docs/superpowers/specs/2026-07-21-backend-architecture-contract-rewrite-design.md`.
- This is a coordinated breaking rewrite. Delete superseded public contracts and do not add deprecated adapters, overloads, aliases, reflection fallbacks, or compatibility wrappers.
- Keep Redis-style whole-instance single-thread execution. Do not introduce per-DB, per-key, sharded, or overlapping command owners.
- Every admitted request produces exactly one ordered reply or explicitly closes its connection.
- Reserve the complete reply envelope before mutation. Never automatically replay a mutation after execution begins.
- `yierdis-db-api` and `yierdis-db-memory` contain no RESP formula, RESP byte metric, protocol adapter, or server API dependency.
- `yierdis-db-memory` depends on `yierdis-memory-api`, not `yierdis-memory-ffm`; FFM stays behind `StableMemoryBackend`.
- `server-main` contains CLI conversion, concrete adapters, and final wiring only. HELLO, INFO, STATS, reply rendering, and command implementations live outside it.
- Maven POMs are the sole source of actual dependencies. The policy may forbid edges but must not duplicate allowed dependency lists.
- All Java and Maven commands use `/usr/lib/jvm/java-25-openjdk-amd64`.
- Use `superpowers:test-driven-development` for every production change, `superpowers:systematic-debugging` for unexpected failures, `write-comments` for every changed Java comment/Javadoc, and `superpowers:verification-before-completion` before claiming success.
- Preserve unrelated user changes. Every subsystem task ends with its focused owner-module tests green and a reviewable commit. A contract-first task may leave only its explicitly named downstream consumers uncompilable until the immediately following migration task, as approved in the design; do not add an adapter to hide that coordinated break.
- No ignored or disabled regression test is an acceptable completion condition.

---

## Detailed Subsystem Plans

Execute the exact steps and commits in these documents; this master plan owns ordering and cross-plan consistency:

1. `docs/superpowers/plans/2026-07-21-backend-command-runtime-rewrite.md`
2. `docs/superpowers/plans/2026-07-21-backend-storage-memory-rewrite.md`
3. `docs/superpowers/plans/2026-07-21-backend-network-executor-rewrite.md`

## Frozen Shared Contract Ownership

| Module | Sole owner | Contract consumed by other plans |
| --- | --- | --- |
| `yierdis-server-api` | complete command session | `CommandSession extends DbIndexSession, ClientMetadataSession, TransactionSession, ConnectionStatsSession, ProtocolNegotiationSession` |
| `yierdis-server-api` | prepared execution | `CommandPreparationContext`, `CommandExecutionContext`, `PreparedCommand`, `ValidationResult` |
| `yierdis-server-api` | semantic reply planning | `ReplyShape`, `ReplyShapes`, `ReplyPlan`, `ReplySizer` |
| `yierdis-server-api` | transport-neutral reply ownership | `ExecutionReply`, `ReplyReservationResult`, `CapacityRegistration` |
| `yierdis-server-executor` | command preparation port | `PreparedCommand CommandExecutionEngine.prepare(CommandSession, ExecutionRequest)` |
| `yierdis-server-executor` | serial scheduling and admission | `SerialOwnerExecutor`, `ExecutorAdmission`, `ExecutorAdmissionAttempt` |
| `yierdis-command-api` | one command definition | `CommandSyntax`, `CommandArity`, `CommandKeySpec`, `TransactionPolicy`, `CommandDefinition`, `CommandPreparer` |
| `yierdis-command-api` | server command inputs | `ServerIdentity`, `ServerSnapshotProvider`, immutable snapshot records |
| `yierdis-db-api` | runtime composition | `DbEngineConfig`, `RuntimeDbEngine`, `CommitPublishingDbEngine`, `GlobalMaxmemoryDbEngine`, `DefragmentableDbEngine` |
| `yierdis-db-api` | semantic result views | `ByteValue`, `ByteValueSink`, `PayloadLengthSink`, `ByteSequenceSource`, `ByteMapSource`, `PreparedMutation` |
| `yierdis-memory-api` | stable memory abstraction | `StableMemoryBackend`, `StableMemoryRegion`, allocation scopes, `NativeHandle(long allocatorId, long localRaw)` |
| `yierdis-networking-netty` | transport state | `OutboundWaitKind`, `OutboundCapacityRegistration`, `InboundPauseReason`, `InboundReadControl` |

The following signatures are normative final-state contracts. The explicitly named intermediate checkpoints in the required order may retain the predecessor `execute(...)` engine boundary only until Command/runtime Task 3 atomically replaces it with `prepare(...)` and deletes `execute(...)`; no compatibility overload or second final version is permitted. A child plan must be corrected before execution if its final state defines a second version:

```java
public interface PreparedCommand extends AutoCloseable {
    ReplyShape replyShape();
    ValidationResult validateBeforeExecute();
    void execute(CommandExecutionContext context);
    @Override void close();
}

public interface ReplySizer {
    ReplyPlan plan(CommandSession session, ReplyShape shape);
}

public interface SerialOwnerExecutor extends java.util.concurrent.Executor {
    boolean inOwnerThread();
}

public interface CommandExecutionEngine {
    PreparedCommand prepare(CommandSession session, ExecutionRequest request);
}

public record NativeHandle(long allocatorId, long localRaw) {
}
```

`db-api` does not import these server reply types. A command preparer adapts a DB source to `ReplyShape.PayloadLengths` with a method reference or lambda, retaining the original source without copying. `server-api` does not import DB result types.

## Required Execution Order

Use this order even though the detailed plans are separate documents:

1. Command/runtime Task 1: replace marker `Session` and required transaction defaults while retaining the current execute-shaped engine method for this checkpoint.
2. Command/runtime Task 2: make `CommandSyntax` the single arity, metadata, and transaction-policy source.
3. Storage/memory Task 1: introduce `DbEngineConfig`, required runtime lifecycle, explicit capabilities, and the lower runtime `DbDefragConfig` migration with startup validation, without depending on grouped server configuration.
4. Storage/memory Tasks 2-4: allocator-scoped handle identity, stable backend/FFM facade, and `db-memory` decoupling.
5. Network/executor Task 1: move reply ownership contracts to `server-api`, add `SerialOwnerExecutor`, and add two-phase executor admission without depending on prepared execution.
6. Storage/memory Task 5: expose protocol-neutral scalar, sequence, map, and prepared-mutation sources.
7. Command/runtime Task 3: atomically introduce prepared commands, semantic reply shapes, RESP sizing, command preparation, EXEC envelope preparation, and the initial executor prepared lifecycle.
8. Network/executor Task 2: harden the executor-only stale, blocked, post-mutation-failure, and one-reply-or-close state machine using the contracts from step 7.
9. Network/executor Task 3: replace untyped outbound waits with control-admission and per-lease expansion registrations.
10. Network/executor Task 4: delete the transport pending queue and replace binary read pausing with five independent reasons.
11. Command/runtime Task 4: move HELLO, INFO, STATS, snapshot records, and rendering out of `server-main` after transport snapshots have their final shape.
12. Command/runtime Task 5: group network, executor, reply, storage, and maintenance configuration, replace the earlier temporary flat-to-record composition mappings, and finish runtime/bootstrap wiring.
13. Execute Tasks 1-4 in this master plan.

At each numbered boundary, run the exact focused command in the child task. Do not combine commits across boundaries. A planned contract-first compile break must list every affected downstream module and the next task that restores it; an accidental missing type inside the owner module is not an acceptable red checkpoint.

Storage/memory Task 1 deliberately breaks five direct downstream modules: `yierdis-db-memory`, `yierdis-server-main`, `yierdis-architecture-tests`, `yierdis-integration-tests`, and `yierdis-benchmark`. `yierdis-cli` is the exact sixth, transitively affected leaf module because its tests have a test-scoped dependency on `yierdis-server-main`; its source tree consumes no retired storage API and therefore receives no Task 4 source migration. Storage/memory Task 4 is the immediate migration owner for the five direct modules and must include `yierdis-cli` in its broad restoration verification. No adapter, compatibility overload, fallback benchmark snapshot path, alternate per-root composition path, or second DB/backend owner may bridge that checkpoint.

Storage/memory Task 1 is the sole owner of deleting the four flat
`YierdisInstanceConfig` defrag accessors and introducing `DbDefragConfig`.
Before grouped configuration exists, it updates the existing bootstrap and test
factories to construct that record from their current flat input. Storage/memory
Task 4 similarly uses the existing runtime config only for native slot capacity.
Command/runtime Task 5 creates `StorageConfig` and replaces those temporary
composition expressions; it must not repeat the runtime API migration or add a
compatibility accessor.

---

### Task 1: Make The Maven Reactor Graph The Dependency Source Of Truth

**Files:**

- Create: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/MavenReactorGraph.java`
- Create: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/MavenReactorDependencyGraphTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml`

**Interfaces:**

- Consumes: every active reactor `pom.xml`; `modules`, `forbidden_dependencies`, `forbidden_imports`, `source_ownership`, `owned_packages`, and `target_packages` from the YAML policy.
- Produces: one parsed graph used by dependency-policy and documentation tests; no `allowed_dependencies` policy field.

- [ ] **Step 1: Write the failing reactor graph utility and policy tests**

Create `MavenReactorGraph.java`:

```java
package yier.bubu.redis.architecture;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

final class MavenReactorGraph {
    record Module(
            String artifactId,
            Path directory,
            Set<String> productionDependencies,
            boolean hasProductionSources
    ) {
    }

    record Edge(String from, String to) {
    }

    private final Path root;
    private final Map<String, Module> modules;

    private MavenReactorGraph(Path root, Map<String, Module> modules) {
        this.root = root;
        this.modules = Collections.unmodifiableMap(new LinkedHashMap<>(modules));
    }

    static MavenReactorGraph load(Path root) throws Exception {
        Path normalizedRoot = root.toAbsolutePath().normalize();
        ArrayDeque<Path> pending = new ArrayDeque<>();
        LinkedHashSet<Path> visited = new LinkedHashSet<>();
        List<ParsedPom> parsed = new ArrayList<>();
        pending.add(normalizedRoot.resolve("pom.xml"));
        while (!pending.isEmpty()) {
            Path pom = pending.removeFirst().toAbsolutePath().normalize();
            if (!visited.add(pom)) {
                continue;
            }
            ParsedPom value = parsePom(pom);
            parsed.add(value);
            for (String child : value.childModules()) {
                pending.addLast(pom.getParent().resolve(child).resolve("pom.xml"));
            }
        }

        LinkedHashMap<String, Module> modules = new LinkedHashMap<>();
        for (ParsedPom pom : parsed) {
            Module module = new Module(
                    pom.artifactId(),
                    pom.path().getParent(),
                    Set.copyOf(pom.productionDependencies()),
                    Files.isDirectory(pom.path().getParent().resolve("src/main/java"))
            );
            Module previous = modules.put(module.artifactId(), module);
            if (previous != null) {
                throw new IllegalStateException("duplicate reactor artifactId: " + module.artifactId());
            }
        }
        return new MavenReactorGraph(normalizedRoot, modules);
    }

    Path root() {
        return root;
    }

    Map<String, Module> modules() {
        return modules;
    }

    Set<String> artifactIds() {
        return modules.keySet();
    }

    Set<Edge> internalProductionEdges() {
        LinkedHashSet<Edge> edges = new LinkedHashSet<>();
        for (Module module : modules.values()) {
            for (String dependency : module.productionDependencies()) {
                if (modules.containsKey(dependency)) {
                    edges.add(new Edge(module.artifactId(), dependency));
                }
            }
        }
        return Set.copyOf(edges);
    }

    private static ParsedPom parsePom(Path pom) throws Exception {
        if (!Files.isRegularFile(pom)) {
            throw new IllegalStateException("missing active reactor pom: " + pom);
        }
        Document document;
        try (InputStream in = Files.newInputStream(pom)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            document = factory.newDocumentBuilder().parse(in);
        }
        Element project = document.getDocumentElement();
        String artifactId = requiredDirectText(project, "artifactId", pom);
        List<String> childModules = new ArrayList<>();
        for (Element modules : childElements(project, "modules")) {
            for (Element module : childElements(modules, "module")) {
                childModules.add(module.getTextContent().trim());
            }
        }
        LinkedHashSet<String> dependencies = new LinkedHashSet<>();
        for (Element dependencyContainer : childElements(project, "dependencies")) {
            for (Element dependency : childElements(dependencyContainer, "dependency")) {
                String scope = directText(dependency, "scope");
                if (!"test".equals(scope)) {
                    dependencies.add(requiredDirectText(dependency, "artifactId", pom));
                }
            }
        }
        return new ParsedPom(pom, artifactId, List.copyOf(childModules), Set.copyOf(dependencies));
    }

    private static String requiredDirectText(Element parent, String name, Path pom) {
        String value = directText(parent, name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("missing " + name + " in " + pom);
        }
        return value;
    }

    private static String directText(Element parent, String name) {
        for (Element child : childElements(parent, name)) {
            return child.getTextContent().trim();
        }
        return null;
    }

    private static List<Element> childElements(Element parent, String name) {
        List<Element> out = new ArrayList<>();
        NodeList children = parent.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            String actual = child.getLocalName() == null ? child.getNodeName() : child.getLocalName();
            if (child instanceof Element element && name.equals(actual)) {
                out.add(element);
            }
        }
        return out;
    }

    private record ParsedPom(
            Path path,
            String artifactId,
            List<String> childModules,
            Set<String> productionDependencies
    ) {
    }
}
```

Create `MavenReactorDependencyGraphTest.java`:

```java
package yier.bubu.redis.architecture;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class MavenReactorDependencyGraphTest {
    private static Path root;
    private static MavenReactorGraph graph;
    private static Map<String, Object> policy;

    @BeforeClass
    public static void loadInputs() throws Exception {
        root = findRoot();
        graph = MavenReactorGraph.load(root);
        LoaderOptions options = new LoaderOptions();
        options.setAllowDuplicateKeys(false);
        try (InputStream in = Files.newInputStream(root.resolve(
                "yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml"))) {
            Object loaded = new Yaml(new SafeConstructor(options)).load(in);
            policy = stringMap(loaded, "policy root");
        }
    }

    @Test
    public void policyDoesNotDuplicateAllowedDependencies() {
        for (Map.Entry<String, Object> entry : map(policy, "modules").entrySet()) {
            Assert.assertFalse(
                    entry.getKey() + " duplicates the Maven graph with allowed_dependencies",
                    stringMap(entry.getValue(), entry.getKey()).containsKey("allowed_dependencies")
            );
        }
    }

    @Test
    public void policyModulesAndInternalForbiddenNamesResolveToTheActiveReactor() {
        Map<String, Object> modules = map(policy, "modules");
        List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Object> entry : modules.entrySet()) {
            String moduleName = entry.getKey();
            MavenReactorGraph.Module module = graph.modules().get(moduleName);
            if (module == null) {
                errors.add("unknown policy module: " + moduleName);
                continue;
            }
            Map<String, Object> modulePolicy = stringMap(entry.getValue(), moduleName);
            for (Object item : list(modulePolicy, "forbidden_dependencies")) {
                String forbidden = String.valueOf(item);
                if (forbidden.startsWith("yierdis-") && !graph.artifactIds().contains(forbidden)) {
                    errors.add(moduleName + " names unknown internal module " + forbidden);
                }
                if (module.productionDependencies().contains(forbidden)) {
                    errors.add(moduleName + " -> forbidden dependency " + forbidden);
                }
            }
        }
        Assert.assertTrue(String.join("\n", errors), errors.isEmpty());
    }

    @Test
    public void packageOwnershipIsUniqueAndResolvesToProductionSources() {
        Map<String, String> ownerByPackage = new HashMap<>();
        List<String> errors = new ArrayList<>();
        for (Map.Entry<String, Object> entry : map(policy, "target_packages").entrySet()) {
            String owner = entry.getKey();
            Map<String, Object> target = stringMap(entry.getValue(), owner);
            for (Object item : list(target, "owns")) {
                String packageName = String.valueOf(item);
                String previous = ownerByPackage.put(packageName, owner);
                if (previous != null) {
                    errors.add(packageName + " is owned by both " + previous + " and " + owner);
                }
                if (!packageExists(packageName)) {
                    errors.add("owned package has no production source: " + packageName);
                }
            }
            for (Object item : list(target, "retired_from_active_source_tree")) {
                String retired = String.valueOf(item);
                if (packageExists(retired)) {
                    errors.add("retired package still exists: " + retired);
                }
            }
        }
        Assert.assertTrue(String.join("\n", errors), errors.isEmpty());
    }

    private static boolean packageExists(String packageName) {
        String declaration = "package " + packageName + ";";
        for (MavenReactorGraph.Module module : graph.modules().values()) {
            Path sourceRoot = module.directory().resolve("src/main/java");
            if (!module.hasProductionSources()) {
                continue;
            }
            try (java.util.stream.Stream<Path> files = Files.walk(sourceRoot)) {
                if (files.filter(path -> path.toString().endsWith(".java"))
                        .anyMatch(path -> contains(path, declaration))) {
                    return true;
                }
            } catch (java.io.IOException failure) {
                throw new IllegalStateException("cannot scan " + sourceRoot, failure);
            }
        }
        return false;
    }

    private static boolean contains(Path file, String expected) {
        try {
            return Files.readString(file).contains(expected);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException("cannot read " + file, failure);
        }
    }

    private static Path findRoot() {
        for (Path current = Paths.get("").toAbsolutePath(); current != null; current = current.getParent()) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("yierdis-server"))) {
                return current;
            }
        }
        throw new IllegalStateException("cannot locate Yierdis reactor root");
    }

    private static Map<String, Object> map(Map<String, Object> parent, String key) {
        return stringMap(parent.get(key), key);
    }

    private static Map<String, Object> stringMap(Object value, String location) {
        Assert.assertTrue(location + " must be a map", value instanceof Map<?, ?>);
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
            Assert.assertTrue(location + " contains a non-string key", entry.getKey() instanceof String);
            out.put((String) entry.getKey(), entry.getValue());
        }
        return out;
    }

    private static List<?> list(Map<String, Object> parent, String key) {
        Object value = parent.get(key);
        Assert.assertTrue(key + " must be a list", value instanceof List<?>);
        return (List<?>) value;
    }
}
```

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=MavenReactorDependencyGraphTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because every current module policy duplicates `allowed_dependencies`, and several retired `yierdis-core-*`/`yierdis-networking-*` names do not resolve to the active reactor.

- [ ] **Step 3: Remove dependency allow-lists and stale policy module names**

Edit `architecture-policy.yml` so every entry under `modules` contains forbidden rules and optional ownership metadata, but no dependency allow-list. The final `yierdis-server-api` entry illustrates the exact shape:

```yaml
modules:
  yierdis-server-api:
    forbidden_dependencies:
      - yierdis-db-memory
      - yierdis-networking-netty
      - yierdis-server-main
      - yierdis-memory-ffm
      - netty-all
    forbidden_imports:
      - yier.bubu.redis.command
      - yier.bubu.redis.storage
      - yier.bubu.redis.memory.foreign
      - yier.bubu.redis.protocol.resp
      - yier.bubu.redis.app.server
      - io.netty
```

- delete every `allowed_dependencies` key and its list;
- deduplicate repeated forbidden entries;
- remove retired internal names that are not active reactor artifacts, including every `yierdis-core-*`, `yierdis-networking-model`, and `yierdis-networking-codec` entry;
- retain active internal forbidden edges and third-party forbidden artifacts such as `netty-all`;
- keep `forbidden_imports`, `source_ownership`, `owned_packages`, and `target_packages` semantics;
- update module entries to the post-migration POM artifact IDs only.

In `ArchitecturePolicyResourceTest.architecturePolicyResourceNamesCurrentBoundaryRules`, replace the old allow-list assertion with:

```java
Assert.assertFalse(module + " must not duplicate Maven dependencies",
        modulePolicy.containsKey("allowed_dependencies"));
Assert.assertTrue(module + " must declare forbidden_dependencies",
        modulePolicy.get("forbidden_dependencies") instanceof List<?>);
Assert.assertTrue(module + " must declare forbidden_imports",
        modulePolicy.get("forbidden_imports") instanceof List<?>);
```

In `ArchitectureBoundaryTest`, remove only the assertions that use policy text as an allow-list from these methods; retain their direct POM parsing assertions and forbidden-import/forbidden-edge assertions:

```text
commonMemoryMustRemainAProductionDependencyFreeContractModule
memoryApiMustRemainNeutralContractModule
storageApiMustRemainNeutralContractModule
runtimeApiMustRemainNeutralContractModule
storageMemoryAndTestkitMustReplaceCoreDbImplementationModule
runtimeEmbeddedMustDeclareRuntimeApiBoundary
engineMustAvoidFutureProhibitedImplementationFamilies
serverStorageApiImportsMustHaveDirectDependency
serverRuntimeApiImportsMustHaveDirectDependency
```

- [ ] **Step 4: Run graph and existing architecture tests and verify GREEN**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=MavenReactorDependencyGraphTest,ArchitecturePolicyResourceTest,ArchitectureBoundaryTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; actual dependencies come only from active POMs, the YAML contains no allowed-dependency mirror, all policy module names resolve, and package ownership is unique and live.

- [ ] **Step 5: Commit the dependency graph source-of-truth change**

```bash
git add \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/MavenReactorGraph.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/MavenReactorDependencyGraphTest.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitecturePolicyResourceTest.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/ArchitectureBoundaryTest.java \
  yierdis-tests/yierdis-architecture-tests/src/test/resources/architecture-policy.yml
git commit -m "test: derive architecture dependencies from Maven"
```

---

### Task 2: Enforce The Rewritten Cross-Module Contracts

**Files:**

- Create: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/BackendContractRewriteGuardTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java`
- Modify: `yierdis-tests/yierdis-architecture-tests/pom.xml`

**Interfaces:**

- Consumes: the final public types and module layout from all three child plans.
- Produces: executable absence, source-boundary, entry-point, capability, and handle-identity guards.

- [ ] **Step 1: Write failing source and reflection guards**

Create `BackendContractRewriteGuardTest.java`:

```java
package yier.bubu.redis.architecture;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;
import yier.bubu.redis.execution.api.CommandSession;
import yier.bubu.redis.memory.api.NativeHandle;
import yier.bubu.redis.memory.api.StableMemoryBackend;
import yier.bubu.redis.storage.api.CommitPublishingDbEngine;
import yier.bubu.redis.storage.api.DefragmentableDbEngine;
import yier.bubu.redis.storage.api.GlobalMaxmemoryDbEngine;
import yier.bubu.redis.storage.api.RuntimeDbEngine;

public class BackendContractRewriteGuardTest {
    private static final Path ROOT = findRoot();

    @Test
    public void retiredWeakContractsAndTransportQueueAreAbsent() {
        for (String relative : List.of(
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/Session.java",
                "yierdis-server/yierdis-server-api/src/main/java/yier/bubu/redis/execution/api/CommandSessionCapabilities.java",
                "yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/YierdisFastCommandHandler.java",
                "yierdis-server/yierdis-server-main/src/main/java/yier/bubu/redis/app/server/RegisteredRespMessage.java",
                "yierdis-memory/yierdis-memory-api/src/main/java/yier/bubu/redis/memory/api/NativeAllocator.java"
        )) {
            Assert.assertFalse("retired contract remains: " + relative, Files.exists(ROOT.resolve(relative)));
        }
    }

    @Test
    public void executionEntryPointsRequireCommandSession() throws Exception {
        Assert.assertEquals(CommandSession.class,
                yier.bubu.redis.execution.executor.ExecutionConnection.class
                        .getMethod("session").getReturnType());
        Assert.assertEquals(CommandSession.class,
                yier.bubu.redis.execution.executor.CommandExecutionEngine.class
                        .getMethod("prepare", CommandSession.class,
                                yier.bubu.redis.execution.api.ExecutionRequest.class)
                        .getParameterTypes()[0]);
        Assert.assertEquals(CommandSession.class,
                yier.bubu.redis.execution.engine.YierdisEngine.class
                        .getMethod("prepare", CommandSession.class,
                                yier.bubu.redis.execution.api.ExecutionRequest.class)
                        .getParameterTypes()[0]);
    }

    @Test
    public void optionalRuntimeCapabilitiesContainNoDefaultMethods() {
        for (Class<?> capability : List.of(
                CommitPublishingDbEngine.class,
                GlobalMaxmemoryDbEngine.class,
                DefragmentableDbEngine.class
        )) {
            for (Method method : capability.getDeclaredMethods()) {
                Assert.assertFalse(capability.getSimpleName() + "." + method.getName()
                        + " must be required", method.isDefault());
            }
        }
        for (Method method : RuntimeDbEngine.class.getDeclaredMethods()) {
            Assert.assertFalse("RuntimeDbEngine." + method.getName() + " must be required",
                    method.isDefault());
        }
    }

    @Test
    public void nativeHandleIsAllocatorScopedAndStableBackendHasNoDefaults() {
        Assert.assertArrayEquals(
                new String[]{"allocatorId", "localRaw"},
                java.util.Arrays.stream(NativeHandle.class.getRecordComponents())
                        .map(component -> component.getName())
                        .toArray(String[]::new)
        );
        for (Method method : StableMemoryBackend.class.getDeclaredMethods()) {
            Assert.assertFalse("StableMemoryBackend." + method.getName() + " must be required",
                    method.isDefault());
        }
    }

    @Test
    public void storageSourcesContainNoRespKnowledgeOrFfmImplementationImports() throws IOException {
        assertNoSourceText(
                List.of(
                        "yierdis-db/yierdis-db-api/src/main/java",
                        "yierdis-db/yierdis-db-memory/src/main/java"
                ),
                List.of(
                        "import yier.bubu.redis.protocol.",
                        "import yier.bubu.redis.execution.api.",
                        "encodedResp",
                        "RespValueMetrics",
                        "decimalDigits(",
                        "CRLF"
                )
        );
        assertNoSourceText(
                List.of("yierdis-db/yierdis-db-memory/src/main/java"),
                List.of(
                        "import yier.bubu.redis.memory.foreign.",
                        "YierdisFfmMemoryRuntime",
                        "YierdisFfmRegion",
                        "YierdisStableNativeAllocator"
                )
        );
    }

    @Test
    public void serverMainContainsCompositionAndAdaptersButNoCommandOrRenderer() throws IOException {
        assertNoSourceText(
                List.of("yierdis-server/yierdis-server-main/src/main/java"),
                List.of(
                        "implements CommandPreparer",
                        "implements CommandModule",
                        "class ServerInfoRenderer",
                        "class ServerStatsRenderer",
                        "class ServerCommandModule",
                        "implements RedisReplyWriter"
                )
        );
    }

    private static void assertNoSourceText(List<String> roots, List<String> forbidden) throws IOException {
        List<String> errors = new ArrayList<>();
        for (String relativeRoot : roots) {
            Path sourceRoot = ROOT.resolve(relativeRoot);
            try (Stream<Path> files = Files.walk(sourceRoot)) {
                for (Path file : files.filter(path -> path.toString().endsWith(".java")).toList()) {
                    String source = Files.readString(file, StandardCharsets.UTF_8);
                    for (String token : forbidden) {
                        if (source.contains(token)) {
                            errors.add(ROOT.relativize(file) + " contains " + token);
                        }
                    }
                }
            }
        }
        Assert.assertTrue(String.join("\n", errors), errors.isEmpty());
    }

    private static Path findRoot() {
        for (Path current = Paths.get("").toAbsolutePath(); current != null; current = current.getParent()) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("yierdis-server"))) {
                return current;
            }
        }
        throw new IllegalStateException("cannot locate Yierdis reactor root");
    }
}
```

Add direct test-scope dependencies for `yierdis-memory-api`, `yierdis-server-core`, and `yierdis-server-runtime-api` to the architecture-test POM. Do not add implementation dependencies solely to make a forbidden edge invisible.

- [ ] **Step 2: Expand the ArchUnit dependency rules**

In `ArchitectureDependencyRuleTest`, retain the helper functions and replace the forbidden-prefix lists with the post-rewrite boundaries:

```java
@Test
public void commandModulesDoNotDependOnProtocolMemoryExecutorOrNetty() {
    assertNoDependencies(
            "command boundary",
            name -> name.startsWith("yier.bubu.redis.command."),
            List.of(
                    "yier.bubu.redis.protocol.",
                    "yier.bubu.redis.memory.",
                    "yier.bubu.redis.execution.executor.",
                    "io.netty."
            )
    );
}

@Test
public void dbMemoryDependsOnMemoryApiButNotFfmImplementation() {
    assertNoDependencies(
            "db-memory stable backend boundary",
            name -> name.startsWith("yier.bubu.redis.storage.memory."),
            List.of(
                    "yier.bubu.redis.command.",
                    "yier.bubu.redis.protocol.",
                    "yier.bubu.redis.execution.",
                    "yier.bubu.redis.memory.foreign.",
                    "io.netty."
            )
    );
}

@Test
public void executorDependsOnContractsButNotConcreteCommandStorageOrTransport() {
    assertNoDependencies(
            "serial executor boundary",
            name -> name.startsWith("yier.bubu.redis.execution.executor."),
            List.of(
                    "yier.bubu.redis.command.",
                    "yier.bubu.redis.storage.",
                    "yier.bubu.redis.runtime.",
                    "yier.bubu.redis.protocol.",
                    "io.netty."
            )
    );
}
```

Keep the existing execution-API rule and extend its forbidden list with `yier.bubu.redis.storage.` and `yier.bubu.redis.memory.foreign.`. Keep the specialized maxmemory/ledger rule for `YierdisDbMaxmemorySupport` and `internal.ledger`; both remain production classes after the storage migration and must continue to depend only on `memory-api` types.

- [ ] **Step 3: Run the new guards and verify RED against any residue**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=BackendContractRewriteGuardTest,ArchitectureDependencyRuleTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected before all child plans are complete: FAIL and name the remaining marker session, transport queue, native allocator, RESP metric, FFM import, server renderer, default capability, or forbidden class edge. After executing all child tasks, the same command must pass without weakening a guard.

- [ ] **Step 4: Remove only genuine final residue identified by the guards**

Resolve each reported item at its designated owner:

```text
Session/capability cast                 -> command/runtime Task 1
Command descriptor/parser policy       -> command/runtime Task 2
RESP planning or command replay        -> command/runtime Task 3 and network/executor Task 2
HELLO/INFO/STATS implementation        -> command/runtime Task 4
flat config/bootstrap side channel     -> command/runtime Task 5
runtime default capability             -> storage/memory Task 1
NativeAllocator or allocatorless handle-> storage/memory Tasks 2-4
RESP metric or FFM import in db-memory -> storage/memory Task 5 and Task 4
pending transport queue/pause boolean  -> network/executor Task 4
untyped waiter                         -> network/executor Task 3
```

Do not add an exception list. A test that matches no production class must fail or be deleted when its protected class is intentionally retired.

- [ ] **Step 5: Run all architecture tests and verify GREEN**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am test
```

Expected: PASS with the complete architecture-test module; no source guard or ArchUnit rule is skipped.

- [ ] **Step 6: Commit the final contract guards**

```bash
git add \
  yierdis-tests/yierdis-architecture-tests/pom.xml \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/BackendContractRewriteGuardTest.java \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ArchitectureDependencyRuleTest.java
git commit -m "test: enforce rewritten backend contracts"
```

---

### Task 3: Synchronize Current Architecture Documentation With The Reactor

**Files:**

- Create: `yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ModuleArchitectureDocumentationTest.java`
- Modify: `docs/project-docs/module-architecture.md`
- Modify: `docs/project-docs/core-logic-index.md`
- Modify: `docs/project-docs/glossary.md`
- Modify: `docs/project-docs/native-memory-runtime.md`
- Modify: `docs/project-docs/native-allocator-and-handles.md`
- Modify: `docs/project-docs/production-hardening-operations.md`
- Modify: `README.md`

**Interfaces:**

- Consumes: `MavenReactorGraph` from Task 1 and the final names from all subsystem plans.
- Produces: an exact POM-derived internal production-edge block and current operator/developer documentation with no retired contract names.

- [ ] **Step 1: Write the failing documentation graph test**

Create `ModuleArchitectureDocumentationTest.java`:

```java
package yier.bubu.redis.architecture;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class ModuleArchitectureDocumentationTest {
    private static final String START = "<!-- BEGIN POM-DERIVED INTERNAL EDGES -->";
    private static final String END = "<!-- END POM-DERIVED INTERNAL EDGES -->";

    @Test
    public void documentedInternalEdgesEqualProductionPomEdges() throws Exception {
        Path root = findRoot();
        MavenReactorGraph graph = MavenReactorGraph.load(root);
        String document = Files.readString(
                root.resolve("docs/project-docs/module-architecture.md"),
                StandardCharsets.UTF_8
        );
        int start = document.indexOf(START);
        int end = document.indexOf(END);
        Assert.assertTrue("missing POM-derived edge block", start >= 0 && end > start);
        String block = document.substring(start + START.length(), end);
        Set<MavenReactorGraph.Edge> documented = new LinkedHashSet<>();
        for (String line : block.lines().toList()) {
            String trimmed = line.trim();
            if (!trimmed.startsWith("- `") || !trimmed.endsWith("`")) {
                continue;
            }
            String[] pair = trimmed.substring(3, trimmed.length() - 1).split(" -> ", -1);
            Assert.assertEquals("invalid documented edge: " + trimmed, 2, pair.length);
            documented.add(new MavenReactorGraph.Edge(pair[0], pair[1]));
        }

        Set<MavenReactorGraph.Edge> expected = new LinkedHashSet<>();
        for (MavenReactorGraph.Edge edge : graph.internalProductionEdges()) {
            MavenReactorGraph.Module source = graph.modules().get(edge.from());
            MavenReactorGraph.Module target = graph.modules().get(edge.to());
            if (source.hasProductionSources() && target.hasProductionSources()) {
                expected.add(edge);
            }
        }
        Assert.assertEquals(expected, documented);
    }

    private static Path findRoot() {
        for (Path current = Paths.get("").toAbsolutePath(); current != null; current = current.getParent()) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("yierdis-server"))) {
                return current;
            }
        }
        throw new IllegalStateException("cannot locate Yierdis reactor root");
    }
}
```

- [ ] **Step 2: Run the documentation test and verify RED**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ModuleArchitectureDocumentationTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: FAIL because `module-architecture.md` has no exact POM-derived edge block and still describes `Session`, `NativeAllocator`, FFM-coupled DB memory, and server-main-owned server commands.

- [ ] **Step 3: Rewrite the current module and contract documentation**

In `module-architecture.md`:

- retain the human-readable Mermaid view;
- update it for the final POM graph;
- add the two exact marker comments from the test;
- between them list every production-source internal edge in sorted `- `source -> target`` form, with no prose lines that start with `- ``;
- state that POMs define actual edges and `architecture-policy.yml` only forbids edges and owns packages/composition rules.

Update the remaining documents with these exact concepts and names:

```text
CommandSession              complete connection-scoped command contract
SerialOwnerExecutor         one physical owner thread for command and maintenance work
CommandSyntax               sole arity, key-spec, metadata, and transaction-policy source
PreparedCommand             prepare -> reserve -> validate -> execute once -> render
ReplyShape / ReplySizer     semantic reply in server API; RESP formulas in protocol adapter
DbEngineConfig              one named DB factory configuration input
StableMemoryBackend         complete memory API consumed by db-memory
NativeHandle                allocatorId + localRaw; cross-backend access rejected
InboundPauseReason          composable ingress pause ownership
OutboundWaitKind            control admission distinct from lease expansion
ServerSnapshotProvider      pure immutable input to HELLO/INFO/STATS command code
YierdisServerConfig         NetworkConfig, ExecutorConfig, ReplyConfig, StorageConfig, MaintenanceConfig
```

Delete descriptions of these retired paths everywhere under `README.md` and `docs/project-docs`:

```text
Session marker
CommandSessionCapabilities.from
CommandDescriptor arity fallback
CommandReplyPlanner
ReplyPlans in command/storage
YierdisFastCommandHandler.pendingSubmissions
binary inputPausedByReply/inputDisabledByExecutor ownership
NativeAllocator as the db-memory extension point
allocatorless NativeHandle raw identity
db-memory direct YierdisFfmMemoryRuntime/YierdisFfmRegion construction
server-main-owned ServerCommandModule or INFO/STATS renderer
allowed_dependencies in architecture-policy.yml
```

Keep operator-facing CLI flag names stable unless command/runtime Task 5 explicitly changes a flag. Explain the grouped records as internal configuration ownership, and document the single `StorageConfig -> DbEngineConfig` native-defrag path.

- [ ] **Step 4: Run documentation and architecture tests and verify GREEN**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn -pl yierdis-tests/yierdis-architecture-tests -am \
  -Dtest=ModuleArchitectureDocumentationTest,ArchitectureBoundaryTest,ArchitecturePolicyResourceTest \
  -Dsurefire.failIfNoSpecifiedTests=false \
  test
```

Expected: PASS; documented internal production edges equal the POM graph exactly and existing documentation guards find no retired behavior.

- [ ] **Step 5: Verify retired architecture language is absent**

Run:

```bash
rg -n \
  'CommandSessionCapabilities\.from|YierdisFastCommandHandler|pendingSubmissions|allowed_dependencies|allocatorless NativeHandle|server-main-owned ServerCommandModule' \
  README.md docs/project-docs
```

Expected: no output.

- [ ] **Step 6: Commit current architecture documentation**

```bash
git add \
  README.md \
  docs/project-docs/module-architecture.md \
  docs/project-docs/core-logic-index.md \
  docs/project-docs/glossary.md \
  docs/project-docs/native-memory-runtime.md \
  docs/project-docs/native-allocator-and-handles.md \
  docs/project-docs/production-hardening-operations.md \
  yierdis-tests/yierdis-architecture-tests/src/test/java/yier/bubu/redis/architecture/ModuleArchitectureDocumentationTest.java
git commit -m "docs: synchronize rewritten backend architecture"
```

---

### Task 4: Run The Full JDK 25 Acceptance Gate

**Files:**

- Verify only; any correction belongs to the task that owns the failing behavior and receives its own focused regression test and commit.

**Interfaces:**

- Consumes: every child-plan task and master Tasks 1-3.
- Produces: evidence that the complete reactor, architecture rules, loopback server, allocator-sensitive commands, and cleanup paths pass together.

- [ ] **Step 1: Verify deleted API and duplicate-state residue is absent**

Run each command and require no output:

```bash
find yierdis-server -path '*/src/main/java/*' \
  \( -name Session.java -o -name CommandSessionCapabilities.java \
     -o -name YierdisFastCommandHandler.java -o -name RegisteredRespMessage.java \) -print
```

```bash
rg -n \
  'pendingSubmissions|inputPausedByReply|inputDisabledByExecutor|ReplyCapacityUnavailableException|CommandDescriptor|CommandReplyPlanner|allowed_dependencies' \
  --glob '*.java' --glob '*.yml' \
  yierdis-command yierdis-db yierdis-memory yierdis-networking yierdis-server yierdis-tests
```

```bash
rg -n \
  'import yier\.bubu\.redis\.memory\.foreign|YierdisFfmMemoryRuntime|YierdisFfmRegion|YierdisStableNativeAllocator|RespValueMetrics|encodedResp|decimalDigits' \
  --glob '*.java' \
  yierdis-db/yierdis-db-api/src/main/java \
  yierdis-db/yierdis-db-memory/src/main/java
```

Expected: all three commands produce no output. A match is fixed in its owning subsystem; it is not added to an allow-list.

- [ ] **Step 2: Verify no regression test was disabled**

Run:

```bash
rg -n '@Ignore|@Disabled' --glob '*.java' .
```

Expected: no output.

- [ ] **Step 3: Run the complete Maven reactor**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
mvn clean test
```

Expected: BUILD SUCCESS; all unit, architecture, and integration modules pass on JDK 25.

- [ ] **Step 4: Package and run the loopback smoke suite with allocator-sensitive commands**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \
ALLOCATOR_SMOKE=1 \
PORT=16379 \
scripts/smoke.sh
```

Expected output ends with `[smoke] done`; PING, SET/GET, APPEND, list, hash, set, sorted-set, and delete paths succeed against the packaged server.

- [ ] **Step 5: Run a deterministic short soak for lifecycle and budget cleanup**

Run:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 \\
PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH \\
scripts/production-hardening-soak.sh --duration-seconds 60 --seed 20260721
```

Expected output ends with `passed`; the generated report contains no result-unknown replay, leaked reply lease, leaked ingress lease, owner-thread violation, or shutdown timeout.

- [ ] **Step 6: Check patch integrity and final repository state**

Run:

```bash
git diff --check
```

Expected: no output.

Run:

```bash
git status --short --branch
```

Expected: the branch contains only intentional committed rewrite work and no generated smoke/soak artifact staged for commit.

Do not create an empty acceptance commit. Record the exact Maven, smoke, and soak results in the implementation handoff.
