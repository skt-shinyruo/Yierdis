# Command Composition Unification Design

## Status

Design approved in conversation on 2026-06-27.

## Goal

Make command registration explicit and uniform across all reusable runtime entry points.

After this change:

- `YierdisFastCommandProcessor` must stop self-registering transaction commands.
- Every reusable runtime or helper entry point must declare its command surface through one explicit composition root.
- The processor and engine layers must execute against an already-built registry instead of participating in command assembly.

## Problem

The current command surface is assembled through three different mechanisms:

- `YierdisFastCommandProcessor` implicitly registers `TransactionCommands`.
- `DefaultCommandModules` aggregates the transport-neutral built-in commands.
- `YierdisServerBootstrap` adds server-local commands through `ServerCommandModule`.

That split has four costs:

1. There is no single source file that answers "which commands does this runtime support?"
2. `YierdisFastCommandProcessor` mixes execution with composition-root behavior.
3. Transaction support looks like a built-in processor feature even though it is really a command bundle plus queueing behavior.
4. Embedded and test helpers can drift because they assemble command surfaces ad hoc.

The hardest coupling is `TransactionCommands`. `EXEC` replays queued requests by calling back into `YierdisFastCommandProcessor`, which is why the processor currently constructs and registers the transaction module itself. As long as that coupling remains, command assembly cannot become fully explicit.

## Scope

This design covers:

- `yierdis-command-core` command assembly and transaction replay boundaries.
- `YierdisFastCommandProcessor` and `DefaultYierdisEngine` construction APIs.
- Server, embedded, and reusable test-helper command entry points.
- Architecture and unit/integration tests that enforce the new steady state.

This design does not require every low-level unit test to use a runtime composition root. Focused processor/registry unit tests may still build a registry directly when they are testing processor behavior in isolation rather than a reusable runtime entry point.

## Non-Goals

- Do not change Redis command semantics.
- Do not change the existing built-in command implementations inside `DefaultCommandModules`.
- Do not move server-local commands such as `HELLO`, `INFO`, and `STATS` into `command-core`.
- Do not move transaction queue policy out of `YierdisFastCommandProcessor`.
- Do not introduce plugin discovery, dynamic module loading, or configuration-driven command enabling.
- Do not change RESP protocol behavior, executor scheduling, or server lifecycle wiring beyond construction APIs needed for explicit command composition.

## Design

### Separate assembly from execution

`YierdisFastCommandProcessor` should become an execution-only type.

It should no longer accept `CommandModule...` or `Iterable<? extends CommandModule>` constructor inputs. It should instead execute against a fully built `CommandRegistry`.

Recommended public construction shape:

```java
public YierdisFastCommandProcessor(CommandRegistry registry)
public YierdisFastCommandProcessor(
        YierdisCommandProcessorOptions options,
        CommandRegistry registry
)
```

The processor keeps:

- request validation
- null bulk rejection rules
- transaction queue policy
- exception translation
- change emission
- spec lookup and execution

The processor loses:

- implicit transaction command registration
- helper logic that registers caller-supplied modules
- any knowledge of default, server-local, or embedded command bundles

### Keep command assembly mechanism in `command-core`

`command-core` should provide the neutral mechanism for turning modules into a runtime registry.

Add a utility such as:

```java
public final class CommandRegistries {
    public static CommandRegistry from(CommandModule... modules)
    public static CommandRegistry from(Iterable<? extends CommandModule> modules)
    public static void registerInto(CommandRegistry registry, CommandModule... modules)
    public static void registerInto(CommandRegistry registry, Iterable<? extends CommandModule> modules)
}
```

Behavior:

- allocate a fresh `CommandRegistry`
- reject null modules
- register modules in the order provided
- return the completed registry

This utility is intentionally small. It is enough to standardize explicit assembly without introducing an extra abstraction layer whose only job would be to forward registration.

`CommandModule` remains the smallest reusable registration unit.
`CommandRegistry` remains the final runtime lookup structure.
`CommandRegistries` becomes the canonical bridge between the two.

`registerInto(...)` exists for reusable composition roots that must create the processor before they can instantiate `TransactionCommands(processor::execute)`.

### Decouple transaction commands from the processor

Introduce a small replay abstraction in `command-core`:

```java
public interface QueuedCommandReplayer {
    void replay(ExecutionRequest request, CommandContext ctx);
}
```

`TransactionCommands` should depend on `QueuedCommandReplayer`, not on `YierdisFastCommandProcessor`.

`EXEC` then becomes:

- drain queued requests from the transaction state
- for each request, create the replay `CommandContext`
- invoke `replayer.replay(queuedRequest, replayCtx)`

This keeps transaction replay behavior unchanged while removing the reason for processor self-registration.

The concrete replayer for the current runtime can simply be `processor::execute`.

### Keep transaction queueing in the processor

This design deliberately does not move `TransactionQueuePolicy` into the transaction module.

The split should be:

- `TransactionCommands` is responsible for explicit transaction commands such as `MULTI`, `EXEC`, and `DISCARD`.
- `YierdisFastCommandProcessor` remains responsible for the execution-path rule that normal commands queue instead of executing immediately when the connection is inside `MULTI`.

That boundary keeps transaction command registration explicit without smearing command-dispatch flow control into a command module.

### Make `DefaultYierdisEngine` execution-only

`DefaultYierdisEngine` should stop accepting command modules or command-processor options for assembly.

Recommended public shape:

```java
public DefaultYierdisEngine(
        YierdisFastCommandProcessor commandProcessor,
        Runnable maintenanceTick
)
```

`DefaultYierdisEngine` should only:

- adapt `Session` into `CommandContext`
- delegate execution to the already-built processor
- run `maintenanceTick`

This change ensures the engine layer no longer hides command assembly behind convenience constructors.

### Explicit composition roots per reusable runtime

Each reusable runtime entry point must have exactly one readable command composition root.

Recommended roots:

- `ServerCommandComposition` in `yierdis-server-main`
- `EmbeddedCommandComposition` in the embedded/runtime helper area
- `TestCommandComposition` in reusable integration/helper test code if a separate reusable helper surface still exists

Each root should expose at least one explicit factory that makes the final command set obvious. For runtimes that include transaction replay, `createProcessor(...)` should be the primary API because the root may need the processor instance while completing registry assembly.

```java
public final class ServerCommandComposition {
    public static YierdisFastCommandProcessor createProcessor(...)
    public static CommandRegistry createRegistry(...)
}
```

The important rule is not the exact factory signature ordering. The important rule is that each reusable entry point has one file that makes the final command set obvious.

### Composition content by runtime

`ServerCommandComposition` should explicitly assemble:

1. `new TransactionCommands(replayer)`
2. `DefaultCommandModules.create(...)`
3. `new ServerCommandModule(infoProvider)`

`EmbeddedCommandComposition` should explicitly assemble:

1. `new TransactionCommands(replayer)`
2. `DefaultCommandModules.create(...)`
3. any embedded-only extra modules if they exist

`TestCommandComposition` should explicitly assemble:

1. `new TransactionCommands(replayer)` when the helper claims transaction support
2. `DefaultCommandModules.create(...)` when the helper claims built-in command support
3. any helper-specific extra modules

The runtime root owns the final list. `DefaultCommandModules` remains a transport-neutral built-in bundle, not the global composition root.

### Construction flow after the change

The steady-state server construction flow should look like:

```java
CommandRegistry registry = new CommandRegistry();
YierdisFastCommandProcessor processor =
        new YierdisFastCommandProcessor(commandProcessorOptions, registry);
CommandRegistries.registerInto(
        registry,
        new TransactionCommands(processor::execute),
        DefaultCommandModules.create(...),
        new ServerCommandModule(infoProvider)
);
YierdisEngine engine = new DefaultYierdisEngine(processor, maintenanceTick);
```

This is the recommended pattern for reusable runtime roots:

1. create the `CommandRegistry`
2. create the `YierdisFastCommandProcessor` bound to that registry
3. register modules into that registry using a replayer backed by that processor
4. expose the processor only after registration is complete

This keeps assembly explicit without reintroducing processor-owned registration. The processor is allowed to hold a reference to a registry that is still being assembled, but that registry mutation must remain fully encapsulated inside the composition root before the processor is published or used.

Implementation must preserve the design rule: transaction modules depend on a narrow replay abstraction, not on the processor type.

### Recommended implementation choice for assembly

Among the explored options, the approved direction is:

- keep the assembly mechanism in `command-core`
- keep runtime-specific command sets in their owning layers
- do not centralize all runtime profiles into `command-core`

That means:

- `command-core` owns `CommandRegistries` and `QueuedCommandReplayer`
- `server-main` owns the server command root
- embedded/test helper areas own their own roots

This preserves layering while making assembly explicit everywhere.

### Update tests and architecture guards

Tests and guards should enforce the new steady state.

Add or update checks so that:

- `YierdisFastCommandProcessor` no longer exposes varargs/iterable module constructors.
- `YierdisFastCommandProcessor` source no longer contains `new TransactionCommands(this)`.
- `DefaultYierdisEngine` no longer assembles modules internally.
- reusable runtime/helper entry points use their named composition roots.
- server command production code still keeps server-local command parsing isolated to `ServerCommandModule`.

Processor unit tests should shift from "constructor registers kernel commands" assumptions to explicit registry construction. Tests that exist only to prove implicit registration should be rewritten or deleted because implicit registration is the behavior being removed.

## Compatibility Impact

This is an internal Java API breaking change.

Expected breaks:

- callers that instantiate `YierdisFastCommandProcessor` with command modules
- callers that instantiate `DefaultYierdisEngine` with command modules
- tests that assume transaction commands exist on an otherwise empty processor
- reusable helper factories that currently assemble modules ad hoc

Runtime wire behavior must remain unchanged for existing supported runtimes:

- server still supports transaction commands
- server still supports built-in data and connection commands
- server still supports `HELLO`, `INFO`, and `STATS`
- embedded/test helpers that currently expose the same command surface must continue to expose it unless intentionally narrowed

## Migration Plan

Implement in four steps:

1. Introduce `QueuedCommandReplayer` and update `TransactionCommands` to use it.
2. Introduce `CommandRegistries` and change processor/engine APIs to consume explicit assembly results.
3. Add `ServerCommandComposition`, `EmbeddedCommandComposition`, and any reusable `TestCommandComposition`, then move all reusable entry points over.
4. Delete obsolete constructors, update tests, and tighten architecture guards.

This order minimizes the period where both implicit and explicit assembly models coexist.

## Testing Strategy

Run at least:

```bash
mvn -pl yierdis-command/yierdis-command-core test
mvn -pl yierdis-command/yierdis-command-builtin test
mvn -pl yierdis-server/yierdis-server-core test
mvn -pl yierdis-server/yierdis-server-main test
mvn -pl yierdis-tests/yierdis-integration-tests test
mvn -pl yierdis-tests/yierdis-architecture-tests test
```

Pay particular attention to:

- transaction command behavior: `MULTI`, `EXEC`, `DISCARD`, abort paths, queued replay
- unknown command behavior on intentionally small registries
- `COMMAND`, `COMMAND COUNT`, and `COMMAND INFO` metadata output
- server startup wiring that expects `HELLO`, `INFO`, `STATS`, and core data commands to coexist

No Redis wire-level behavior should change. Existing integration tests should be enough if command surfaces and transaction replay remain stable.

## Risks

- The transaction replay decoupling is structurally simple but easy to get subtly wrong if context or reply ordering changes during `EXEC`.
- If reusable test helpers do not adopt composition roots, production code could become explicit while helper code stays fragmented.
- Over-centralizing runtime-specific command lists into `command-core` would break layering and recreate the original smell in a different module.
- Keeping both old and new constructors for too long would preserve ambiguity. The cleanup should remove the old construction paths in the same implementation round.
- `COMMAND` output is derived from the final registry. Any accidental omission in a runtime root will surface as missing command metadata and must be caught by tests.
