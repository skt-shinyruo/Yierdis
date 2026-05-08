---
name: use-jdk25
description: Use when working in the Yierdis repository and a command must run Maven, Java, javac, tests, packaging, scripts, smoke checks, or benchmarks with the local JDK 25 installation.
---

# Use JDK 25

Yierdis requires JDK 25. The local machine has OpenJDK 25 at:

```bash
/usr/lib/jvm/java-25-openjdk-amd64
```

## Commands

In an interactive zsh shell, run `jdk25` before Java/Maven work. `jdk25` is a function from `~/.zshrc`, not a standalone binary. Use `jdk` to print the active `JAVA_HOME`, `java -version`, and `javac -version`.

For Codex tool calls, scripts, or any non-interactive shell, prefer the explicit prefix because `~/.zshrc` may not be loaded:

```bash
JAVA_HOME=/usr/lib/jvm/java-25-openjdk-amd64 PATH=/usr/lib/jvm/java-25-openjdk-amd64/bin:$PATH mvn test
```

Use the same prefix for `mvn`, `java`, `javac`, `./scripts/smoke.sh`, and `./scripts/bench.sh`.
