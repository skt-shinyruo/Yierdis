package yier.bubu.redis.architecture;

import com.tngtech.archunit.core.domain.Dependency;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class ArchitectureDependencyRuleTest {
    private static final JavaClasses PRODUCTION_CLASSES = new ClassFileImporter()
            .withImportOption(new ImportOption.DoNotIncludeTests())
            .importPackages("yier.bubu.redis", "io.netty");

    @Test
    public void commandImplementationDoesNotDependOnProtocolStorageInternalsExecutorOrNetty() {
        assertNoDependencies(
                "command implementation boundary",
                name -> name.startsWith("yier.bubu.redis.command."),
                List.of(
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.storage.memory.",
                        "yier.bubu.redis.memory.api.",
                        "yier.bubu.redis.execution.executor.",
                        "io.netty."
                )
        );
    }

    @Test
    public void storageMemoryDoesNotDependOnCommandProtocolExecutorOrNetty() {
        assertNoDependencies(
                "storage-memory boundary",
                name -> name.startsWith("yier.bubu.redis.storage.memory."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.execution.executor.",
                        "io.netty."
                )
        );
    }

    @Test
    public void executorCoreDoesNotDependOnCommandStorageRuntimeProtocolOrNetty() {
        assertNoDependencies(
                "executor-core boundary",
                name -> name.startsWith("yier.bubu.redis.execution.executor."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.storage.memory.",
                        "yier.bubu.redis.runtime.",
                        "yier.bubu.redis.protocol.",
                        "io.netty."
                )
        );
    }

    @Test
    public void executionApiDoesNotDependOnImplementationLayers() {
        assertNoDependencies(
                "execution-api boundary",
                name -> name.startsWith("yier.bubu.redis.execution.api."),
                List.of(
                        "yier.bubu.redis.command.",
                        "yier.bubu.redis.storage.memory.",
                        "yier.bubu.redis.runtime.",
                        "yier.bubu.redis.protocol.",
                        "yier.bubu.redis.storage.api.",
                        "io.netty."
                )
        );
    }

    private static void assertNoDependencies(
            String ruleName,
            Predicate<String> originMatcher,
            List<String> forbiddenPrefixes
    ) {
        List<String> offenders = new ArrayList<>();
        for (JavaClass origin : PRODUCTION_CLASSES) {
            String originName = origin.getName();
            if (!originMatcher.test(originName)) {
                continue;
            }
            for (Dependency dependency : origin.getDirectDependenciesFromSelf()) {
                String targetName = dependency.getTargetClass().getName();
                for (String forbiddenPrefix : forbiddenPrefixes) {
                    if (targetName.startsWith(forbiddenPrefix)) {
                        offenders.add(ruleName + ": " + originName + " -> " + targetName);
                    }
                }
            }
        }
        Assert.assertTrue(String.join("\n", offenders), offenders.isEmpty());
    }
}
