package yier.bubu.redis.app.bench;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class SmokeScriptContractTest {
    @Test
    public void smokeScriptReportsSameReadinessTimeoutItWaitsFor() throws IOException {
        String script = Files.readString(findRepoRoot().resolve("scripts/smoke.sh"));

        Assert.assertTrue(script.contains("READY_TIMEOUT_SEC=\"${READY_TIMEOUT_SEC:-30}\""));
        Assert.assertTrue(script.contains("local deadline_sec=\"$READY_TIMEOUT_SEC\""));
        Assert.assertTrue(script.contains("server 未在 ${READY_TIMEOUT_SEC}s 内就绪"));
        Assert.assertFalse(script.contains("server 未在 10s 内就绪"));
    }

    @Test
    public void smokeScriptHasOptInAllocatorSensitivePath() throws IOException {
        String script = Files.readString(findRepoRoot().resolve("scripts/smoke.sh"));

        Assert.assertTrue(script.contains("ALLOCATOR_SMOKE=\"${ALLOCATOR_SMOKE:-0}\""));
        Assert.assertTrue(script.contains("[[ \"$ALLOCATOR_SMOKE\" == \"1\" ]]"));
        Assert.assertTrue(script.contains("allocator-sensitive command path"));
        Assert.assertTrue(script.contains("APPEND smoke:native:string -tail"));
        Assert.assertTrue(script.contains("LPUSH smoke:native:list a"));
        Assert.assertTrue(script.contains("HSET smoke:native:hash f v"));
        Assert.assertTrue(script.contains("SADD smoke:native:set m"));
        Assert.assertTrue(script.contains("ZADD smoke:native:zset 1 m"));
        Assert.assertTrue(script.contains("DEL smoke:native:string smoke:native:list smoke:native:hash smoke:native:set smoke:native:zset"));
    }

    private static Path findRepoRoot() {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isRegularFile(current.resolve("scripts/smoke.sh"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new IllegalStateException("repository root not found");
    }
}
