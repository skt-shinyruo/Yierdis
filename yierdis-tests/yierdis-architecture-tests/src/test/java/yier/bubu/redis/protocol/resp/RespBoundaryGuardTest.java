package yier.bubu.redis.protocol.resp;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class RespBoundaryGuardTest {
    @Test
    public void productionSourcesMustNotReferenceRetiredWireProtocol() throws IOException {
        Path root = resolveRepoRoot();
        try (Stream<Path> files = Files.walk(root)) {
            Path offender = files
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().contains("/src/main/java/") || p.getFileName().toString().equals("pom.xml"))
                    .filter(p -> !p.toString().contains("/target/"))
                    .filter(p -> containsAny(p, retiredNeedles()))
                    .findFirst()
                    .orElse(null);
            Assert.assertNull("retired protocol reference remains: " + offender, offender);
        }
    }

    private static Path resolveRepoRoot() {
        Path current = Path.of("").toAbsolutePath().normalize();
        while (current != null) {
            if (Files.isRegularFile(current.resolve("pom.xml"))
                    && Files.isDirectory(current.resolve("yierdis-networking"))
                    && Files.isDirectory(current.resolve("yierdis-tests"))) {
                return current;
            }
            current = current.getParent();
        }
        throw new AssertionError("unable to locate repository root");
    }

    private static String[] retiredNeedles() {
        return new String[]{
                "protocol." + "custom." + "v1",
                "Custom" + "ProtocolV1",
                "JsonLine" + "ReplyWriter",
                "yierdis-networking-" + "custom-v1",
                "yierdis-networking-" + "custom-v1-execution"
        };
    }

    private static boolean containsAny(Path path, String[] needles) {
        for (String needle : needles) {
            if (contains(path, needle)) {
                return true;
            }
        }
        return false;
    }

    private static boolean contains(Path path, String needle) {
        try {
            return Files.readString(path, StandardCharsets.UTF_8).contains(needle);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
