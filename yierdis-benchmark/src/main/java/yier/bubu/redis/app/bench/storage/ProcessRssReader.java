package yier.bubu.redis.app.bench.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalLong;

final class ProcessRssReader {
    private static final Path PROC_STATUS = Path.of("/proc/self/status");

    private ProcessRssReader() {
    }

    static OptionalLong currentBytes() {
        try {
            return parseStatus(Files.readString(PROC_STATUS, StandardCharsets.UTF_8));
        } catch (IOException | SecurityException ignored) {
            return OptionalLong.empty();
        }
    }

    static OptionalLong parseStatus(String status) {
        if (status == null) {
            return OptionalLong.empty();
        }
        for (String line : status.split("\\R")) {
            if (!line.startsWith("VmRSS:")) {
                continue;
            }
            String[] fields = line.trim().split("\\s+");
            if (fields.length < 3 || !"kB".equals(fields[2])) {
                return OptionalLong.empty();
            }
            try {
                long kibibytes = Long.parseLong(fields[1]);
                if (kibibytes < 0L) {
                    return OptionalLong.empty();
                }
                return OptionalLong.of(Math.multiplyExact(kibibytes, 1024L));
            } catch (ArithmeticException | NumberFormatException ignored) {
                return OptionalLong.empty();
            }
        }
        return OptionalLong.empty();
    }
}
