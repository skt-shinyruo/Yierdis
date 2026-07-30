package yier.bubu.redis.command.kernel;

import yier.bubu.redis.command.api.CommandModule;
import yier.bubu.redis.command.api.CommandSpec;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class CommandRegistry implements CommandModule.Registration {
    private final LinkedHashMap<String, CommandSpec> mutableSpecs = new LinkedHashMap<>();
    private Map<String, CommandSpec> sealedSpecs;

    @Override
    public void register(CommandSpec spec) {
        ensureMutable();
        CommandSpec registered = Objects.requireNonNull(spec, "spec");
        String nameUpper = registered.syntax().nameUpper();
        if (mutableSpecs.putIfAbsent(nameUpper, registered) != null) {
            throw new IllegalArgumentException("duplicate command registration: " + nameUpper);
        }
    }

    public void seal() {
        if (sealedSpecs == null) {
            sealedSpecs = Map.copyOf(mutableSpecs);
        }
    }

    @Override
    public int commandCount() {
        return specs().size();
    }

    @Override
    public boolean containsUpperName(String nameUpper) {
        String normalized = normalizeMetadataName(nameUpper);
        return normalized != null && specs().containsKey(normalized);
    }

    @Override
    public CommandSpec specByUpperName(String nameUpper) {
        String normalized = normalizeMetadataName(nameUpper);
        return normalized == null ? null : specs().get(normalized);
    }

    CommandSpec specByExactUpperName(String nameUpper) {
        return nameUpper == null ? null : specs().get(nameUpper);
    }

    @Override
    public String[] upperNamesSorted() {
        String[] names = specs().keySet().toArray(new String[0]);
        Arrays.sort(names);
        return names;
    }

    private Map<String, CommandSpec> specs() {
        if (sealedSpecs == null) {
            throw new IllegalStateException("command registry must be sealed before lookup");
        }
        return sealedSpecs;
    }

    private void ensureMutable() {
        if (sealedSpecs != null) {
            throw new IllegalStateException("command registry is sealed");
        }
    }

    private static String normalizeMetadataName(String nameUpper) {
        if (nameUpper == null || nameUpper.isBlank()) {
            return null;
        }
        String normalized = nameUpper.trim().toUpperCase(Locale.ROOT);
        return normalized.isEmpty() ? null : normalized;
    }
}
