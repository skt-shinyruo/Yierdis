package yier.bubu.redis.command.api;

import java.util.Arrays;

public final class CommandArity {
    private enum Kind { EXACT, MIN, RANGE, ONE_OF, PAIR_TAIL }

    private final Kind kind;
    private final int first;
    private final int second;
    private final int[] allowed;

    private CommandArity(Kind kind, int first, int second, int[] allowed) {
        this.kind = kind;
        this.first = first;
        this.second = second;
        this.allowed = allowed;
    }

    public static CommandArity exact(int argc) {
        requirePositive(argc, "argc");
        return new CommandArity(Kind.EXACT, argc, 0, null);
    }

    public static CommandArity min(int minArgc) {
        requirePositive(minArgc, "minArgc");
        return new CommandArity(Kind.MIN, minArgc, 0, null);
    }

    public static CommandArity range(int minArgc, int maxArgc) {
        requirePositive(minArgc, "minArgc");
        if (maxArgc < minArgc) {
            throw new IllegalArgumentException("maxArgc must be >= minArgc");
        }
        return new CommandArity(Kind.RANGE, minArgc, maxArgc, null);
    }

    public static CommandArity oneOf(int... allowedArgc) {
        if (allowedArgc == null || allowedArgc.length == 0) {
            throw new IllegalArgumentException("allowedArgc must not be empty");
        }
        int[] copy = Arrays.copyOf(allowedArgc, allowedArgc.length);
        Arrays.sort(copy);
        requirePositive(copy[0], "allowedArgc");
        for (int i = 1; i < copy.length; i++) {
            requirePositive(copy[i], "allowedArgc");
            if (copy[i] == copy[i - 1]) {
                throw new IllegalArgumentException("allowedArgc must not contain duplicates");
            }
        }
        return new CommandArity(Kind.ONE_OF, copy[0], 0, copy);
    }

    public static CommandArity pairTail(int minArgc, int tailStartIndex) {
        requirePositive(minArgc, "minArgc");
        if (tailStartIndex < 0 || tailStartIndex > minArgc) {
            throw new IllegalArgumentException("tailStartIndex is out of range");
        }
        return new CommandArity(Kind.PAIR_TAIL, minArgc, tailStartIndex, null);
    }

    public void validate(String commandLower, CommandArgs args) throws CommandParseException {
        if (!accepts(args.argc())) {
            throw new CommandParseException(
                    "ERR wrong number of arguments for '" + commandLower + "' command"
            );
        }
    }

    private boolean accepts(int argc) {
        return switch (kind) {
            case EXACT -> argc == first;
            case MIN -> argc >= first;
            case RANGE -> argc >= first && argc <= second;
            case ONE_OF -> Arrays.binarySearch(allowed, argc) >= 0;
            case PAIR_TAIL -> argc >= first && ((argc - second) & 1) == 0;
        };
    }

    public int redisMetadataArity() {
        return kind == Kind.EXACT ? first : -first;
    }

    private static void requirePositive(int value, String name) {
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }
}
