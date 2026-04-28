package yier.bubu.redis.command;

import java.util.Arrays;

final class CommandArity {
    private enum Kind {
        EXACT,
        MIN,
        RANGE,
        ONE_OF,
        PAIR_TAIL
    }

    private final Kind kind;
    private final String commandLower;
    private final int first;
    private final int second;
    private final int[] allowed;

    private CommandArity(Kind kind, String commandLower, int first, int second, int[] allowed) {
        this.kind = kind;
        this.commandLower = commandLower;
        this.first = first;
        this.second = second;
        this.allowed = allowed;
    }

    static CommandArity exact(int argc, String commandLower) {
        return new CommandArity(Kind.EXACT, commandLower, argc, 0, null);
    }

    static CommandArity min(int minArgc, String commandLower) {
        return new CommandArity(Kind.MIN, commandLower, minArgc, 0, null);
    }

    static CommandArity range(int minArgc, int maxArgc, String commandLower) {
        return new CommandArity(Kind.RANGE, commandLower, minArgc, maxArgc, null);
    }

    static CommandArity oneOf(String commandLower, int... allowedArgc) {
        if (allowedArgc == null || allowedArgc.length == 0) {
            throw new IllegalArgumentException("allowedArgc must not be empty");
        }
        return new CommandArity(Kind.ONE_OF, commandLower, 0, 0, Arrays.copyOf(allowedArgc, allowedArgc.length));
    }

    static CommandArity pairTail(int minArgc, int tailStartIndex, String commandLower) {
        return new CommandArity(Kind.PAIR_TAIL, commandLower, minArgc, tailStartIndex, null);
    }

    CommandParseError validate(ArgReader args) {
        int argc = args.argc();
        boolean ok = switch (kind) {
            case EXACT -> argc == first;
            case MIN -> argc >= first;
            case RANGE -> argc >= first && argc <= second;
            case ONE_OF -> contains(argc);
            case PAIR_TAIL -> argc >= first && ((argc - second) & 1) == 0;
        };
        return ok ? null : CommandParseError.wrongArity(commandLower);
    }

    private boolean contains(int argc) {
        for (int value : allowed) {
            if (value == argc) {
                return true;
            }
        }
        return false;
    }
}
