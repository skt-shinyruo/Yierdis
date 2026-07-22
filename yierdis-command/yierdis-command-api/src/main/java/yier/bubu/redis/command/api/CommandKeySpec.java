package yier.bubu.redis.command.api;

public record CommandKeySpec(int firstKeyIndex, int lastKeyIndex, int keyStep) {
    public static final CommandKeySpec NONE = new CommandKeySpec(0, 0, 0);

    public CommandKeySpec {
        if (firstKeyIndex < 0 || lastKeyIndex < -1 || keyStep < 0) {
            throw new IllegalArgumentException("invalid key index or step");
        }
        if (firstKeyIndex == 0 && (lastKeyIndex != 0 || keyStep != 0)) {
            throw new IllegalArgumentException("keyless commands must use 0, 0, 0");
        }
        if (firstKeyIndex > 0 && keyStep == 0) {
            throw new IllegalArgumentException("keyStep must be positive for keyed commands");
        }
        if (lastKeyIndex == -1 && firstKeyIndex == 0) {
            throw new IllegalArgumentException("variable-tail keys require firstKeyIndex > 0");
        }
        if (lastKeyIndex != -1 && firstKeyIndex > lastKeyIndex) {
            throw new IllegalArgumentException("lastKeyIndex must be -1 or >= firstKeyIndex");
        }
    }
}
