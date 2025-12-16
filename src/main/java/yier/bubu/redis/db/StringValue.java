package yier.bubu.redis.db;

final class StringValue implements YierdisValue {
    private String value;

    StringValue(String value) {
        this.value = value;
    }

    String get() {
        return value;
    }

    void set(String value) {
        this.value = value;
    }

    @Override
    public ValueType type() {
        return ValueType.STRING;
    }
}
