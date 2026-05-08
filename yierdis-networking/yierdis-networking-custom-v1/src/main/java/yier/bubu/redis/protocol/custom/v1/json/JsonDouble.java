package yier.bubu.redis.protocol.custom.v1.json;

public record JsonDouble(double value) implements JsonNumber {
    public JsonDouble {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            throw new IllegalArgumentException("value must be finite");
        }
    }
}
