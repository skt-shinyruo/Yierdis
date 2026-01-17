package yier.bubu.redis.protocol;

public enum RespType {
    SIMPLE_STRING,
    ERROR,
    INTEGER,
    BULK_STRING,
    ARRAY,
    MAP,
    NULL
}
