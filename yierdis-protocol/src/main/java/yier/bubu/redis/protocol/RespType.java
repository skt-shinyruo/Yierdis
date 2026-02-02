package yier.bubu.redis.protocol;

public enum RespType {
    SIMPLE_STRING,
    ERROR,
    INTEGER,
    BOOLEAN,
    DOUBLE,
    BIG_NUMBER,
    BULK_STRING,
    VERBATIM_STRING,
    BLOB_ERROR,
    ARRAY,
    MAP,
    SET,
    PUSH,
    ATTRIBUTE,
    NULL
}
