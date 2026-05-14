package yier.bubu.redis.memory.foreign;

public enum YierdisNativeSizeClass {
    B16(16),
    B24(24),
    B32(32),
    B48(48),
    B64(64),
    B96(96),
    B128(128),
    B192(192),
    B256(256),
    B384(384),
    B512(512),
    B768(768),
    B1024(1024),
    B1536(1536),
    B2048(2048),
    B3072(3072),
    B4096(4096),
    B6144(6144),
    B8192(8192),
    B12288(12288),
    B16384(16384),
    B24576(24576),
    B32768(32768);

    public static final int MAX_SMALL_BYTES = B32768.bytes;

    private final int bytes;

    YierdisNativeSizeClass(int bytes) {
        this.bytes = bytes;
    }

    public int bytes() {
        return bytes;
    }

    public boolean supports(int requestedBytes) {
        return requestedBytes > 0 && requestedBytes <= bytes;
    }

    public static YierdisNativeSizeClass forSize(int requestedBytes) {
        if (requestedBytes <= 0 || requestedBytes > MAX_SMALL_BYTES) {
            throw new IllegalArgumentException("not a small size: " + requestedBytes);
        }
        for (YierdisNativeSizeClass sizeClass : values()) {
            if (sizeClass.supports(requestedBytes)) {
                return sizeClass;
            }
        }
        throw new IllegalArgumentException("not a small size: " + requestedBytes);
    }
}
