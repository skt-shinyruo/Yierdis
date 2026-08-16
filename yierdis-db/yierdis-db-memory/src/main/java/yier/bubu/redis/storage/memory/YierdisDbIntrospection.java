package yier.bubu.redis.storage.memory;

import yier.bubu.redis.bytes.BytesView;
import java.util.Objects;

final class YierdisDbIntrospection {
    private final YierdisDbKernel kernel;

    YierdisDbIntrospection(YierdisDbKernel kernel) {
        this.kernel = Objects.requireNonNull(kernel, "kernel");
    }

    String objectEncoding(BytesView keyView) {
        return kernel.inspect(scope -> scope.objectEncoding(keyView));
    }

    String objectEncoding(byte[] keyBytes) {
        return kernel.inspect(scope -> scope.objectEncoding(keyBytes));
    }
}
