package yier.bubu.redis.app.server;

import io.netty.util.ReferenceCountUtil;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RESP 解码结果与其已注册回复槽位的所有权绑定。
 */
final class RegisteredRespMessage implements AutoCloseable {
    private final Object message;
    private final ReplySlot slot;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean messageTransferred = new AtomicBoolean();

    RegisteredRespMessage(Object message, ReplySlot slot) {
        this.message = Objects.requireNonNull(message, "message");
        this.slot = Objects.requireNonNull(slot, "slot");
    }

    Object message() {
        return message;
    }

    ReplySlot slot() {
        return slot;
    }

    Object takeMessage() {
        if (!messageTransferred.compareAndSet(false, true)) {
            throw new IllegalStateException("registered RESP message was already transferred");
        }
        return message;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (!messageTransferred.get()) {
            closeMessage(message);
        }
        slot.cancel();
    }

    private static void closeMessage(Object message) {
        if (message instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception ignored) {
                // 释放解码输入失败时仍需继续取消槽位额度。
            }
            return;
        }
        ReferenceCountUtil.release(message);
    }
}
