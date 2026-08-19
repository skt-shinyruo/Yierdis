package yier.bubu.redis.app.server;

import yier.bubu.redis.protocol.resp.netty.RespDecodedMessage;

import java.util.Objects;

/**
 * RESP 解码结果与其已注册回复槽位的所有权绑定。
 */
final class RegisteredRespMessage implements AutoCloseable {
    private final RespDecodedMessage message;
    private final ReplySlot slot;
    private boolean closed;
    private boolean messageTransferred;

    RegisteredRespMessage(RespDecodedMessage message, ReplySlot slot) {
        this.message = Objects.requireNonNull(message, "message");
        this.slot = Objects.requireNonNull(slot, "slot");
    }

    RespDecodedMessage message() {
        return message;
    }

    ReplySlot slot() {
        return slot;
    }

    synchronized RespDecodedMessage takeMessage() {
        if (closed || messageTransferred) {
            throw new IllegalStateException("registered RESP message is no longer available");
        }
        messageTransferred = true;
        return message;
    }

    @Override
    public void close() {
        boolean closeMessage;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            closeMessage = !messageTransferred;
        }
        try {
            if (closeMessage) {
                message.close();
            }
        } catch (Throwable ignored) {
            // 请求清理失败时仍需取消 reply slot，避免保留额度。
        } finally {
            slot.cancel();
        }
    }
}
