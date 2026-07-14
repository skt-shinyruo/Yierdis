package yier.bubu.redis.common.command;

/**
 * 命令 argv 的只读借用视图。
 *
 * <p>该接口不暴露所有权操作；调用者只能在提供方声明的有效期内读取字节。</p>
 */
public interface CommandRecordView {
    int argc();

    boolean isNull(int index);

    int len(int index);

    byte byteAt(int index, int offset);

    void copyToByteArray(int index, byte[] dst, int dstOff);

    default byte[] toByteArray(int index) {
        if (isNull(index)) {
            return null;
        }
        int length = len(index);
        if (length < 0) {
            throw new IllegalStateException("non-null command argument has a negative length");
        }
        byte[] copy = new byte[length];
        copyToByteArray(index, copy, 0);
        return copy;
    }

    long retainedMemoryBytes();
}
