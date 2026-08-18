package yier.bubu.redis.storage.api;

import java.util.List;
import yier.bubu.redis.storage.api.result.ByteSequenceSource;
import yier.bubu.redis.storage.api.result.PoppedValueSequence;

public interface ListOps {
    ByteSequenceSource lrange(byte[] keyBytes, int start, int stop);

    WriteResult<Long> lpush(byte[] keyBytes, List<byte[]> values);

    WriteResult<Long> rpush(byte[] keyBytes, List<byte[]> values);

    PreparedMutation<PoppedValueSequence> preparePop(byte[] keyBytes, int count, boolean left);
}
