package yier.bubu.redis.app.bench.storage;

import org.junit.Assert;
import org.junit.Test;

public class ProcessRssReaderTest {
    @Test
    public void parsesLinuxVmRssInKibibytes() {
        Assert.assertEquals(
                1_263_616L,
                ProcessRssReader.parseStatus("Name:\tjava\nVmRSS:\t1234 kB\n").orElseThrow()
        );
    }

    @Test
    public void unavailableOrMalformedRssIsNotFabricated() {
        Assert.assertTrue(ProcessRssReader.parseStatus("Name:\tjava\n").isEmpty());
        Assert.assertTrue(ProcessRssReader.parseStatus("VmRSS:\tnot-a-number kB\n").isEmpty());
        Assert.assertTrue(ProcessRssReader.parseStatus("VmRSS:\t12 MB\n").isEmpty());
        Assert.assertTrue(ProcessRssReader.parseStatus(null).isEmpty());
    }
}
