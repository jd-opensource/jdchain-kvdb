package com.jd.blockchain.kvdb.engine.wal;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;

public class OverwriteTest {

    @Test
    public void test() throws IOException {
        CheckpointWriter writer = new CheckpointWriter(Files.createTempFile("wal", "meta"));
        Assert.assertEquals(-1l, writer.read().longValue());

        writer.write(0l);
        Assert.assertEquals(0l, writer.read().longValue());

        writer.write(1l);
        Assert.assertEquals(1l, writer.read().longValue());

        writer.write(-1l);
        Assert.assertEquals(-1l, writer.read().longValue());

        writer.write(Long.MAX_VALUE);
        Assert.assertEquals(Long.MAX_VALUE, writer.read().longValue());

        writer.write(1 - Long.MAX_VALUE);
        Assert.assertEquals(1 - Long.MAX_VALUE, writer.read().longValue());
    }

}
