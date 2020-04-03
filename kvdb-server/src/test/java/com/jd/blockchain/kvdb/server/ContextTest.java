package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.handler.*;
import com.jd.blockchain.utils.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.jd.blockchain.kvdb.protocol.Command.CommandType.*;

public class ContextTest {

    private DefaultServerContext context;

    @Before
    public void setUp() {
        context = new DefaultServerContext();
    }

    @After
    public void tearDown() {
        context.stop();
        FileUtils.deletePath(new File(context.getConfig().getDbPath()), true);
    }

    @Test
    public void test() {
        context.addExecutor(SELECT.getCommand(), new SelectExecutor());
        context.addExecutor(EXISTS.getCommand(), new ExistsExecutor());
        context.addExecutor(GET.getCommand(), new GetExecutor());
        context.addExecutor(PUT.getCommand(), new PutExecutor());
        context.addExecutor(BATCH_BEGIN.getCommand(), new BatchBeginExecutor());
        context.addExecutor(BATCH_ABORT.getCommand(), new BatchAbortExecutor());
        context.addExecutor(BATCH_COMMIT.getCommand(), new BatchCommitExecutor());

        Assert.assertEquals(4, context.dbSize());
        Assert.assertEquals(0, context.getClients());
        Assert.assertTrue(context.getExecutor(SELECT.getCommand()) instanceof SelectExecutor);
        Assert.assertTrue(context.getExecutor(EXISTS.getCommand()) instanceof ExistsExecutor);
        Assert.assertTrue(context.getExecutor(GET.getCommand()) instanceof GetExecutor);
        Assert.assertTrue(context.getExecutor(PUT.getCommand()) instanceof PutExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_BEGIN.getCommand()) instanceof BatchBeginExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_ABORT.getCommand()) instanceof BatchAbortExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_COMMIT.getCommand()) instanceof BatchCommitExecutor);

    }
}
