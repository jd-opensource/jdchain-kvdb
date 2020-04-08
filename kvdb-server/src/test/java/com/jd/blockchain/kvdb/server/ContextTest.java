package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.config.ServerConfig;
import com.jd.blockchain.kvdb.server.executor.*;
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
    public void setUp() throws Exception {
        context = new DefaultServerContext(new ServerConfig(this.getClass().getResource("/").getFile()));
    }

    @After
    public void tearDown() {
        context.stop();
        FileUtils.deletePath(new File(context.getConfig().getKvdbConfig().getDbsRootdir()), true);
    }

    @Test
    public void test() {
        context.addExecutor(USE.getCommand(), new UseExecutor());
        context.addExecutor(CREATE_DATABASE.getCommand(), new CreateDatabaseExecutor());
        context.addExecutor(INFO.getCommand(), new InfoExecutor());
        context.addExecutor(EXISTS.getCommand(), new ExistsExecutor());
        context.addExecutor(GET.getCommand(), new GetExecutor());
        context.addExecutor(PUT.getCommand(), new PutExecutor());
        context.addExecutor(BATCH_BEGIN.getCommand(), new BatchBeginExecutor());
        context.addExecutor(BATCH_ABORT.getCommand(), new BatchAbortExecutor());
        context.addExecutor(BATCH_COMMIT.getCommand(), new BatchCommitExecutor());

        Assert.assertEquals(0, context.getClients());
        Assert.assertTrue(context.getExecutor(USE.getCommand()) instanceof UseExecutor);
        Assert.assertTrue(context.getExecutor(CREATE_DATABASE.getCommand()) instanceof CreateDatabaseExecutor);
        Assert.assertTrue(context.getExecutor(INFO.getCommand()) instanceof InfoExecutor);
        Assert.assertTrue(context.getExecutor(EXISTS.getCommand()) instanceof ExistsExecutor);
        Assert.assertTrue(context.getExecutor(GET.getCommand()) instanceof GetExecutor);
        Assert.assertTrue(context.getExecutor(PUT.getCommand()) instanceof PutExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_BEGIN.getCommand()) instanceof BatchBeginExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_ABORT.getCommand()) instanceof BatchAbortExecutor);
        Assert.assertTrue(context.getExecutor(BATCH_COMMIT.getCommand()) instanceof BatchCommitExecutor);

    }
}