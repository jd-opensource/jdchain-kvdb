package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.server.wal.Entity;
import com.jd.blockchain.kvdb.server.wal.RedoLog;
import com.jd.blockchain.kvdb.server.wal.Wal;
import com.jd.blockchain.kvdb.server.wal.WalCommand;
import com.jd.blockchain.kvdb.server.wal.WalEntity;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

public class RedoLogTest {

    @Test
    public void testWrite() throws IOException {
        Wal wal = new RedoLog("");
        String id = UUID.randomUUID().toString();
        wal.append(WalEntity.newCreateDatabaseEntity(id));
        wal.flush();
        Assert.assertTrue(wal.exists(id));
        Entity entity = wal.get(id);
        Assert.assertTrue(WalCommand.CREATE_DATABASE.equals(entity.getCommand()));

        Assert.assertFalse(wal.exists(id + id));
    }

    @Test
    public void testRead() throws IOException {
        Wal wal = new RedoLog("");
        long position = 0;
        while (true) {
            long next = wal.next(position);
            if (next == -1) {
                break;
            }
            Entity e = wal.get(position, (int) (next - position - RedoLog.HEADER_SIZE * 2));
            System.out.println(e.id());
            System.out.println(e.getCommand().toString());
            position = next;
        }

    }

}
