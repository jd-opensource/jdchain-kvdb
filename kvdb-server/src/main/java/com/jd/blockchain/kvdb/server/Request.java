package com.jd.blockchain.kvdb.server;

import com.jd.blockchain.kvdb.protocol.Command;

public interface Request {

    String getId();

    Command getCommand();

    Session getSession();

    ServerContext getServerContext();

}
