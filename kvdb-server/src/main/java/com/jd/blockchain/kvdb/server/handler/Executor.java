package com.jd.blockchain.kvdb.server.handler;

import com.jd.blockchain.kvdb.protocol.Message;
import com.jd.blockchain.kvdb.server.Request;

public interface Executor {

    Message execute(Request request);

}
