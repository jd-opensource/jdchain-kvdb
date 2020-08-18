package com.jd.blockchain.kvdb.engine.wal;

import com.jd.blockchain.kvdb.engine.proto.Entity;

import java.io.IOException;

public interface Iterator {

    boolean hasNext() throws IOException;

    Entity next() throws IOException;

}
