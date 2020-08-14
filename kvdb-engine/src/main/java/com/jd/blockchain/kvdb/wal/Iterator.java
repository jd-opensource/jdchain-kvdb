package com.jd.blockchain.kvdb.wal;

import com.jd.blockchain.kvdb.proto.Entity;

import java.io.IOException;

public interface Iterator {

    boolean hasNext() throws IOException;

    Entity next() throws IOException;

}
