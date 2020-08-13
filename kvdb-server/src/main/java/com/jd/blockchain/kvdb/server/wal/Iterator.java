package com.jd.blockchain.kvdb.server.wal;

import java.io.IOException;

public interface Iterator {

    boolean hasNext() throws IOException;

    Entity next() throws IOException;

}
