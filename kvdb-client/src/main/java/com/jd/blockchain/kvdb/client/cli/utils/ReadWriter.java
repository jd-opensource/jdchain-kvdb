package com.jd.blockchain.kvdb.client.cli.utils;

public interface ReadWriter {

    String readLine();

    void writePrefix(String host, int port);

    void write(Object out);

    void writeLine(Object out);

}
