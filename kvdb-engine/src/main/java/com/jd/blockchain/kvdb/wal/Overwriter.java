package com.jd.blockchain.kvdb.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Overwriter implements Overwrite<byte[]> {

    private Path path;

    public Overwriter(String path) {
        this.path = Paths.get(path);
    }

    @Override
    public byte[] read() throws IOException {
        return Files.readAllBytes(path);
    }

    @Override
    public void write(byte[] data) throws IOException {
        Files.write(path, data);
    }
}
