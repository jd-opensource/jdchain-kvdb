package com.jd.blockchain.kvdb.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Overwriter implements Overwrite<byte[]> {

    private Path path;

    public Overwriter(Path path) throws IOException {
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        this.path = path;
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
