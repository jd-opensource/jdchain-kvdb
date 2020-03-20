package com.jd.blockchain.kvdb.client.cli.utils;

import com.jd.blockchain.utils.io.RuntimeIOException;

import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStreamReader;

public class ConsoleUtils {

    public static ReadWriter getReadWriter() {
        Console cs = System.console();
        if (cs == null) {
            return new SystemInputReader();
        } else {
            return new SystemConsoleReader(cs);
        }
    }

    private static class SystemConsoleReader implements ReadWriter {

        private Console cs;

        public SystemConsoleReader(Console cs) {
            this.cs = cs;
        }

        @Override
        public String readLine() {
            return cs.readLine(">");
        }

        @Override
        public void writePrefix(String host, int port) {
            cs.writer().print(host + ":" + port);
        }

        @Override
        public void write(Object out) {
            cs.writer().print(out);
        }

        @Override
        public void writeLine(Object out) {
            cs.writer().println(out);
        }

    }

    private static class SystemInputReader implements ReadWriter {

        private BufferedReader bufReader = new BufferedReader(new InputStreamReader(System.in));

        @Override
        public String readLine() {
            try {
                return bufReader.readLine();
            } catch (IOException e) {
                throw new RuntimeIOException(e.getMessage(), e);
            }
        }

        @Override
        public void writePrefix(String host, int port) {
            System.out.print(host + ":" + port + "> ");
        }

        @Override
        public void write(Object out) {
            System.out.print(out);
        }

        @Override
        public void writeLine(Object out) {
            System.out.println(out);
        }

    }

}
