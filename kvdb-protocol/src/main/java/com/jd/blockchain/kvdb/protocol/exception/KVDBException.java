package com.jd.blockchain.kvdb.protocol.exception;

public class KVDBException extends Exception {

    public KVDBException(Exception e) {
        super(e);
    }

    public KVDBException(String message) {
        super(message);
    }

}
