package com.jd.blockchain.kvdb.protocol.exception;

public class KVDBException extends Exception {

    private String message;

    public KVDBException(Exception e) {
        super(e);
    }

    public KVDBException(String message) {
        this.message = message;
    }

    @Override
    public String getMessage() {
        return message;
    }
}
