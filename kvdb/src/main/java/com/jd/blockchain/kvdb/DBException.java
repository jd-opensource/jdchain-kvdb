package com.jd.blockchain.kvdb;

public class DBException extends RuntimeException {

	private static final long serialVersionUID = -4472465847454195374L;

	public DBException() {
	}

	public DBException(String message) {
		super(message);
	}

	public DBException(String message, Throwable cause) {
		super(message, cause);
	}

}
