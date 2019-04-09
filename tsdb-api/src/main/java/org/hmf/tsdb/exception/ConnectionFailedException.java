package org.hmf.tsdb.exception;

public class ConnectionFailedException extends Exception {
	private static final long serialVersionUID = 1L;

	public ConnectionFailedException(String msg) {
		super(msg);
	}
}
