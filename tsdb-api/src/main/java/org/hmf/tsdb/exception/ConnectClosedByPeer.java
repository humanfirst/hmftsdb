package org.hmf.tsdb.exception;

public class ConnectClosedByPeer extends Exception{

	private static final long serialVersionUID = 1L;

	public ConnectClosedByPeer(String msg) {
		super(msg);
	}
}
