package edu.usc.bg.server;


public class TokenObject {
	byte[] token;
	SocketIO socket;
	
	//	boolean cache;

	public TokenObject(SocketIO socket, byte[] token) {
		this.token = token;
		this.socket = socket;
		
	}
	
	public TokenObject( byte[] token) {
		// used for testing purposes
		this.token = token;
		this.socket = null;
		
	
	}

	public byte[] getRequestArray() {
		return token;
	}

	public void setToken(byte[] token) {
		this.token = token;
	}

	public SocketIO getSocket() {
		return socket;
	}

	public void setSocket(SocketIO socket) {
		this.socket = socket;
	}
	//public boolean isCache() {
	//return cache;
	//}

}
