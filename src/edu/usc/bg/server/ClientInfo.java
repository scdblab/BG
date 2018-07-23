package edu.usc.bg.server;

public class ClientInfo {
	private String IP;
	int ClientId;
	private int Port;
	public ClientInfo( String ip, int p, int id)
	{
		setIP(ip);
		setPort(p);
		ClientId=id;
		
	}
	public String getIP() {
		return IP;
	}
	public void setIP(String iP) {
		IP = iP;
	}
	public int getPort() {
		return Port;
	}
	public void setPort(int port) {
		Port = port;
	}

}
