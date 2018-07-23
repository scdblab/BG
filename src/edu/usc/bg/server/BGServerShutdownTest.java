package edu.usc.bg.server;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

public class BGServerShutdownTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String s="10.0.1.25";
		String tokens[]=s.split(",");
		for (String ip: tokens)
		{
		new KillT(ip).start();
		}
	}

} 

class KillT extends Thread
{
	KillT(String i){
		ip=i;
	}
	String ip;
	public void run(){
		try {
			SocketIO socket = new SocketIO (new Socket(ip,55269 ));
			socket.sendValue(RequestHandler.THREAD_DUMP_REQUEST);
		//	
			
			socket.closeAll();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}
