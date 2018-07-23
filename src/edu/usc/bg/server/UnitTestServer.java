package edu.usc.bg.server;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


public class UnitTestServer extends Thread {
	public static int port=53122;
public void run()
{
	ServerSocket server=null;
	try {
		
		server = new ServerSocket(port);
		UnitTest.serverSemaphore.release();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		
		System.out.println("Server not starting");
		e.printStackTrace();
	}
	Socket s=null;
	for (int i=0; i<UnitTest.NumSockets;i++)
	{
		try {
			s=server.accept();
			
		} catch (IOException e) {
			System.out.println("Error with accept");
			e.printStackTrace();
		}
		
	}
	
	
}
}
