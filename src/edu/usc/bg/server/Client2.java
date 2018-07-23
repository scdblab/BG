package edu.usc.bg.server;
import java.io.ByteArrayOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;


public class Client2 {}

	/**
	 * @param args
	 */
/*
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		 try {
				
			         
			            Socket socket1 = new Socket("localhost", 53138);
			            SocketIO socket=new SocketIO(socket1);
			            ByteArrayOutputStream baos = new ByteArrayOutputStream();
			            ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
			    		ByteBuffer bb = ByteBuffer.allocate(24);
			    	
			    		bb.putInt(4);
			    		bb.putInt(4,2);
			    		bb.putInt(8,0);
			    		bb.putInt(12,1);
			    		bb.putInt(16,4);
			    		bb.putInt(20,5);
			    		
			    		
			    		baos.write(bb.array());

			    		bb.clear();
			    		//System.out.println("Client Send 01");

			    		//Determine what client ip's to send to the receiving client.

			    		baos.flush();
			    		socket.writeBytes(baos.toByteArray());
			    		
			    		socket.readByte();
			    		
			    		/*
			    		byte request[] = socket.readBytes();
			    		request = Arrays.copyOfRange(request, 0, request.length);
			    		int b=ByteBuffer.wrap(Arrays.copyOfRange(request, 0, 4)).getInt();
			    		System.out.println("Client Recived from Server" + b);
			           */
/*
			               
			              
			        } 
			        catch (Exception e)
			        {
			            System.out.println("Don't know " + e.getMessage());
			            System.exit(1);
			        }

	}

}
*/