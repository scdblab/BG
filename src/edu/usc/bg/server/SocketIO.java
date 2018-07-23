package edu.usc.bg.server;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Scanner;

import edu.usc.bg.base.Client;

/**
 * SocketIO manages socket operations performed by the CORE.
 * This includes the read/write actions performed by Data Input/Output Streams.
 * 
 * Take care to understand the usage of each method.
 * For example, writeBytes always writes the length of the message (4 byte int) first
 * and readBytes always reads in the length of the message (4 byte int) first.
 */
public class SocketIO {

	Socket socket;
//	public static int SOCKET_MAX_READ_TIME_SECONDS=30; //
//	public static boolean testing=true;
	private DataInputStream in = null;
	BufferedOutputStream out = null;
	ByteArrayOutputStream baos = null;
	
	public SocketIO(Socket sock) throws IOException {
		this.socket = sock;
		this.setDataInputStream(new DataInputStream(new BufferedInputStream(sock.getInputStream())));
		this.out = new BufferedOutputStream(sock.getOutputStream());
	}
	
	public void writeByte(byte val) throws IOException {
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		out.write((byte)val);
		out.flush();
	}
	
	public boolean isConnected() {
		return (socket != null && socket.isConnected());
	}
	public boolean isAlive() {
		if (!isConnected())
			return false;

		/*// try to talk to the server w/ a dumb query to ask its version
		try {
			byte b[] = new byte[KosarClient_Server.clientID.length+1];
			System.arraycopy(KosarClient_Server.clientID, 0, b, 1, KosarClient_Server.clientID.length);
			this.write(b);
		    this.flush();
			this.readInt();
		} catch (IOException ex) {
			return false;
		}*/

		return true;
	}
	
	
	public int getLocalPortNum(){
		return socket.getLocalPort();
	}
	
	
	protected static Socket getSocket( String host, int port, int timeout ) throws IOException {
        SocketChannel sock = SocketChannel.open();
        sock.socket().connect( new InetSocketAddress( host, port ), timeout );
        System.out.println("Connected to " + host + " : " + port);
        System.out.println("New Sock at " + sock.socket().getInetAddress() + " : " + sock.socket().getLocalPort());
        return sock.socket();
}

	public void writeBytes(byte[] val) throws IOException {
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		// trying to fix a bug: 1- comment all baos related statements 2- allocate 4+length instead of 4 bytes in bb 3- add bb.put(val) 4- add out.write(bb.array());;
		
//		baos.reset();
		//System.out.println("Byteeee " + val.length);
		ByteBuffer bb = ByteBuffer.allocate(4+val.length);
		bb.putInt(val.length);
		bb.put(val);
//		baos.write(bb.array());
//		baos.write(val);
//		baos.flush();
		out.write(bb.array());
//		out.write(baos.toByteArray());
		out.flush();
		
//		baos.reset();
	}
	
	public int writeInt(int returnVal) throws IOException{
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(returnVal);
		
		out.write(bb.array());
		out.flush();
		return 1;
		// This socket is shared, and should not be closed.
		// s.close();
	}
	

	public int writeLong(long returnVal) throws IOException{
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		ByteBuffer bb = ByteBuffer.allocate(8);
		bb.putLong(returnVal);
		
		out.write(bb.array());
		out.flush();
		return 1;
		// This socket is shared, and should not be closed.
		// s.close();
	}
	
	public byte readByte() throws IOException {
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		return getDataInputStream().readByte();
	}

	public static long checkReadBytesResponse(SocketIO soc, int maxSeconds)
	{
		 
		 boolean flag=true;
		 long EndTime = (System.currentTimeMillis())+ maxSeconds*1000;
		 long start= System.currentTimeMillis();
		 // this while is used for testing
		 try {
			while (soc.getDataInputStream().available()==0 && flag) 
			 {
				 if (System.currentTimeMillis()>EndTime)
				 {
					 flag=false;
					 System.out.println("Response not recieved will block ...");
				 }
			 }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error in check read response " + e.getMessage());
		}
		 return (System.currentTimeMillis()-start);

	}
	public byte[] readBytes() throws IOException {
		
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		byte[] input = null;
		int length = 0;
		int bytesRead = 0;
		try {
			
			length = readInt();
			input = new byte[length];
			if (getDataInputStream() != null && length > 0) {
				while (bytesRead < length) {
					input[bytesRead++] = getDataInputStream().readByte();
					
				}
			}
		} catch (EOFException eof) {
			System.out.println("EOF12");
		} catch (IndexOutOfBoundsException i) {
			System.out.println("Error: SocketIO - Out of bounds.");
			System.out.println("Length: "+length+" bytesRead: "+bytesRead);
			i.printStackTrace();
		}
		return input;
	}
	
	public int read(byte[] b) throws IOException {
		if (socket == null || !socket.isConnected()) {
			throw new IOException(
					"++++ attempting to read from closed socket");
		}
		
		int count = 0;
		while (count < b.length) {
			int cnt = getDataInputStream().read(b, count, (b.length - count));
			count += cnt;
		}
		return count;
	}
	public short readShort() throws IOException {
		if (socket == null || !socket.isConnected()) {
			throw new IOException(
					"++++ attempting to read from closed socket");
		}
		return getDataInputStream().readShort();
	}
	
	
	public long readLong() throws EOFException, IOException {
		if(socket == null || !socket.isConnected())
		{
			System.out.println("Null Sockettt");
			throw new IOException("Error: Attempting to read from closed socket");
		
		}
		try {
	
			 
			
			return getDataInputStream().readLong();

		} catch(EOFException eof) {
			System.out.println("read long End of File. " + eof.getMessage());
			
			//System.exit(0);
		}
		return 0;
	}
	public int readi() throws IOException{
		byte[] b = new byte[4] ;
		in.read(b, 0, 4);
		return ByteBuffer.wrap(b).getInt();
	}
	public int readInt() throws EOFException, IOException {
		if(socket == null || !socket.isConnected())
		{
			System.out.println("Null Sockettt");
			throw new IOException("Error: Attempting to read from closed socket");
		
		}
		try {
		//	System.out.println("Test before read int ");
	
			 
			
			return getDataInputStream().readInt();

		} catch(EOFException eof) {
			System.out.println("1End of File. " + eof.getMessage());
			
			//System.exit(0);
		}
		return 0;
	}

	public void closeAll() throws IOException {
		if(socket == null || !socket.isConnected())
			throw new IOException("Error: Attempting to read from closed socket");
		
		socket.close();
		out.close();
		getDataInputStream().close();
	}
	
	public Socket getSocket() {
		return socket;
	}
	
	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public BufferedOutputStream getOut() {
		return out;
	}
	
	public int sendValue(int i)
	{
		try
		{
		//ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(i);
		
		writeBytes(bb.array());
		bb.clear();
		return 1;
		}
		catch(Exception ex)
		{
			System.out.println("Error: Socket Send " + ex.getMessage());
			return -1;
		}
	}
	
//	public int sendLong(long i)
//	{
//		try
//		{
//		//ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        //ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
//		ByteBuffer bb = ByteBuffer.allocate(8);
//		bb.putLong(i);
//		
//		writeBytes(bb.array());
//		bb.clear();
//		return 1;
//		}
//		catch(Exception ex)
//		{
//			System.out.println("Error: Socket Send Long " + ex.getMessage());
//			return -1;
//		}
//	}
	
	public void trueClose() throws IOException {
        trueClose( true );
}

/**
 * closes socket and all streams connected to it
 *
 * @throws IOException if fails to close streams or socket
 */
public void trueClose(boolean addToDeadPool) throws IOException {

	boolean err = false;
	StringBuilder errMsg = new StringBuilder();

	if (getDataInputStream() != null) {
		try {
			getDataInputStream().close();
		} catch (IOException ioe) {
			errMsg.append("++++ error closing input stream for socket: "
					+ toString() + " for host: " + "\n");
			errMsg.append(ioe.getMessage());
			err = true;
		}
	}

	if (out != null) {
		try {
			out.close();
		} catch (IOException ioe) {
			errMsg.append("++++ error closing output stream for socket: "
					+ toString() + " for host: " + "\n");
			errMsg.append(ioe.getMessage());
			err = true;
		}
	}

	if (socket != null) {
		try {
			socket.close();
		} catch (IOException ioe) {
			errMsg.append("++++ error closing socket: " + toString()
					+ " for host: " + "\n");
			errMsg.append(ioe.getMessage());
			err = true;
		}
	}

	
}

public DataInputStream getDataInputStream() {
	return in;
}

public void setDataInputStream(DataInputStream in) {
	this.in = in;
}	
}
