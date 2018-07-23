package kosar;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SocketIO manages socket operations performed by the CORE. This includes the
 * read/write actions performed by Data Input/Output Streams.
 * 
 * Take care to understand the usage of each method. For example, writeBytes
 * always writes the length of the message (4 byte int) first and readBytes
 * always reads in the length of the message (4 byte int) first.
 */
public class SocketIO {

	Socket socket;
	// public static int SOCKET_MAX_READ_TIME_SECONDS=30; //
	// public static boolean testing=true;
	private DataInputStream in = null;
	DataOutputStream out = null;
	boolean inUse = false;

	public SocketIO(Socket sock) throws IOException {
		this.socket = sock;
		this.setDataInputStream(new DataInputStream(new BufferedInputStream(
				sock.getInputStream())));
		this.out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream()));
	}

	public void writeByte(byte val) throws IOException {
		if (socket == null || !socket.isConnected())
			throw new IOException(
					"Error: Attempting to read from closed socket");

		out.write((byte) val);
		out.flush();
	}

	public boolean isConnected() {
		return (socket != null && socket.isConnected());
	}

	public boolean isAlive() {
		if (!isConnected())
			return false;

		/*
		 * // try to talk to the server w/ a dumb query to ask its version try {
		 * byte b[] = new byte[KosarClient_Server.clientID.length+1];
		 * System.arraycopy(KosarClient_Server.clientID, 0, b, 1,
		 * KosarClient_Server.clientID.length); this.write(b); this.flush();
		 * this.readInt(); } catch (IOException ex) { return false; }
		 */

		return true;
	}

	public int getLocalPortNum() {
		return socket.getLocalPort();
	}

	protected static Socket getSocket(String host, int port, int timeout)
			throws IOException {
		SocketChannel sock = SocketChannel.open();
		sock.socket().connect(new InetSocketAddress(host, port), timeout);
		System.out.println("Connected to " + host + " : " + port);
		System.out.println("New Sock at " + sock.socket().getInetAddress()
				+ " : " + sock.socket().getLocalPort());
		return sock.socket();
	}

	private static final AtomicLong writeCount = new AtomicLong(0);
	
	private static BufferedWriter bw = null;
	
	private static final AtomicLong readCount = new AtomicLong(0);
	
	private static BufferedWriter rw;
	
	public static void initialize() {
		File file = new File("C:\\Users\\haoyu\\Documents\\KOSAR-Core\\OldBG\\log\\writeCount.txt");
		File file2 = new File("C:\\Users\\haoyu\\Documents\\KOSAR-Core\\OldBG\\log\\readCount.txt");
		
		try {
			bw = new BufferedWriter(new FileWriter(file));
			rw = new BufferedWriter(new FileWriter(file2));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	public void writeBytes(byte[] val) throws IOException {
		if (socket == null || !socket.isConnected())
			throw new IOException(
					"Error: Attempting to read from closed socket");
		if (bw == null) {
			
		}
		ByteBuffer bb = ByteBuffer.allocate(4 + val.length);
		bb.putInt(val.length);
		bb.put(val);
		out.write(bb.array());
		out.flush();
//		System.out.println(writeCount.incrementAndGet());
//		synchronized(bw) {
//			bw.write(String.valueOf(writeCount.incrementAndGet()));
//			bw.newLine();
//			bw.flush();
//		}
		if (val == null || val.length < 4) {
			System.out.println("packet too short");
		}
	}

	public byte[] readBytes() throws IOException {

		if (socket == null || !socket.isConnected())
			throw new IOException(
					"Error: Attempting to read from closed socket");

		byte[] input = null;
		int length = 0;
		int bytesRead = 0;
		length = readInt();
		if (length < 4) {
			System.out.println("Socket read Length " + length);
		}
		// System.out.println(length + " read from socket");
		input = new byte[length];
		if (getDataInputStream() != null && length > 0) {
			while (bytesRead < length) {
				input[bytesRead++] = getDataInputStream().readByte();
			}
		}
//		synchronized (rw) {
//			rw.write(String.valueOf(readCount.incrementAndGet()));
//			rw.newLine();
//			rw.flush();
//		}
		return input;
	}

	public int readInt() throws EOFException, IOException {
		if (socket == null || !socket.isConnected()) {
			System.out.println("Null Sockettt");
			throw new IOException(
					"Error: Attempting to read from closed socket");

		}
		return getDataInputStream().readInt();
	}

	public void closeAll() throws IOException {
		if (socket == null || !socket.isConnected())
			throw new IOException(
					"Error: Attempting to read from closed socket");

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

	public DataOutputStream getOut() {
		return out;
	}

	public int sendValue(int i) {
		try {
			// ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(i);

			writeBytes(bb.array());
			bb.clear();
			return 1;
		} catch (Exception ex) {
			System.out.println("Error: Socket Send " + ex.getMessage());
			return -1;
		}
	}

	// public int sendLong(long i)
	// {
	// try
	// {
	// //ByteArrayOutputStream baos = new ByteArrayOutputStream();
	// //ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
	// ByteBuffer bb = ByteBuffer.allocate(8);
	// bb.putLong(i);
	//
	// writeBytes(bb.array());
	// bb.clear();
	// return 1;
	// }
	// catch(Exception ex)
	// {
	// System.out.println("Error: Socket Send Long " + ex.getMessage());
	// return -1;
	// }
	// }

	public void trueClose() throws IOException {
		trueClose(true);
	}

	/**
	 * closes socket and all streams connected to it
	 * 
	 * @throws IOException
	 *             if fails to close streams or socket
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
