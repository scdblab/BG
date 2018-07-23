package kosar;

import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Properties;

import kosar.SocketIO;

/**
 * Each Client has one RequestHandler, which manages its requests. Though each
 * command may be a multi-threaded request, the request handler is
 * single-threaded. Requests may be processed on a different thread.
 */
public class RequestHandler extends Thread {
	final public static int HANDSHAKE_REQUEST = 0;
	final public static int STOP_HANDLING_REQUEST = 98;
	final public static int FULL_SHUTDOWN_REQUEST = 9999;
	final public static int SHUTDOWN_SOCKET_POOL_REQUEST = 999;
	private boolean continueHandling = true;
	private int handlerID;
	SocketIO socket = null;

	RequestHandler(Socket sock, int handlerID, Properties properties) {
		try {
			this.socket = new SocketIO(sock);
			this.handlerID = handlerID;
		} catch (IOException e) {
			System.out
					.println("Error: RequestHandler - Unable to establish SocketIO from socket");
		}
	}

	public void run() {
		byte request[] = null;
		try {
			while (continueHandling) {
				try {
					if (socket != null || !socket.getSocket().isConnected()
							|| !socket.getSocket().isClosed()) {
						// Reads in the request from the Client.
						// Format is always 4-byte int command followed by byte
						// array
						// whose format is determined by the command.
						request = socket.readBytes();
					} else {
						System.out
								.println("Error: RequestHandler - Socket is null.");
						break;
					}
				} catch (SocketTimeoutException e) {
//					e.printStackTrace();
				} catch (EOFException eof) {
//					eof.printStackTrace();
					System.out.println("End of Stream. Good Bye.");
					socket.closeAll();
					break;
				} catch (Exception e) {
//					e.printStackTrace();
//					System.out.println(e.getMessage()
//							+ "Client Stream Shutdown.");
//					System.exit(0);
					break;
				}
				ClientTokenWorker worker = new ClientTokenWorker();
				worker.doWork(socket, request);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				socket.closeAll();
			} catch (IOException e) {
				System.out
						.println("Error: RequestHandler - Failed to close streams");
				e.printStackTrace();
			}
			if (AsyncSocketServer.verbose)
				System.out.println("RequestHandler "
						+ " shut down. I/O cleanup complete.");
		}
	}

	public int sumShutdownReqs() {
		int sum = 0;
		for (int i = 0; i < AsyncSocketServer.handlercount.get(); i++)
			if (AsyncSocketServer.handlers.get(i).continueHandling == false)
				sum = sum + 1;
		return sum;
	}

	public SocketIO getSocket() {
		return socket;
	}

	public void setSocket(SocketIO socket) {
		this.socket = socket;
	}

	public void stopHandling() {
		this.continueHandling = false;
	}
}