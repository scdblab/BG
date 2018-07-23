package kosar;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;

import kosar.PacketFactory;
import kosar.SocketIO;


/**
 * An asynchronous task that renew life lease with Core in the given interval 
 * 
 * @author Haoyu Huang
 *
 */
public class LifeLeaseRenewer implements Runnable {

	/**
	 * renew life lease request
	 */
	private byte[] request;
	
	private long duration;
	
	private volatile boolean isRunning = false;
	
//	private final Logger logger = LogManager.getLogger(LifeLeaseRenewer.class);
	
	public LifeLeaseRenewer(long clientId, long duration) {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putInt(PacketFactory.CMD_RENEW_LIVE_LEASE);
		bb.putLong(clientId);
		this.request = bb.array();
		this.duration = duration;
		this.isRunning = true;
	}
	
	@Override
	public void run() {
		while (isRunning) {
			try {
				SocketIO socket = AsyncSocketServer.SocketPool.get(ClientAction.SERVER_ID).getConnection();
				socket.writeBytes(request);
				ByteBuffer ret = ByteBuffer.wrap(socket.readBytes());
				
				int cmd = ret.getInt();
				if (PacketFactory.CMD_LIVE_LEASE_RENEWED == cmd) {
//					logger.info("renewed successfully");
				} else {
					// stop using cache.
				}
				AsyncSocketServer.SocketPool.get(ClientAction.SERVER_ID).checkIn(socket);
			} catch (ConnectException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(this.duration);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void stop() {
		this.isRunning = false;
	}
}
