package kosar;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Vector;
import java.util.concurrent.Callable;

import kosar.ByteUtil;
import kosar.PacketFactory;
import kosar.SocketIO;
import edu.usc.bg.base.ByteIterator;

/**
 * An asynchronous task that will consume value for a query from other client
 * 
 * @author Haoyu Huang
 * 
 */
public class CopyValueCallable implements Callable<Object> {

	/**
	 * target client Id
	 */
	private int clientId;

	/**
	 * target client address
	 */
	private String addr;

	/**
	 * action to be performed. The value belongs to one of {@value PacketFactory#CMD_STEAL}, {@value PacketFactory#CMD_USE_ONCE}, {@value PacketFactory#CMD_COPY}
	 */
	private int action;

	/**
	 * query
	 */
	private String query;

	private boolean rejected = false;
	
	byte[] read_buffer = new byte[65536];
	
//	private final Logger logger = LogManager.getLogger(CopyValueCallable.class);

	public CopyValueCallable(String addr, int action, String query) {
		super();
		this.addr = addr;
		this.action = action;
		this.query = query;
	}
	
	@Override
	public String toString() {
		return "CopyValueCallable [clientId=" + clientId + ", addr=" + addr
				+ ", action=" + action + ", query=" + query + "]";
	}



	@SuppressWarnings("unchecked")
	@Override
	public Object call() throws Exception {
		synchronized (AsyncSocketServer.SocketPool) {
			if (!AsyncSocketServer.SocketPool.containsKey(addr)) {
				AsyncSocketServer.generateSocketPool(addr);
				System.out.println("generate sockets to client " + addr);
			}
		}
		Object res = null;
		
		SocketIO socketIO = AsyncSocketServer.SocketPool.get(addr)
				.getConnection();
		ByteBuffer buffer = ByteBuffer.allocate(8 + query.length());
		buffer.putInt(action);
		buffer.putInt(this.query.length());
		buffer.put(ByteUtil.string2Byte(query));
		socketIO.writeBytes(buffer.array());

		ByteBuffer ret = ByteBuffer.wrap(socketIO.readBytes());
		int commandId = ret.getInt();
		if (PacketFactory.CMD_USE_GRANTED == commandId) {
			int len = ret.getInt();
			byte[] buf = new byte[len];
			ret.get(buf);
			if (CoreClient.VIEW_PROFILE == this.query.charAt(0)) {
				res = new HashMap<String, ByteIterator>();
				common.CacheUtilities.unMarshallHashMap((HashMap<String, ByteIterator>) res, buf, read_buffer);
			} else if (CoreClient.LIST_FRIEND == this.query.charAt(0) || CoreClient.VIEW_PENDING == this.query.charAt(0)) {
				res = new Vector<HashMap<String, ByteIterator>>();
				common.CacheUtilities.unMarshallVectorOfHashMaps(buf, (Vector<HashMap<String, ByteIterator>>) res, read_buffer);
			} else {
				System.out.println(String.format("don't know this query %s", this.query));
			}
			
		} else if (PacketFactory.CMD_USE_REJECTED == commandId || PacketFactory.CMD_USE_NULL == commandId) {
			rejected = true;
		}
		AsyncSocketServer.SocketPool.get(addr).checkIn(socketIO);
		return res;
	}

	public boolean isRejected() {
		return rejected;
	}

	public int getClientId() {
		return clientId;
	}

	public String getAddr() {
		return addr;
	}
}
