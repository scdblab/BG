package kosar;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import common.CacheUtilities;

import kosar.AtomicBusyWaitingLock;
import kosar.ByteUtil;
import kosar.PacketFactory;
import kosar.SocketIO;
import edu.usc.bg.base.ByteIterator;

/**
 * Wraps all raw client actions that interacts with Core. <br/>
 * All functions are package protected, users should use {@link ClientAction}
 * instead.
 * 
 * @author Haoyu Huang
 * 
 */
public class ClientBaseAction {

	// private static final Logger logger = LogManager
	// .getLogger(ClientBaseAction.class);

	static abstract class ActionListener<T, K> {

		private int responseCommandId;

		private T value;
		
		private K deltas;

		private int port;

		public abstract void onComplete();

		public abstract void onNetworkError(Exception e);

		public abstract void onFault(Throwable t);

		public int getRepsponseCommandId() {
			return responseCommandId;
		}

		public void setResponseCommandId(int result) {
			this.responseCommandId = result;
		}

		public T getResponseValue() {
			return value;
		}

		public void setResponseValue(T value) {
			this.value = value;
		}
		
		public K getDeltas() {
			return deltas;
		}
		
		public void setDeltas(K deltas) {
			this.deltas = deltas;
		}		

		public int getPort() {
			return port;
		}

		public void setPort(int port) {
			this.port = port;
		}
	}

	static long register(String remoteHostId, long clientId,
			int clientListenPort, ActionListener<Void, Void> listener) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(4 + 8 + 4);
			bb.putInt(PacketFactory.CMD_REGISTER);
			bb.putLong(clientId);
			bb.putInt(clientListenPort);
			socket.writeBytes(bb.array());

			ByteBuffer bytes = ByteBuffer.wrap(socket.readBytes());
			int retVal = bytes.getInt();
			if (retVal == PacketFactory.CMD_REGISTER) {
				bytes.getLong();
				bytes.getInt();
				ClientAction.consistencyMode = bytes.getInt();
			}
			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
		return clientId;
	}

	static boolean unregister(long clientId, String remoteHostId,
			ActionListener<Void, Void> listener) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(4 + 8);
			bb.putInt(PacketFactory.CMD_UNREGISTER);
			bb.putLong(clientId);
			socket.writeBytes(bb.array());

			int retVal = ByteBuffer.wrap(socket.readBytes()).getInt();
			AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
			return true;
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		return false;
	}

	public static Integer acquireILease(long clientId, String remoteHostId,
			Set<String> internalKeys, String key, Set<String> triggerSchemas, ActionListener<Map<String, Long>, List<Delta>> listener) {
		Integer leaseNumber = null;
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			int capacity = 8 + 4 + 4 + 4 + 4 + 4 + key.length();

			if (internalKeys != null && internalKeys.size() != 0) {
				for (String internalKey : internalKeys) {
					capacity += 4 + internalKey.length();
				}
			}

			if (triggerSchemas != null && triggerSchemas.size() != 0) {
				for (String trigger : triggerSchemas) {
					capacity += 4 + trigger.length();
				}
			}
			ByteBuffer bb = ByteBuffer.allocate(capacity);
			bb.putInt(PacketFactory.CMD_ACQUIRE_I_LEASE);
			bb.putLong(clientId);
			bb.putInt(internalKeys.size());
			bb.putInt(key.length());
			if (triggerSchemas != null) {
				bb.putInt(triggerSchemas.size());
			} else {
				bb.putInt(0);
			}
			bb.put(ByteUtil.string2Byte(key));
			if (internalKeys != null) {
				for (String internalKey : internalKeys) {
					bb.putInt(internalKey.length());
					bb.put(ByteUtil.string2Byte(internalKey));
				}
			}

			if (triggerSchemas != null) {
				for (String triggerSchema : triggerSchemas) {
					bb.putInt(triggerSchema.length());
					bb.put(ByteUtil.string2Byte(triggerSchema));
				}
			}
			socket.writeBytes(bb.array());

			ByteBuffer ret = ByteBuffer.wrap(socket.readBytes());
			int commandId = -10000;
			commandId = ret.getInt();
			int serverPort = -1;

			if (PacketFactory.CMD_REQUEST_ABORTED == commandId) {
				if (listener != null) {
					listener.setResponseCommandId(PacketFactory.CMD_REQUEST_ABORTED);
				}
				System.out.println("acquire I lease aborted");
				AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
				return leaseNumber;
			}

			if (PacketFactory.CMD_I_LEASE_GRANTED == commandId) {
				leaseNumber = ret.getInt();
				
				int size = ret.getInt();
				List<Delta> deltas = new LinkedList<Delta>();
				for (int i = 0; i < size; i++) {
					int op = ret.getInt();
					long id = ret.getLong();
					int transId = ret.getInt();
					switch (op) {
					case Delta.ADD:
					case Delta.RMV:
						int len = ret.getInt();
						byte[] attr = new byte[len];
						ret.get(attr);
						deltas.add(new Delta(id, transId, op, ByteUtil.byte2String(attr)));
						break;
					case Delta.INCR:
					case Delta.DECR:
						len = ret.getInt();
						attr = new byte[len];
						ret.get(attr);
						int d = ret.getInt();
						Pair<String, Integer> pair = 
								new Pair<String, Integer>(ByteUtil.byte2String(attr), d);
						deltas.add(new Delta(id, transId, op, pair));
						break;
					}
				}
				
				if (listener != null)
					listener.setDeltas(deltas);
				// logger.debug("lease number: " + leaseNumber);
			} else if (PacketFactory.CMD_I_LEASE_REJECTED == commandId) {
				// logger.debug("I lease rejected");
			} else if (PacketFactory.CMD_TRIGGER_REGISTER_FAILED == commandId) {
				System.out.println("trigger failed");
			} else if (PacketFactory.CMD_USE_ONCE == commandId
					|| PacketFactory.CMD_COPY == commandId
					|| PacketFactory.CMD_STEAL == commandId) {
				leaseNumber = ret.getInt();
				int size = ret.getInt();
				Map<String, Long> responseIds = new HashMap<String, Long>();
				for (int i = 0; i < size; i++) {
					long id = ret.getLong();
					int len = ret.getInt();
					byte[] addr = new byte[len];
					ret.get(addr);
					responseIds.put(ByteUtil.byte2String(addr), id);
				}
				
				if (listener != null) {
					listener.setResponseValue(responseIds);
				}
			} else {
				System.out.println("doesn't recognize this cmd");
			}
			
			if (listener != null) {
				listener.setResponseCommandId(commandId);
				listener.setPort(serverPort);
			}
			
			if (commandId == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
		return leaseNumber;
	}

	static Object copyValue(long fromClientId, int action, String addr,
			String query, byte[] unmarshallBuffer) {
		Object res = null;
		try {
			if (!AsyncSocketServer.SocketPool.containsKey(addr)) {
				System.out.println("no sockets for this addr" + addr);
				AsyncSocketServer.generateSocketPool(addr);
//				System.exit(0);
			}
			SocketIO socketIO = AsyncSocketServer.SocketPool.get(addr)
					.getConnection();
			ByteBuffer buffer = ByteBuffer.allocate(8 + query.length());
			buffer.putInt(action);
			buffer.putInt(query.length());
			buffer.put(ByteUtil.string2Byte(query));
			socketIO.writeBytes(buffer.array());

			ByteBuffer ret = ByteBuffer.wrap(socketIO.readBytes());
			int commandId = ret.getInt();
			if (PacketFactory.CMD_USE_GRANTED == commandId) {
				int len = ret.getInt();
				byte[] buf = new byte[len];
				ret.get(buf);
				
				res = CacheHelper.deserialize(buf, unmarshallBuffer);
//				if (CoreClient.VIEW_PROFILE == query.charAt(0)) {
//					res = new HashMap<String, ByteIterator>();
//					common.CacheUtilities.unMarshallHashMap(
//							(HashMap<String, ByteIterator>) res, buf,
//							unmarshallBuffer);
//				} else if (CoreClient.LIST_FRIEND == query.charAt(0)
//						|| CoreClient.VIEW_PENDING == query.charAt(0)) {
//					res = new Vector<HashMap<String, ByteIterator>>();
//					common.CacheUtilities.unMarshallVectorOfStrings(buf,
//							(Vector<String>) res, unmarshallBuffer);
//				} else {
//					System.out.println(String.format(
//							"copy value don't know this query %s", query));
//				}

			} else if (PacketFactory.CMD_USE_REJECTED == commandId
					|| PacketFactory.CMD_USE_NULL == commandId) {
			}
			AsyncSocketServer.SocketPool.get(addr).checkIn(socketIO);
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", addr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		return res;
	}

//	static Object asyncCopyValue(int clientId, int action,
//			Map<Integer, String> clients, String key,
//			ActionListener<List<Integer>, Void> listener) {
//		Object value = null;
//		if (clients.size() == 0) {
//			return value;
//		}
//		ExecutorService executorService = Executors.newFixedThreadPool(clients
//				.size());
//		Set<Integer> ids = clients.keySet();
//		List<Future<Object>> futures = new ArrayList<Future<Object>>();
//		List<CopyValueCallable> callables = new ArrayList<CopyValueCallable>();
//		List<Integer> rejectedIds = new ArrayList<Integer>();
//		for (Integer id : ids) {
//			CopyValueCallable c = new CopyValueCallable(clients.get(id),
//					action, key);
//			callables.add(c);
//			Future<Object> future = executorService.submit(c);
//			futures.add(future);
//		}
//		for (int i = 0; i < futures.size(); i++) {
//			try {
//				Object res = futures.get(i).get();
//				if (res != null) {
//					value = res;
//				}
//				if (callables.get(i).isRejected()) {
//					rejectedIds.add(callables.get(i).getClientId());
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//				System.out.println(String.format("copy %s failed %d %s", key,
//						callables.get(i).getClientId(), callables.get(i)
//								.getAddr()));
//			} catch (ExecutionException e) {
//				e.printStackTrace();
//				System.out.println(String.format("copy %s failed %d %s", key,
//						callables.get(i).getClientId(), callables.get(i)
//								.getAddr()));
//			}
//		}
//		if (listener != null) {
//			listener.setResponseValue(rejectedIds);
//		}
//		executorService.shutdown();
//		return value;
//	}

	public static Object releaseILease(int cmdId, long clientId, 
			String remoteHostId, Integer leaseNumber, 
			Set<String> internalKeys, String key,
			long gotFromClientId, ActionListener<Void, Void> listener, Object value) {		
		int retVal = -10000;
		try {
//			String temp = null;
			AtomicBusyWaitingLock lock;
			
			if (ClientAction.consistencyMode == ClientAction.NO_IQ) {
				retVal = PacketFactory.CMD_I_LEASE_RELEASED;
//				for (String internalKey : internalKeys) {
//					temp = internalKey;
//				}
//				lock = CoreClient.getLock(temp);
				lock = CoreClient.getLock(key);
				lock.lock();
			} else {
				SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
						.getConnection();
				int capacity = 8 + 4 + 4 + 4 + 4 + key.length();
				if (internalKeys != null && internalKeys.size() != 0) {
					for (String internalKey : internalKeys) {
						capacity += 4 + internalKey.length();
					}
				}
				if (PacketFactory.CMD_STOLEN == cmdId
						|| PacketFactory.CMD_CONSUMED == cmdId) {
					capacity += 8;
				}
	
				ByteBuffer bb = ByteBuffer.allocate(capacity);
				bb.putInt(cmdId);
				bb.putLong(clientId);
				bb.putInt(leaseNumber.intValue());
				bb.putInt(internalKeys.size());
				bb.putInt(key.length());
				if (PacketFactory.CMD_STOLEN == cmdId
						|| PacketFactory.CMD_CONSUMED == cmdId) {
					if (gotFromClientId <= 0) {
						System.out.println("got from client Id " + gotFromClientId);
					}
					bb.putLong(gotFromClientId);
				}
				bb.put(ByteUtil.string2Byte(key));				
				if (internalKeys != null) {
					for (String internalKey : internalKeys) {
//						temp = internalKey;
						bb.putInt(internalKey.length());
						bb.put(ByteUtil.string2Byte(internalKey));
					}
				}
				
//				lock = CoreClient.getLock(temp);
				lock = CoreClient.getLock(key);
				lock.lock();
				socket.writeBytes(bb.array());
	
				retVal = ByteBuffer.wrap(socket.readBytes()).getInt();
				AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
			}

			if (PacketFactory.CMD_I_LEASE_RELEASED == retVal
					|| PacketFactory.CMD_STOLEN_RELEASED == retVal) {
//				if (temp != null && value != null) {
				if (key != null && value != null) {
					ClientAction.releasedItem.incrementAndGet();
//					CacheHelper.set(temp, value);					
					CacheHelper.set(key, value);
				}
			}
			
			lock.unlock();

			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		return value;
	}

	static int invalidateNotification(long clientId, String remoteHostId,
			int transactionId, Set<String> internalKeys,
			ActionListener<Void, Void> listener) {
		int retVal = -1;
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			int capacity = 20;
			if (internalKeys != null) {
				for (String ik : internalKeys) {
					capacity += (4 + ik.length());
				}
			}

			ByteBuffer bb = ByteBuffer.allocate(capacity);

			bb.putInt(PacketFactory.CMD_INVALIDATE_NOTIFICATION);
			bb.putLong(clientId);
			bb.putInt(transactionId);
			if (internalKeys != null) {
				bb.putInt(internalKeys.size());
				for (String ik : internalKeys) {
					bb.putInt(ik.length());
					bb.put(ByteUtil.string2Byte(ik));
				}
			} else {
				bb.putInt(0);
			}

			socket.writeBytes(bb.array());

			retVal = ByteBuffer.wrap(socket.readBytes()).getInt();

			if (PacketFactory.CMD_REQUEST_ABORTED == retVal) {
				AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
				return retVal;
			}

			
			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
		return retVal;
	}

	static int commitTID(long clientId, String remoteHostId, int transactionId,
			ActionListener<Void, Void> listener) {
		if (ClientAction.consistencyMode == ClientAction.I_ONLY ||
				ClientAction.consistencyMode == ClientAction.NO_IQ)
			return 0;
		
		int retVal = -1;
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.putInt(PacketFactory.CMD_COMMIT_TID);
			bb.putLong(clientId);
			bb.putInt(transactionId);
			socket.writeBytes(bb.array());

			retVal = ByteBuffer.wrap(socket.readBytes()).getInt();

			if (PacketFactory.CMD_REQUEST_ABORTED == retVal) {
				AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
				return retVal;
			}
			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
		return retVal;
	}

	static int abortTID(long clientId, String remoteHostId, int transactionId,
			ActionListener<Void, Void> listener) {
		int retVal = -1;
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.putInt(PacketFactory.CMD_ABORT_TID);
			bb.putLong(clientId);
			bb.putInt(transactionId);
			socket.writeBytes(bb.array());

			retVal = ByteBuffer.wrap(socket.readBytes()).getInt();

			if (PacketFactory.CMD_REQUEST_ABORTED == retVal) {
				AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
				return retVal;
			}
			if (listener != null) {
				listener.setResponseCommandId(retVal);
			}
			if (retVal == 1) {
				if (listener != null) {
					listener.onComplete();
				}
			}
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
		return retVal;
	}

	static void shutdownLocalAndRemoteHostSocketPool(long clientId,
			String remoteHostId) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(4 + 8);
			bb.putInt(RequestHandler.SHUTDOWN_SOCKET_POOL_REQUEST);
			bb.putLong(clientId);
			socket.writeBytes(bb.array());
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
	}

	public static void printStat(String remoteHostId) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(remoteHostId)
				.getConnection();
		try {
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(PacketFactory.CMD_STAT);
			socket.writeBytes(bb.array());
			socket.readBytes();
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", remoteHostId, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		AsyncSocketServer.SocketPool.get(remoteHostId).checkIn(socket);
	}
	
	public static int acquireQLease2(long clientId, String serverAddr, int transactionId,
			HashMap<String, List<Delta>> ik2Deltas, HashMap<String, Map<String, Long>> hm) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(serverAddr).getConnection();
		int retVal = -1;
		try {
			int capacity = 20;
			if (ik2Deltas != null) {
				for (String ik: ik2Deltas.keySet()) {
					capacity += 4 + ik.length();
					List<Delta> deltas = ik2Deltas.get(ik);
					capacity += 4;
					if (deltas != null) {
						for (Delta d: deltas) {
							capacity += 4;	// for op
							switch (d.getOp()) {
							case Delta.ADD:
							case Delta.RMV:
								capacity += 4 + ((String)d.getData()).length();
								break;
							case Delta.INCR:
							case Delta.DECR:
								@SuppressWarnings("unchecked")
								Pair<String, Integer> pair = (Pair<String, Integer>)d.getData();
								capacity += 4 + pair.getKey().length() + 4;
								break;
							}
						}
					}
				}
			}

			ByteBuffer bb = ByteBuffer.allocate(capacity);
			bb.putInt(PacketFactory.CMD_ACQUIRE_Q_LEASE_REFILL);
			bb.putLong(clientId);
			bb.putInt(transactionId);
			if (ik2Deltas != null) {
				bb.putInt(ik2Deltas.size());
				for (String ik : ik2Deltas.keySet()) {
					bb.putInt(ik.length());
					bb.put(ByteUtil.string2Byte(ik));
					
					List<Delta> deltas = ik2Deltas.get(ik);
					if (deltas != null) {
						bb.putInt(deltas.size());
						for (Delta d: deltas) {
							bb.putInt(d.getOp());
							switch (d.getOp()) {
							case Delta.ADD:
							case Delta.RMV:
								String attr = (String)d.getData();
								bb.putInt(attr.length());
								bb.put(edu.usc.IQ.base.ByteUtil.string2Byte(attr));
								break;
							case Delta.INCR:
							case Delta.DECR:
								@SuppressWarnings("unchecked")
								Pair<String, Integer> pair = (Pair<String, Integer>) d.getData();
								bb.putInt(pair.getKey().length());
								bb.put(ByteUtil.string2Byte(pair.getKey()));
								bb.putInt(pair.getValue());
								break;
							}
						}
					} else {
						bb.putInt(0);
					}
				}
			} else {
				bb.putInt(0);
			}

			ByteBuffer res;
			while (true) {		// retry until the Q lease is granted.
				ClientAction.numSendQLeases.incrementAndGet();
				socket.writeBytes(bb.array());
				
				res = ByteBuffer.wrap(socket.readBytes());
				retVal = res.getInt();
	
				if (retVal != PacketFactory.CMD_Q_LEASE_GRANTED) {
//					System.out.println(String.format("Q lease is reject %d",transactionId));
					Thread.sleep(CoreClient.backOffTime);	
					ClientAction.backoffWrites.incrementAndGet();
					continue;
				}
				break;
			}
			
			bb.clear();			
			
			int size = res.getInt();
			for (int i = 0; i < size; i++) {
				int len = res.getInt();
				
				byte[] dat = new byte[len];
				res.get(dat);								
				String key = new String(dat);
				
				int sz = res.getInt();
				
				HashMap<String, Long> cids = new HashMap<String, Long>();
				for (int j = 0; j < sz; j++) {
					long cid = res.getLong();
					int l = res.getInt();
					byte[] addr = new byte[l];
					res.get(addr);
					cids.put(new String(addr), cid);
				}
				
				hm.put(key, cids);
			}
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", serverAddr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		AsyncSocketServer.SocketPool.get(serverAddr).checkIn(socket);
		return retVal;
	}


	public static int acquireQLease(long clientId, String serverAddr, int transactionId,
			Set<String> iks, HashMap<String, Map<String, Long>> hm) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(serverAddr).getConnection();
		int retVal = -1;
		try {
			int capacity = 20;
			if (iks != null) {
				for (String ik : iks) {
					capacity += (4 + ik.length());
				}
			}

			ByteBuffer bb = ByteBuffer.allocate(capacity);
			bb.putInt(PacketFactory.CMD_ACQUIRE_Q_LEASE_REFRESH);
			bb.putLong(clientId);
			bb.putInt(transactionId);
			if (iks != null) {
				bb.putInt(iks.size());
				for (String ik : iks) {
					bb.putInt(ik.length());
					bb.put(ByteUtil.string2Byte(ik));
				}
			} else {
				bb.putInt(0);
			}

			ByteBuffer res;
			while (true) {		// retry until the Q lease is granted.
				ClientAction.numSendQLeases.incrementAndGet();
				socket.writeBytes(bb.array());
				
				res = ByteBuffer.wrap(socket.readBytes());
				retVal = res.getInt();
	
				if (retVal != PacketFactory.CMD_Q_LEASE_GRANTED) {
//					System.out.println(String.format("Q lease is reject %d",transactionId));
					Thread.sleep(CoreClient.backOffTime);	
					ClientAction.backoffWrites.incrementAndGet();
					continue;
				}
				break;
			}
			
			bb.clear();			
			
			int size = res.getInt();
			for (int i = 0; i < size; i++) {
				int len = res.getInt();
				
				byte[] dat = new byte[len];
				res.get(dat);								
				String key = new String(dat);
				
				int sz = res.getInt();
				
				HashMap<String, Long> cids = new HashMap<String, Long>();
				for (int j = 0; j < sz; j++) {
					long cid = res.getLong();
					int l = res.getInt();
					byte[] addr = new byte[l];
					res.get(addr);
					cids.put(new String(addr), cid);
				}
				
				hm.put(key, cids);
			}
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", serverAddr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		AsyncSocketServer.SocketPool.get(serverAddr).checkIn(socket);
		return retVal;
	}

	public static int releaseQLease(long cid, String serverAddr, int tid, Set<String> iks) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(serverAddr).getConnection();
		int retVal = -1;
		try {
			// calc size
//			int size = 4;	// len iks
//			for (String ik: iks) {
//				size += 4+ ik.length() + 4 + 4+ik.length();
//			}
//			size += 4;
			
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.putInt(PacketFactory.CMD_RELEASE_Q_LEASE_REFRESH);
			bb.putLong(cid);
			bb.putInt(tid);
			
			socket.writeBytes(bb.array());
			bb.clear();
			
			retVal = ByteBuffer.wrap(socket.readBytes()).getInt();
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", serverAddr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		AsyncSocketServer.SocketPool.get(serverAddr).checkIn(socket);
		return retVal;
	}

	public static int abortQLeaseRefresh(long cid, String serverAddr, int tid) {
		SocketIO socket = AsyncSocketServer.SocketPool.get(serverAddr).getConnection();
		int retVal = -1;
		try {
			ByteBuffer bb = ByteBuffer.allocate(16);
			bb.putInt(PacketFactory.CMD_ABORT_Q_LEASE_REFRESH);
			bb.putLong(cid);
			bb.putInt(tid);
			
			socket.writeBytes(bb.array());
			bb.clear();
			
			retVal = ByteBuffer.wrap(socket.readBytes()).getInt();
		} catch (Exception e) {
			System.out.println(String.format("ip %s %s", serverAddr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);			
		}
		
		AsyncSocketServer.SocketPool.get(serverAddr).checkIn(socket);
		return retVal;
	}

	public static int requestUpdate(long cid, int tid, String addr, String query, Set<String> iks, 
			Map<String, Map<String, Long>> keys2clients) {
		int res = -1;
		try {
			if (!AsyncSocketServer.SocketPool.containsKey(addr)) {
				System.out.println("no sockets for this addr" + addr);
				AsyncSocketServer.generateSocketPool(addr);
			}
			SocketIO socketIO = AsyncSocketServer.SocketPool.get(addr)
					.getConnection();
			int size = 8 + 8 + 4 + query.length();
//			for (String ik: iks) {
//				size += 4 + ik.length();
//				
//				if (keys2clients != null) {
//					Map<String, Long> clients = keys2clients.get(ik);
//					size += 4;
//					for (String client: clients.keySet()) {
//						size += 4 + client.length() + 8;
//					}
//				} else {
//					size += 4;
//					size += 4+ CoreClient.me.length() + 8;
//				}
//			}
			
			ByteBuffer buffer = ByteBuffer.allocate(size);
			buffer.putInt(PacketFactory.CMD_REQUEST_UPDATE);
			buffer.putLong(cid);
			buffer.putInt(tid);
			buffer.putInt(query.length());
			buffer.put(ByteUtil.string2Byte(query));
			
//			buffer.putInt(iks.size());
//			for (String ik: iks) {
//				buffer.putInt(ik.length());
//				buffer.put(ByteUtil.string2Byte(ik));
//				
//				if (keys2clients != null) {
//					Map<String, Long> clients = keys2clients.get(ik);
//					buffer.putInt(clients.size());
//					for (String client: clients.keySet()) {
//						buffer.putInt(client.length());
//						buffer.put(client.getBytes());
//						buffer.putLong(clients.get(client));
//					}
//				} else {
//					buffer.putInt(1);
//					buffer.putInt(CoreClient.me.length());
//					buffer.put(CoreClient.me.getBytes());
//					buffer.putLong(cid);
//				}
//			}
			
			socketIO.writeBytes(buffer.array());

			ByteBuffer ret = ByteBuffer.wrap(socketIO.readBytes());
			res = ret.getInt();
			
			if (res != PacketFactory.CMD_OK) {
				System.out.println(
						"Something went wrong with requesting update.");
			}
			
			AsyncSocketServer.SocketPool.get(addr).checkIn(socketIO);
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", addr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
		return 0;
	}

	public static void sendDML(long clientid, String addr, 
			int transactionId, String query, Set<String> iks, boolean priority) {
		int res = -1;
		try {
			if (!AsyncSocketServer.SocketPool.containsKey(addr)) {
				System.out.println("No socket for this addr "+addr);
				AsyncSocketServer.generateSocketPool(addr);
			}
			
			SocketIO socketIO = AsyncSocketServer.SocketPool.get(addr)
					.getConnection();
			int size = 4 + 8 + 4 + 4 + 4 + query.length()+4;
			if (iks != null) {
				for (String ik: iks) {
					size += 4 + ik.length();
				}
			}
			
			ByteBuffer buffer = ByteBuffer.allocate(size);
			buffer.putInt(PacketFactory.CMD_DML_SHIPPING);
			if (priority)
				buffer.putInt(1);
			else
				buffer.putInt(0);
			buffer.putLong(clientid);
			buffer.putInt(transactionId);
			buffer.putInt(query.length());
			buffer.put(ByteUtil.string2Byte(query));
			
			if (iks != null) {
				buffer.putInt(iks.size());
				for (String ik: iks) {
					buffer.putInt(ik.length());
					buffer.put(ik.getBytes());
				}
			} else {
				buffer.putInt(0);
			}
			
			socketIO.writeBytes(buffer.array());

			ByteBuffer ret = ByteBuffer.wrap(socketIO.readBytes());
			res = ret.getInt();
			
			if (res != PacketFactory.CMD_OK) {
				System.out.println(
						"Something went wrong with requesting update.");
			}
			
			AsyncSocketServer.SocketPool.get(addr).checkIn(socketIO);
		}  catch (Exception e) {
			System.out.println(String.format("ip %s %s", addr, e.getMessage()));
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}
}
