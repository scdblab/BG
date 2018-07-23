package kosar;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import kosar.AtomicBusyWaitingLock;
import kosar.ByteUtil;
import kosar.PacketFactory;
import kosar.SocketIO;

/**
 * Worker thread that will get token object from the request queue and working
 * on it.
 * 
 * @author Haoyu Huang
 * 
 */
public class ClientTokenWorker {

	public static final ConcurrentHashMap<String, Long> recWrites = new ConcurrentHashMap<String, Long>();

	public static final AtomicLong invalidatedItem = new AtomicLong(0);

	public void doWork(SocketIO socket, byte[] requestArray) {
		ByteBuffer bb = ByteBuffer.wrap(requestArray);
		int command = bb.getInt();
		byte[] response = null;
		switch (command) {
		case PacketFactory.CMD_REGISTER:
			break;
		case PacketFactory.CMD_UNREGISTER:
			break;
		case PacketFactory.CMD_INVALIDATE_NOTIFICATION:
			response = handle(bb);
			break;
		case PacketFactory.CMD_TEST_INVALIDATE:
			response = handle2(bb);
			break;
		case PacketFactory.CMD_STEAL:
			response = handleSteal(bb);
			break;
		case PacketFactory.CMD_USE_ONCE:
		case PacketFactory.CMD_COPY:
			response = handleCopy(bb);
			break;
		case PacketFactory.CMD_REQUEST_UPDATE:
			response = handleRequestUpdate(bb);
			break;
//		case PacketFactory.CMD_DML_SHIPPING:
//			response = handleDMLShipping(bb, doDMLListener);
//			break;
//		case PacketFactory.CMD_COMMIT_CHANGE:
//			response = handleCommitChange(bb);
//			break;
		default:
			break;
		}
		if (response != null) {
			try {
				socket.writeBytes(response);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

//	private byte[] handleCommitChange(ByteBuffer bb) {
//		long clientId = bb.getLong();
//		int transactionId = bb.getInt();
//		int result = bb.getInt();
//		int len = bb.getInt();
//		byte[] buf = new byte[len];
//		bb.get(buf);
//		String query = new String(buf);
//		
//		Set<String> keys = getKeysFromQuery(query);
//		for (String key: keys) {
//			CacheHelper.commitChange(key, clientId, transactionId, result);
//		}
//		
//		return PacketFactory.createCommandIdResposnePacket(
//				PacketFactory.CMD_OK).array();		
//	}
	
//	private byte[] handleDMLShipping(ByteBuffer bb, DoDMLListener listener) {
//		int len = bb.getInt();
//		byte[] buf = new byte[len];
//		bb.get(buf);
//		String query = new String(buf);
//		
//		DBJob job = new DBJob(listener, query, null, null);
//		DBWorker.addJob(job);	
//		
//		// reply with success
//		return PacketFactory.createCommandIdResposnePacket(
//				PacketFactory.CMD_OK).array();		
//	}

	private byte[] handleRequestUpdate(ByteBuffer bb) {
		long cid = bb.getLong();
		int tid = bb.getInt();
		int len = bb.getInt();
		byte[] buf = new byte[len];
		bb.get(buf);
		String query = new String(buf);
		
		HashMap<String, Object> data = new HashMap<String, Object>();
		Set<String> keys = new HashSet<String>();	
		
		keys = getKeysFromQuery(query);
		String[] tokens = query.split(",");
		char action = query.charAt(0);
		int inviter = Integer.parseInt(tokens[1]);
		int invitee = Integer.parseInt(tokens[2]);
		ClientAction.getCache(true, inviter, invitee, action, keys, null, null, null, data);
		ClientAction.updateCache(inviter, invitee, action, data, cid, tid);	
		
		// reply with success
		return PacketFactory.createCommandIdResposnePacket(
				PacketFactory.CMD_OK).array();
	}

	private Set<String> getKeysFromQuery(String query) { 
		Set<String> keys = new HashSet<String>();
		String[] ems = query.split(",");
		int inviter = Integer.parseInt(ems[1]);
		int invitee = Integer.parseInt(ems[2]);
		
		switch (query.charAt(0)) {
		case CoreClient.INVITE:
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee));
			keys.add(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee));
			break;
		case CoreClient.REJECT:
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee));
			keys.add(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee));
			break;
		case CoreClient.ACCEPT:
			keys.add(CoreClient.getIK(CoreClient.VIEW_PENDING, invitee));
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter));
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee));
			keys.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviter));
			keys.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitee));
			break;
		case CoreClient.THAW:
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter));
			keys.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee));
			keys.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviter));
			keys.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitee));
			break;
		}
		
		return keys;
	}

//	private void updateCache(String key, String query, 
//			Map<String, Map<String,Long>> keys2Clients, 
//			DoReadListener doReadListener, byte[] unmarshallBuffer) {
//		int id = Integer.parseInt(key.replaceAll("[^0-9]+", ""));		
//		String[] ems = query.split(",");
//		int inviter = Integer.parseInt(ems[1]);
//		int invitee = Integer.parseInt(ems[2]);
//
//		Object val = ClientAction.getLocalCache(key);
//		if (val == null)
//			return;
//		
//		String profileKey;
//		switch (query.charAt(0)) {
//		case CoreClient.INVITE:
//			switch (key.charAt(0)) {
//			case CoreClient.VIEW_PROFILE:
//				if (id == invitee) {
//					D.incr(val, "pendingcount", 1);
//				} else {
//					System.out.println(
//							String.format("Should not receive this key Reject "
//									+ "id=%d inviter=%d invitee=%d", 
//									id, inviter, invitee));
//				}
//				break;
//			case CoreClient.VIEW_PENDING:
//				profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee, inviter);
//				D.add(val, profileKey);
//				break;
//			}
//			break;
//		case CoreClient.REJECT:
//			switch (key.charAt(0)) {
//			case CoreClient.VIEW_PROFILE:
//				if (id == invitee) {
//					D.decr(val, "pendingcount", 1);
//				} else {
//					System.out.println(
//							String.format("Should not receive this key Reject "
//									+ "id=%d inviter=%d invitee=%d", 
//									id, inviter, invitee));
//				}
//				break;
//			case CoreClient.VIEW_PENDING:
//				if (id != invitee) {
//					System.out.println("Error: should not receive this"+id+ " "+invitee);
//				}
//				profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee, inviter);
//				D.rmv(val, profileKey);
//				break;
//			}
//			break;
//		case CoreClient.ACCEPT:
//			switch (key.charAt(0)) {
//			case CoreClient.VIEW_PENDING:
//				if (id == invitee) {
//					profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee, inviter);
//					D.rmv(val, profileKey);
//				} else {
//					System.out.println(
//							String.format("Should not receive this key Reject "
//									+ "id=%d inviter=%d invitee=%d", 
//									id, inviter, invitee));					
//				}
//				break;
//			case CoreClient.VIEW_PROFILE:
//				if (id == invitee) {
//					if (!key.contains("?")) {
//						D.decr(val, "pendingcount", 1);
//					}				
//				}
//				
//				D.incr(val, "friendcount", 1);
//				break;				
//			case CoreClient.LIST_FRIEND:
//				if (id == inviter) {
//					profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter, invitee);
//				} else {
//					profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee, inviter);
//				}
//				D.add(val, profileKey);
//				break;
//			}
//			break;
//		case CoreClient.THAW:
//			switch (key.charAt(0)) {
//			case CoreClient.VIEW_PROFILE:
//				D.decr(val, "friendcount", 1);
//				break;
//			case CoreClient.LIST_FRIEND:
//				if (id == inviter) {
//					profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviter, invitee);
//				} else {
//					profileKey = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitee, inviter);
//				}
//				D.rmv(val, profileKey);
//				break;
//			default:
//				System.out.println("THAW Should never receive this key " + key);
//				break;
//			}
//			break;
//		}
//	}

	private byte[] handleCopy(ByteBuffer bb) {
		int len;
		String ik;
		byte[] value;
		len = bb.getInt();
		byte[] buf = new byte[len];
		bb.get(buf);
		ik = ByteUtil.byte2String(buf);

		ClientAction.consumeFromMe.incrementAndGet();

		AtomicBusyWaitingLock lock = CoreClient.getLock(ik);
		lock.lock();
		value = CacheHelper.getSerialized(ik);
		if (value != null) {
			ClientAction.consumedFromMe.incrementAndGet();
		}
		byte[] ret = handleCopyRequest(ik, value);	
		lock.unlock();
		return ret;
	}

	private byte[] handleCopyRequest(String ik, byte[] buf) {
		if (buf == null) {
			return PacketFactory.createCommandIdResposnePacket(
					PacketFactory.CMD_USE_NULL).array();
		}

		return PacketFactory.createCommandIdResposnePacket(
				PacketFactory.CMD_USE_GRANTED, buf).array();
	}

	private byte[] handleSteal(ByteBuffer bb) {
		int len;
		String ik;
		Object value;
		len = bb.getInt();
		byte[] buf = new byte[len];
		bb.get(buf);
		ik = ByteUtil.byte2String(buf);
		ClientAction.stealFromMe.incrementAndGet();
		AtomicBusyWaitingLock lock = CoreClient.getLock(ik);
		lock.lock(); 
		value = CacheHelper.delete(ik, true);
		if (value != null) {
			ClientAction.stolenFromMe.incrementAndGet();
		}
		byte[] ret = handleCopyRequest(ik, (byte[])value);
		lock.unlock();
		return ret;
	}

	public byte[] handle2(ByteBuffer bb) {
		int tid = bb.getInt();
		int size = bb.getInt();
		Set<String> iks = new HashSet<String>();
		for (int i = 0; i < size; i++) {
			int len = bb.getInt();
			byte[] dest = new byte[len];
			bb.get(dest);
			iks.add(ByteUtil.byte2String(dest));
		}
		CoreClient.tid2Iks.put(tid, iks);
		return PacketFactory.createCommandIdResposnePacket(
				PacketFactory.CMD_INVALIDATED).array();
	}

//	private void decodeCheck(int tid, Set<String> iks) {
//		synchronized (CoreClient.tid2Iks) {
//			if (!CoreClient.tid2Iks.containsKey(tid)) {
//				System.out.println("fatal error: tid decode error");
//			} else {
//				Set<String> writes = CoreClient.tid2Iks.get(tid);
//				if (writes.size() != iks.size()) {
//					System.out.println("fatal error: ik decode error");
//				}
//				StringBuilder rec = new StringBuilder();
//				StringBuilder write = new StringBuilder();
//				for (String w : writes) {
//					write.append(w);
//					write.append(",");
//				}
//				for (String r : iks) {
//					rec.append(r);
//					rec.append(",");
//				}
//				for (String recIk : iks) {
//					if (!writes.contains(recIk)) {
//						System.out.println(String.format(
//								" decode error: write: %s rec: %s",
//								write.toString(), rec.toString()));
//						break;
//					}
//				}
//			}
//		}
//	}

	public static final ConcurrentHashMap<Integer, String> map = new ConcurrentHashMap<Integer, String>();
	
	public byte[] handle(ByteBuffer bb) {
		@SuppressWarnings("unused")
		long txClientId = bb.getLong();
		int tid = bb.getInt();
		@SuppressWarnings("unused")
		int corePort = bb.getInt();
		int size = bb.getInt();
		Set<String> iks = new HashSet<String>();
		for (int i = 0; i < size; i++) {
			int len = bb.getInt();
			byte[] dest = new byte[len];
			bb.get(dest);
			iks.add(ByteUtil.byte2String(dest));
		}
		
		for (String ik : iks) {
			AtomicBusyWaitingLock lock = CoreClient.getLock(ik);
			lock.lock();
			Object value = CacheHelper.delete(ik, false);
			if (value != null)
				invalidatedItem.incrementAndGet();
			lock.unlock();
		}
		
		return PacketFactory.createCommandIdResposnePacket(
				PacketFactory.CMD_INVALIDATED).array();
	}

}
