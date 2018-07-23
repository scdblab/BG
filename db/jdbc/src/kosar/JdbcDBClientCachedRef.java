package kosar;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import kosar.JdbcDBClient;

import com.meetup.memcached.IQException;
//import com.meetup.memcached.Logger;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import common.CacheUtilities;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class JdbcDBClientCachedRef extends DB {
	private static final String CACHE_POOL_NAME = "JdbcDBClientCached";

	private static final int CACHE_POOL_NUM_CONNECTIONS = 100;

	DB db = new JdbcDBClient();

	private MemcachedClient cacheclient;

	private static SockIOPool cacheConnectionPool;
	
	private static kosar.SockIOPool[] dmlServerConnectionPools;

	private static String[] servers = new String[] {
		"10.0.0.210:11211"
	};
	
	public static int sleepTime = 15;
	public static final boolean ASYNC = false;

	static {
		try(BufferedReader br = new BufferedReader(new FileReader("/home/hieun/Desktop/BG/servers.txt"))) {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();

			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			String everything = sb.toString().replaceAll("(\\r|\\n)", "");
			servers = everything.split(" ");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
		}
		
		System.out.println("Cache servers: ");
		for (String s: servers) {
			System.out.println(s);
		}
	}

	private Object payload;

	private byte[] deserialize_buffer = new byte[1024 * 5];

	private boolean verbose = false;

	private boolean insertImage = false;

	private SimpleDoReadListener doReadListener = new SimpleDoReadListener(db);
	DoDMLListener doDMLListener = new SimpleDoDMLListener(db, null);

	private boolean limitFriends = true;

	private static final int MAX_LFRIENDS = 10;

	public static boolean initialized = false;
	public static Semaphore semaphore = new Semaphore(1, true);

	public static AtomicInteger numThreads = new AtomicInteger(0);

	public static AtomicCyclicInteger tid = new AtomicCyclicInteger(Integer.MAX_VALUE);
	public static long cid = Utils.genID(10001);

	@Override
	public boolean init() throws DBException {
		db.init();				
		
		ClientAction.resetStats();

		numThreads.incrementAndGet();

		if (!initialized) {
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.out);	
				return false;
			}

			if (!initialized) {
				cacheConnectionPool = SockIOPool.getInstance( CACHE_POOL_NAME );
				cacheConnectionPool.setServers( servers );
				cacheConnectionPool.setFailover( true );
				cacheConnectionPool.setInitConn( 10 ); 
				cacheConnectionPool.setMinConn( 5 );
				cacheConnectionPool.setMaxConn( CACHE_POOL_NUM_CONNECTIONS );			
				cacheConnectionPool.setMaintSleep( 0 );
				cacheConnectionPool.setNagle( false );
				cacheConnectionPool.setSocketTO( 0 );
				cacheConnectionPool.setAliveCheck( false );

				System.out.println("Initializing cache connection pool..." + new Date().toString());
				cacheConnectionPool.initialize();

				initialized = true;
			}
			
			if (ASYNC) {
				dmlServerConnectionPools = new kosar.SockIOPool[servers.length];
				for (int i = 0; i < servers.length; i++) {
					String[] s = servers[i].split(":");
					String server = s[0]+":18188";
					dmlServerConnectionPools[i] = new kosar.SockIOPool(server, 100);
				}
			}

			semaphore.release();
		}

		try {
			cacheclient = new MemcachedClient(CACHE_POOL_NAME);
			cacheclient.setCompressEnable(false);
		} catch (Exception e) {
			System.out.println("MemcacheClient init failed to release semaphore.");
			e.printStackTrace(System.out);
			return false;
		}
		
		List<Logger> loggers = Collections.<Logger>list(LogManager.getCurrentLoggers());
		loggers.add(LogManager.getRootLogger());
		for ( Logger logger : loggers ) {
		    logger.setLevel(Level.OFF);
		}

		return true;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		db.cleanup(warmup);

		int remainThreads = numThreads.decrementAndGet();

		if (remainThreads == 0 && initialized) {
			try {
				semaphore.acquire();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			if (initialized) {
				cacheConnectionPool.shutDown();
				cacheConnectionPool = null;
				System.out.println("Connection pool shut down");
				initialized = false;
			}

			semaphore.release();
		}
		
		System.out.println("Max backoff: "+MemcachedClient.maxBackoff);
	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		return db.insertEntity(entitySet, entityPK, values, insertImage);
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		String query = CoreClient.getQuery(CoreClient.VIEW_PROFILE,
				requesterID, profileOwnerID, insertImage) + "S";		
		String key = CoreClient.getIK(CoreClient.VIEW_PROFILE, profileOwnerID);
		doRead(query, key, result);

		query = CoreClient.getQuery(CoreClient.FRIEND_COUNT, profileOwnerID, false);
		key = CoreClient.getIK(CoreClient.FRIEND_COUNT, profileOwnerID);
		StringBuffer sb = new StringBuffer();
		doRead(query, key, sb);
		result.put("friendcount", new ObjectByteIterator(sb.toString().getBytes()));

		if (requesterID == profileOwnerID) {
			query = CoreClient.getQuery(CoreClient.PENDING_COUNT, profileOwnerID, false);
			key = CoreClient.getIK(CoreClient.PENDING_COUNT, profileOwnerID);
			sb = new StringBuffer();
			doRead(query, key, sb);
			result.put("pendingcount", new ObjectByteIterator(sb.toString().getBytes()));
		}

		return 0;
	}

	private int getIDFromKey(String key) {
		String str = key.replaceAll("[^-0-9]+", " ");
		List<String> list = Arrays.asList(str.trim().split(" "));
		return Integer.parseInt(list.get(0));
	}

	protected Vector<String> computeListOfProfileKeys(String key, Vector<HashMap<String, ByteIterator>> val) {
		Vector<String> value = new Vector<String>();
		for (HashMap<String, ByteIterator> hm: val) {
			int id = Integer.parseInt(new String(hm.get("USERID").toArray()));
			value.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, id));
		}
		return value;
	}

	private void doRead(String query, String key, Object result) {
		ClientAction.totalNumberOfReads.incrementAndGet();
		Integer hcode = computeHashCode(key);
		try {
			payload = cacheclient.iqget(key, hcode);
			if (payload != null) {
				ClientAction.hitCache.incrementAndGet();
				switch (key.charAt(0)) {
				case CoreClient.VIEW_PROFILE:
					CacheUtilities.unMarshallHashMap((HashMap<String, ByteIterator>)result, (byte[])payload, this.deserialize_buffer);
					break;
				case CoreClient.VIEW_PENDING:
				case CoreClient.LIST_FRIEND:
					Vector<String> data = new Vector<String>();
					CacheUtilities.unMarshallVectorOfStrings((byte[])payload, data, this.deserialize_buffer);

					int i = 0;
					Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>) result; 
					for (String profileKey : data) {
						if (limitFriends  && i++ >= MAX_LFRIENDS ) {
							break;
						}
						HashMap<String, ByteIterator> hm = new HashMap<>();
						int profileId = getIDFromKey(profileKey);
						String q = CoreClient.getQuery(CoreClient.VIEW_PROFILE, 0,
								profileId, insertImage);
						doRead(q, profileKey, hm);
						res.add(hm);
					}

					break;
				case CoreClient.FRIEND_COUNT:
				case CoreClient.PENDING_COUNT:
					((StringBuffer)result).append((String)payload);
					break;
				}

				if (verbose ) System.out.println("... Cache Hit!");
				return;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
		}

		// query dbms
		Object data = doReadListener.queryDB(query);
		if (data instanceof Map) {
			ObjectByteIterator.deepCopy((Map<String, ByteIterator>)data, (Map<String, ByteIterator>)result);
		} else if (data instanceof Vector) {
			Vector<HashMap<String, ByteIterator>> v = (Vector<HashMap<String, ByteIterator>>)data;
			Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>)result;
			for (HashMap<String, ByteIterator> hm : v) {
				res.add(hm);
			}
		} else if (data instanceof StringBuffer) {
			((StringBuffer)result).append(data.toString());
		}

		switch (key.charAt(0)) {
		case CoreClient.VIEW_PROFILE:
			payload = CacheUtilities.SerializeHashMap((HashMap<String, ByteIterator>)result);
			break;
		case CoreClient.LIST_FRIEND:
		case CoreClient.VIEW_PENDING:
			Vector<String> res = computeListOfProfileKeys(key, 
					(Vector<HashMap<String, ByteIterator>>) result);
			payload = CacheUtilities.SerializeVectorOfStrings(res);
			break;
		case CoreClient.FRIEND_COUNT:
		case CoreClient.PENDING_COUNT:
			payload = ((StringBuffer)result).toString();
			break;
		}

		try {
			cacheclient.iqset(key, payload, hcode);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		String query = CoreClient.getQuery(CoreClient.LIST_FRIEND,
				requesterID, profileOwnerID, insertImage);
		String key = CoreClient.getIK(CoreClient.LIST_FRIEND, profileOwnerID);
		doRead(query, key, result);
		return 0;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		String query = CoreClient.getQuery(CoreClient.VIEW_PENDING,
				profileOwnerID, insertImage);
		String key = CoreClient.getIK(CoreClient.VIEW_PENDING, profileOwnerID);
		doRead(query, key, results);

		return 0;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		ClientAction.totalNumberOfWrites.incrementAndGet();
		
		String tid = nextTID();
		Set<Integer> codes = new HashSet<Integer>();
		Set<String> keys = new HashSet<String>();
		Boolean b = null;

		String key = CoreClient.getIK(CoreClient.FRIEND_COUNT, inviterID);
		Integer hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqincr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		key = CoreClient.getIK(CoreClient.FRIEND_COUNT, inviteeID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqincr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		key = CoreClient.getIK(CoreClient.PENDING_COUNT, inviteeID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqdecr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		key = CoreClient.getIK(CoreClient.LIST_FRIEND, inviterID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqappend(key, hcode,
					serializeString(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID)), tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		key = CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqappend(key, hcode,
					serializeString(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviterID)), tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		byte[] bytes = null;
		key = CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID);
		hcode = computeHashCode(key);
		while (true) {
			try {
				bytes = (byte[]) cacheclient.quarantineAndRead(tid, key, hcode, false);
				if (bytes == null)
					keys.add(key);
				break;
			} catch (IQException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace(System.out);
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		bytes = doRemoveKeyFromList(bytes, 
				CoreClient.getIK(CoreClient.VIEW_PROFILE, inviterID), 
				inviterID, inviteeID, CoreClient.ACCEPT);

		String dml = CoreClient.getDML(CoreClient.ACCEPT, inviterID, inviteeID);
		
		if (!ASYNC)
			doDMLListener.updateDB(dml);
		else
			sendDML(tid, dml, keys);
	
		try {
			cacheclient.swapAndRelease(key, hcode, bytes);
			cacheclient.commit(tid, codes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	private Integer computeHashCode(String key) {
		String str = key.replaceAll("[^-0-9]+", " ");
		List<String> list = Arrays.asList(str.trim().split(" "));
		int id = 0;
		try {
			id = Integer.parseInt(list.get(0));
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.out.println(key);
			System.exit(-1);
		}
		return id;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		ClientAction.totalNumberOfWrites.incrementAndGet();
		
		String tid = nextTID();
		Set<Integer> codes = new HashSet<Integer>();
		Set<String> keys = new HashSet<String>();
		Boolean b = null;
		
		String key = CoreClient.getIK(CoreClient.PENDING_COUNT, inviteeID);
		Integer hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqdecr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		byte[] bytes = null;
		key = CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID);
		hcode = computeHashCode(key);
		while (true) {
			try {
				bytes = (byte[]) cacheclient.quarantineAndRead(tid, key, hcode, false);
				if (bytes == null)
					keys.add(key);
				break;
			} catch (IQException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			} 
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (bytes != null) {
			bytes = doRemoveKeyFromList(bytes, 
					CoreClient.getIK(CoreClient.VIEW_PROFILE, inviterID), 
					inviterID, inviteeID, CoreClient.REJECT);
		}
		String dml = CoreClient.getDML(CoreClient.REJECT, inviterID,
				inviteeID);
		
		if (!ASYNC)
			doDMLListener.updateDB(dml);
		else
			sendDML(tid, dml, keys);
		
		try {
			cacheclient.swapAndRelease(key, hcode, bytes);
			cacheclient.commit(tid, codes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		ClientAction.totalNumberOfWrites.incrementAndGet();
		
		String dml = CoreClient.getDML(CoreClient.INVITE, inviterID, inviteeID);
		Set<Integer> codes = new HashSet<Integer>();
		Set<String> keys = new HashSet<String>();
		Boolean b = null;

		String tid = nextTID();

		String key = CoreClient.getIK(CoreClient.PENDING_COUNT, inviteeID);
		Integer hcode = computeHashCode(key);
		codes.add(hcode);
		while(true) {
			b = cacheclient.iqincr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}

		String profileInviter = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviterID);
		byte[] data = serializeString(profileInviter);
		key = CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while(true) {
			b = cacheclient.iqappend(key, hcode, data, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		int res;
		if (!ASYNC)
			res = doDMLListener.updateDB(dml);
		else
			res = sendDML(tid, dml, keys);

		try {
			cacheclient.commit(tid, codes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
			return -1;
		}

		return 0;
	}

	private int sendDML(String tid, String dml, Set<String> keys) {		
		String[] ems = dml.split(",");
		int inviter = Integer.parseInt(ems[1]);
		int invitee = Integer.parseInt(ems[2]);		
		int index = (inviter + invitee) % dmlServerConnectionPools.length;
		SocketIO sock = dmlServerConnectionPools[index].getConnection();

		long clientId = cid;
		int len = 4+8+4+tid.length()+4+dml.length()+4;
		for (String key: keys) {
			len += 4 + key.length();
		}
		ByteBuffer buffer = ByteBuffer.allocate(len);
		buffer.putInt(11);
		buffer.putLong(clientId);
		buffer.putInt(tid.length());
		buffer.put(tid.getBytes());
		buffer.putInt(dml.length());
		buffer.put(dml.getBytes());
		buffer.putInt(keys.size());
		for (String key: keys) {
			buffer.putInt(key.length());
			buffer.put(key.getBytes());
		}
		
		try {
			sock.writeBytes(buffer.array());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
			return -1;
		}
		
		int res;
		try {
			byte[] bytes = sock.readBytes();
			buffer = ByteBuffer.wrap(bytes);
			res = buffer.getInt();
			if (res != PacketFactory.CMD_OK)
				System.out.println("Error: send DML result is not CMD_OK");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(System.out);
			return -1;
		}
		
		dmlServerConnectionPools[index].checkIn(sock);
		return res;
	}

	private byte[] serializeString(String profileInviter) {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bos);
		try {
			out.writeInt(profileInviter.length());
			out.write(profileInviter.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				out.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return bos.toByteArray();
	}

	private byte[] doRemoveKeyFromList(byte[] bytes, String key, 
			int inviterID, int inviteeID, char action) {
		if (bytes == null)
			return null;

		Vector<String> pendings = new Vector<String>();
		CacheUtilities.unMarshallVectorOfStrings(bytes, pendings, deserialize_buffer);

		boolean found = false;
		for (String s : pendings) {
			if (s.equals(key)) {
				pendings.remove(s);
				found = true;
				break;
			}
		}

		if (!found)
			System.out.println(String.format("Action %c Cannot find %d of %d %s %s", 
					action, inviterID, inviteeID, key, pendings));

		return CacheUtilities.SerializeVectorOfStrings(pendings);
	}

	private String nextTID() {
		return String.format("%d%d", cid, tid.incrementAndGet());
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return db.viewTopKResources(requesterID, profileOwnerID, k, result);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return db.getCreatedResources(creatorID, result);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		return db.viewCommentOnResource(requesterID, profileOwnerID, 
				resourceID, result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return db.postCommentOnResource(commentCreatorID, resourceCreatorID, 
				resourceID, values);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		return db.delCommentOnResource(resourceCreatorID, 
				resourceID, manipulationID);
	}

	@Override
	public int thawFriendship(int invitorID, int inviteeID) {
		ClientAction.totalNumberOfWrites.incrementAndGet();
		
		String tid = nextTID();
		Set<Integer> codes = new HashSet<Integer>();
		Set<String> keys = new HashSet<String>();
		Boolean b = null;
		
		String key = CoreClient.getIK(CoreClient.FRIEND_COUNT, invitorID);
		Integer hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqdecr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		key = CoreClient.getIK(CoreClient.FRIEND_COUNT, inviteeID);
		hcode = computeHashCode(key);
		codes.add(hcode);
		while (true) {
			b = cacheclient.iqdecr(key, hcode, tid);
			if (b == null) {
				keys.add(key);
				break;
			} else if (b.booleanValue() == true) {
				break;
			}
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		byte[] bytes = null;
		byte[] bytes2 = null;
		String key1 = CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID);
		Integer hcode1 = computeHashCode(key1);
		while (true) {
			try {
				bytes = (byte[]) cacheclient.quarantineAndRead(tid, key1, hcode1, false);
				if (bytes == null)
					keys.add(key1);
				break;
			} catch (IQException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (bytes != null) {
			String profileInvitee = CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID);
			bytes = doRemoveKeyFromList(bytes, profileInvitee, 
					invitorID, inviteeID, CoreClient.THAW);
		}
		
		String key2 = CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID);
		Integer hcode2 = computeHashCode(key2);
		while (true) {
			try {
				bytes2 = (byte[]) cacheclient.quarantineAndRead( tid, key2, hcode2 , false);
				if (bytes2 == null)
					keys.add(key2);
				break;
			} catch (IQException e) {
				// TODO Auto-generated catch block
//				e.printStackTrace();
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		if (bytes2 != null) {
			String profileInviter = CoreClient.getIK(CoreClient.VIEW_PROFILE, invitorID);
			bytes2 = doRemoveKeyFromList(bytes2, profileInviter, 
					invitorID, inviteeID, CoreClient.THAW);
		}

		String dml = CoreClient.getDML(CoreClient.THAW, invitorID, inviteeID);
		if (!ASYNC)
			doDMLListener.updateDB(dml);
		else
			sendDML(tid, dml, keys);

		try {
			cacheclient.swapAndRelease(key1, hcode1, bytes);
			cacheclient.swapAndRelease(key2, hcode2, bytes2);
			cacheclient.commit(tid, codes);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		return db.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return db.CreateFriendship(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {
		db.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		return db.queryPendingFriendshipIds(memberID, pendingIds);
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		return db.queryConfirmedFriendshipIds(memberID, confirmedIds);
	}

	@Override
	public void setProperties(Properties p) {
		// TODO Auto-generated method stub
		this.db.setProperties(p);
	}

	@Override
	public Properties getProperties() {
		// TODO Auto-generated method stub
		return this.db.getProperties();
	}

	@Override
	public void buildIndexes(Properties props) {
		// TODO Auto-generated method stub
		this.db.buildIndexes(props);
	}

	@Override
	public boolean dataAvailable() {
		// TODO Auto-generated method stub
		while (!this.db.dataAvailable())
			;
		return true;
	}

	@Override
	public Connection getConnection() {
		return this.db.getConnection();
	}

}