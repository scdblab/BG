package kosar;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
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

import kosar.JdbcDBClient;
import MySQL.AggrAsAttr2R1TMySQLClient;

import com.meetup.memcached.IQException;
import com.meetup.memcached.Logger;
import com.meetup.memcached.MemcachedClient;
import com.meetup.memcached.SockIOPool;

import common.CacheUtilities;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;

public class JdbcDBClientCachedInv extends DB {
	private static final String CACHE_POOL_NAME = "JdbcDBClientCached";

	private static final int CACHE_POOL_NUM_CONNECTIONS = 100;

	DB db = new JdbcDBClient();
//	DB db = new AggrAsAttr2R1TMySQLClient();
	
	SimpleDoReadListener doReadListener = new SimpleDoReadListener(db);
	DoDMLListener doDMLListener = new SimpleDoDMLListener(db, null);

	private MemcachedClient cacheclient;

	private static SockIOPool cacheConnectionPool;

	private static String[] servers = new String[] { "10.0.0.220:11211" };
	
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
	}

	private Object payload;

	private byte[] deserialize_buffer = new byte[1024 * 5];

	private boolean verbose = false;

	private boolean insertImage = false;

	private boolean limitFriends = true;

	public static boolean initialized = false;
	public static Semaphore semaphore = new Semaphore(1, true);
	
	public static AtomicCyclicInteger tid = new AtomicCyclicInteger(Integer.MAX_VALUE);
	public static long cid = Utils.genID(10001);
	
	public static AtomicInteger numThreads = new AtomicInteger(0);
	
	@Override
	public boolean init() throws DBException {
		Logger.turnOff();
		numThreads.incrementAndGet();
		
		ClientAction.resetStats();
		
		db.init();
		
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
				
				ClientAction.log();
			}
			
			semaphore.release();
		}
	}
	
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		return db.insertEntity(entitySet, entityPK, values, insertImage);
	}
	
	@Override
	public void reconstructSchema() {
		// TODO Auto-generated method stub
		this.db.reconstructSchema();
	}
	
	@Override
	public boolean schemaCreated() {
		// TODO Auto-generated method stub
		return this.db.schemaCreated();
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		String query = CoreClient.getQuery(CoreClient.VIEW_PROFILE,
				profileOwnerID, profileOwnerID, insertImage);
		String key = CoreClient.getIK(CoreClient.VIEW_PROFILE,
					profileOwnerID);
		doRead(query, key, result);
		return 0;
	}
	
	private int getIDFromKey(String key) {
		String str = key.replaceAll("[^-0-9]+", " ");
		List<String> list = Arrays.asList(str.trim().split(" "));
		return Integer.parseInt(list.get(0));
	}
	
	protected Vector<String> computeListOfProfileKeys(Vector<HashMap<String, ByteIterator>> val) {
		Vector<String> value = new Vector<String>();
		for (HashMap<String, ByteIterator> hm: val) {
			int id = Integer.parseInt(new String(hm.get("USERID").toArray()));
			value.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, id));
		}
		return value;
	}
	
	private int computeHashCode(String key) {
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

	@SuppressWarnings("unchecked")
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
						if (limitFriends && i++ == 10) {
							break;
						}
						HashMap<String, ByteIterator> hm = new HashMap<>();
						int profileId = getIDFromKey(profileKey);
						String q = CoreClient.getQuery(CoreClient.VIEW_PROFILE, profileId,
								profileId, insertImage);						
						doRead(q, profileKey, hm);
						res.add(hm);
					}
					
					break;
				}
			
				if (verbose ) System.out.println("... Cache Hit!");
				return;
			} else {
				if (verbose) System.out.println("... Query DB.");
			}
		} catch (Exception e1) {
			e1.printStackTrace(System.out);
		}
		
		// query dbms
		Object data = doReadListener.queryDB(query);
		if (data instanceof HashMap) {
			Map<String, ByteIterator> dat = (Map<String, ByteIterator>)data;
			Map<String, ByteIterator> res = (Map<String, ByteIterator>)result;
			for (String k: dat.keySet()) {
				res.put(k, dat.get(k));
			}
		} else if (data instanceof Vector) {
			Vector<HashMap<String, ByteIterator>> v = (Vector<HashMap<String, ByteIterator>>)data;
			Vector<HashMap<String, ByteIterator>> res = (Vector<HashMap<String, ByteIterator>>)result;
			for (HashMap<String, ByteIterator> hm : v) {
				res.add(hm);
			}
		}
		
		switch (key.charAt(0)) {
		case CoreClient.VIEW_PROFILE:
			payload = CacheUtilities.SerializeHashMap((HashMap<String, ByteIterator>)result);
			break;
		case CoreClient.LIST_FRIEND:
		case CoreClient.VIEW_PENDING:
			Vector<String> res = computeListOfProfileKeys((Vector<HashMap<String, ByteIterator>>) result);
			payload = CacheUtilities.SerializeVectorOfStrings(res);
			break;
		}
		
		try {
			if (!cacheclient.iqset(key, payload, hcode)) {
				ClientAction.failReleasedReads.incrementAndGet();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IQException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void doWrite(String dml, Set<String> keys) {
		String tid = nextTID();
		for (String key: keys) {
			try {
				cacheclient.quarantineAndRegister(tid, key, computeHashCode(key));
			} catch (Exception e) {
				System.out.println(
						String.format("Could not get Q lease for tid %s key %s",
								tid, key));
				e.printStackTrace(System.out);
			}
		}
		
		int txResult = doDMLListener.updateDB(dml);
		
		Set<Integer> codes = new HashSet<Integer>();
		for (String key: keys) {
			codes.add(computeHashCode(key));
		}
		
		if (txResult == 0)
			try {
				cacheclient.commit(tid, codes);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}
	
	private String nextTID() {
		return String.format("%d%d", cid, tid.incrementAndGet());
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
		Set<String> iks = new HashSet<String>();					
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviterID));
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
		iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));
		iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviterID));
		iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

		String dml = CoreClient.getDML(CoreClient.ACCEPT, inviterID, inviteeID);
		doWrite(dml, iks);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		Set<String> iks = new HashSet<String>();				
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
		iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));
		String dml = CoreClient.getDML(CoreClient.REJECT, inviterID, inviteeID);
		doWrite(dml, iks);
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		Set<String> iks = new HashSet<String>();
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
		iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));

		String dml = CoreClient.getDML(CoreClient.INVITE, inviterID, inviteeID);
		doWrite(dml, iks);
		return 0;
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
		Set<String> iks = new HashSet<String>();
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitorID));
		iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
		iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID));
		iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

		String dml = CoreClient.getDML(CoreClient.THAW, invitorID, inviteeID);
		doWrite(dml, iks);
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