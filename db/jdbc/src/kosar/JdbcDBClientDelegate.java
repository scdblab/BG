package kosar;

import static kosar.CoreClient.enableCache;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import MySQL.AggrAsAttr2R1TMySQLClient;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.dblab.cm.LRU;
import edu.usc.dblab.cm.base.CacheManager;

public class JdbcDBClientDelegate extends DB {

	private final DB dbclient = new AggrAsAttr2R1TMySQLClient();
	//	private final DB dbclient = new JdbcDBClient();

	private static final CoreClient client = new CoreClient();

	private static boolean initialized = false;

	private final byte[] unmarshallBuffer = new byte[1024 * 512];

	private static AtomicInteger numthreads = new AtomicInteger(0);

	@Override
	public void setProperties(Properties p) {
		// TODO Auto-generated method stub
		this.dbclient.setProperties(p);
	}

	@Override
	public Properties getProperties() {
		// TODO Auto-generated method stub
		return this.dbclient.getProperties();
	}

	@Override
	public boolean init() throws DBException {
		int currThreads = numthreads.incrementAndGet();
		ClientAction.resetStats();

		synchronized (client) {
			if (!initialized && CoreClient.enableCache) {
				Properties p = getProperties();

				double cacheSize = Double.parseDouble(p.getProperty("cachesize"));

				// convert from Megabytes to Bytes
				cacheSize *= 1024 * 1024;

				String inspector = p.getProperty("inspector");
				int method = -1;
				switch (inspector) {
				case "jvm":
					method = CacheManager.JVM;
					break;
				case "reflection":
					method = CacheManager.REFLECTION;
					break;
				case "profile":
					method = CacheManager.PROFILE;
					break;
				case "stats":
					method = CacheManager.STATISTIC;
					break;
				case "serialization":
					method = CacheManager.SERIALIZATION;
					break;
				case "hybrid":
					method = CacheManager.UNKNOWN;
				default:
					break;
				}

				if (method == CacheManager.UNKNOWN) {
					double t1 = 0.75;
					double t2 = 0.95;
					CacheHelper.cm = new LRU(cacheSize, t1, t2, 1000);
					System.out.println("T1="+t1);
					System.out.println("T2="+t2);					
				} else {
					CacheHelper.cm = new LRU(cacheSize, method);
				}
				System.out.println("Init LRU Cache with size "+cacheSize+" bytes.");
				System.out.println("Size inspector method: "+inspector);

				client.initialize(p);
				initialized = true; 
			}
		}
		while (!this.dbclient.init())
			;

		try {
			int numWarmupThreads = Integer.parseInt(getProperties().getProperty("warmupthreads"));
			int numThreads = Integer.parseInt(getProperties().getProperty("threadcount"));
			if (currThreads == numWarmupThreads+1) {
				CacheHelper.cm.adjustSize();
				System.out.println("Start At: "+(Runtime.getRuntime().totalMemory() - 
						Runtime.getRuntime().freeMemory()));
			}

			if (currThreads == numWarmupThreads + 1 + numThreads) {
				System.out.println("Curr Heap Size: "+(Runtime.getRuntime().totalMemory() - 
						Runtime.getRuntime().freeMemory()));
				System.out.println("Curr Cache Count: "+CacheHelper.cm.count());
				System.out.println("Curr Cache Size: "+CacheHelper.cm.getCurrentCacheSize());
				System.out.println("Curr cache method: "+CacheHelper.cm.getSizeInspectionMethod());
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}

		return true;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {		
		// TODO Auto-generated method stub
		this.dbclient.cleanup(warmup);
	}

	@Override
	public void buildIndexes(Properties props) {
		// TODO Auto-generated method stub
		this.dbclient.buildIndexes(props);
	}

	@Override
	public boolean schemaCreated() {
		// TODO Auto-generated method stub
		return this.dbclient.schemaCreated();
	}

	@Override
	public void reconstructSchema() {
		// TODO Auto-generated method stub
		this.dbclient.reconstructSchema();
	}

	@Override
	public boolean dataAvailable() {
		// TODO Auto-generated method stub
		while (!this.dbclient.dataAvailable())
			;
		return true;
	}

	@Override
	public int acceptFriend(int invitorID, int inviteeID) {
		int res = -1;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitorID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID));
				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

				String dml = CoreClient.getDML(CoreClient.ACCEPT, invitorID,
						inviteeID);
				res = client.doDML(this.dbclient, dml, iks);

			}
		} else {
			res = this.dbclient.acceptFriend(invitorID, inviteeID);
		}
		return res;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		int res = -1;
		if (enableCache) {
			if (requesterID >= 0 && profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.VIEW_PROFILE,
						profileOwnerID, profileOwnerID, insertImage);
				String ik = CoreClient.getIK(CoreClient.VIEW_PROFILE, 
						profileOwnerID);
				Object value = client.doRead(this.dbclient, query, ik,
						this.unmarshallBuffer);
				if (value != null) {
					HashMap<String, ByteIterator> retValue = (HashMap<String, ByteIterator>) value;
					ObjectByteIterator.deepCopy(retValue, result);
					res = 0;
				}
			}
		} else {
			res = this.dbclient.viewProfile(requesterID, profileOwnerID,
					result, insertImage, testMode);
		}
		return res;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int res = -1;
		if (enableCache) {
			if (requesterID >= 0 && profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.LIST_FRIEND,
						requesterID, profileOwnerID, insertImage);
				String ik = CoreClient.getIK(CoreClient.LIST_FRIEND,
						profileOwnerID);
				Object value = client.doRead(this.dbclient, query, ik,
						unmarshallBuffer);
				if (value != null) {
					Vector<HashMap<String, ByteIterator>> retValue = (Vector<HashMap<String, ByteIterator>>) value;
					for (HashMap<String, ByteIterator> v : retValue) {
						HashMap<String, ByteIterator> copy = new HashMap<String, ByteIterator>();
						ObjectByteIterator.deepCopy(v, copy);
						result.add(copy);
					}
					res = 0;
				}

			}
		} else {
			res = this.dbclient.listFriends(requesterID, profileOwnerID,
					fields, result, insertImage, testMode);
		}
		return res;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		int res = -1;
		if (enableCache) {
			if (profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.VIEW_PENDING,
						profileOwnerID, insertImage);
				String ik = CoreClient.getIK(CoreClient.VIEW_PENDING,
						profileOwnerID);
				Object value = client.doRead(this.dbclient, query, ik,
						unmarshallBuffer);
				if (value != null) {
					Vector<HashMap<String, ByteIterator>> retValue = (Vector<HashMap<String, ByteIterator>>) value;
					for (HashMap<String, ByteIterator> v : retValue) {
						HashMap<String, ByteIterator> copy = new HashMap<String, ByteIterator>();
						ObjectByteIterator.deepCopy(v, copy);
						result.add(copy);
					}
					res = 0;
				}
			}
		} else {
			res = this.dbclient.viewFriendReq(profileOwnerID, result,
					insertImage, testMode);
		}
		return res;
	}

	@Override
	public int rejectFriend(int invitorID, int inviteeID) {
		int res = -1;
		long st = System.nanoTime();
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));

				String dml = CoreClient.getDML(CoreClient.REJECT, invitorID,
						inviteeID);
				res = client.doDML(this.dbclient, dml, iks);

			}
		} else {
			res = this.dbclient.rejectFriend(invitorID, inviteeID);
		}
		return res;
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		int res = -1;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));

				String dml = CoreClient.getDML(CoreClient.INVITE, invitorID,
						inviteeID);
				res = client.doDML(this.dbclient, dml, iks);

			}
		} else {
			res = this.dbclient.inviteFriend(invitorID, inviteeID);
		}
		return res;
	}

	@Override
	public int thawFriendship(int invitorID, int inviteeID) {
		int res = -1;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, invitorID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID));
				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

				String dml = CoreClient.getDML(CoreClient.THAW, invitorID,
						inviteeID);
				res = client.doDML(this.dbclient, dml, iks);

			}
		} else {
			res = this.dbclient.thawFriendship(invitorID, inviteeID);
		}
		return res;

	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		// TODO Auto-generated method stub
		return this.dbclient.insertEntity(entitySet, entityPK, values,
				insertImage);
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return this.dbclient.viewTopKResources(requesterID, profileOwnerID, k,
				result);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return this.getCreatedResources(creatorID, result);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		return this.viewCommentOnResource(requesterID, profileOwnerID,
				resourceID, result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		return this.dbclient.postCommentOnResource(commentCreatorID,
				resourceCreatorID, resourceID, values);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		return this.dbclient.delCommentOnResource(resourceCreatorID,
				resourceID, manipulationID);
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		return this.dbclient.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		return this.dbclient.CreateFriendship(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {
		// TODO Auto-generated method stub
		this.dbclient.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int inviteeid,
			Vector<Integer> pendingIds) {
		// TODO Auto-generated method stub
		return this.dbclient.queryPendingFriendshipIds(inviteeid, pendingIds);
	}

	@Override
	public int queryConfirmedFriendshipIds(int profileId,
			Vector<Integer> confirmedIds) {
		// TODO Auto-generated method stub
		return this.dbclient.queryConfirmedFriendshipIds(profileId,
				confirmedIds);
	}

	@Override
	public Connection getConnection() {
		return this.dbclient.getConnection();
	}
}
