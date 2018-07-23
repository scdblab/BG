package kosar;

import static kosar.CoreClient.enableCache;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import MySQL.AggrAsAttr2R1TMySQLClient;
import kosar.JdbcDBClient;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.dblab.cm.LRU;
import edu.usc.dblab.cm.base.CacheManager;

public class RelaxedConsistencyClient extends DB {
	private static final int NOT_SUCCESS = -1;
	
	private final JdbcDBClient dbclient = new JdbcDBClient();
//	private final AggrAsAttr2R1TMySQLClient dbclient = 
//			new AggrAsAttr2R1TMySQLClient();
	
	private final byte[] unmarshallBuffer = new byte[1024 * 16];
	
	private static final CoreClient coreClient = new CoreClient();
	private static boolean initialized = false;

	@Override
	public void setProperties(Properties p) {
		this.dbclient.setProperties(p);
	}

	@Override
	public Properties getProperties() {
		return this.dbclient.getProperties();
	}

	@Override
	public boolean init() throws DBException {
		// reset counter
		ClientAction.resetStats();
		
		synchronized (coreClient) {
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
				
				coreClient.initialize(p);
				
				if (ClientAction.ASYNC) {
					for (String addr: CoreServerAddr.dmlProcessAddrs)
						AsyncSocketServer.generateSocketPool(addr);
				}
				
				initialized = true;
			}
		}
		while (!this.dbclient.init())
			;
		
		return true;
	}
	
	@Override
	public void cleanup(boolean warmup) throws DBException {
		this.dbclient.cleanup(warmup);
	}

	@Override
	public void buildIndexes(Properties props) {
		this.dbclient.buildIndexes(props);
	}

	@Override
	public boolean schemaCreated() {
		return this.dbclient.schemaCreated();
	}

	@Override
	public void reconstructSchema() {
		this.dbclient.reconstructSchema();
	}

	@Override
	public boolean dataAvailable() {
		while (!this.dbclient.dataAvailable())
			;
		return true;
	}

	@Override
	public int acceptFriend(int invitorID, int inviteeID) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
					iks.add(CoreClient
							.getIK(CoreClient.VIEW_PROFILE, invitorID));
					iks.add(CoreClient
							.getIK(CoreClient.VIEW_PROFILE, inviteeID));

					iks.add(CoreClient
							.getIK(CoreClient.VIEW_PENDING, inviteeID));

					iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID));
					iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

				String dml = CoreClient.getDML(CoreClient.ACCEPT, invitorID,
						inviteeID);
				res = coreClient.doDMLRefresh(this.dbclient, dml, iks, unmarshallBuffer); 

			}
		} else {
			res = this.dbclient.acceptFriend(invitorID, inviteeID);
		}
		return res;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (requesterID >= 0 && profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.VIEW_PROFILE,
						profileOwnerID, profileOwnerID, insertImage);
				String ik = CoreClient.getIK(CoreClient.VIEW_PROFILE,
								profileOwnerID);
				Object value = coreClient.doRead(this.dbclient, query, ik,
						this.unmarshallBuffer);
				if (value != null) {
					synchronized (value) {
						HashMap<String, ByteIterator> retValue = (HashMap<String, ByteIterator>) value;
						ObjectByteIterator.deepCopy(retValue, result);						
					}
					res = 0;
				}
			}
		} else {
			res = this.dbclient.viewProfile(requesterID, profileOwnerID,
					result, insertImage, testMode);
		}
		return res;
	}

	@SuppressWarnings("unchecked")
	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (requesterID >= 0 && profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.LIST_FRIEND,
						requesterID, profileOwnerID, insertImage);
				String ik = "";
					ik = CoreClient.getIK(CoreClient.LIST_FRIEND,
							profileOwnerID);
				Object value = coreClient.doRead(this.dbclient, query, ik,
						unmarshallBuffer);
				if (value != null) {
					synchronized (value) {
						Vector<HashMap<String, ByteIterator>> retValue = (Vector<HashMap<String, ByteIterator>>) value;
						for (HashMap<String, ByteIterator> v : retValue) {
							HashMap<String, ByteIterator> copy = new HashMap<String, ByteIterator>();
							ObjectByteIterator.deepCopy(v, copy);
							result.add(copy);
						}						
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

	@SuppressWarnings("unchecked")
	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (profileOwnerID >= 0) {
				String query = CoreClient.getQuery(CoreClient.VIEW_PENDING,
						profileOwnerID, insertImage);
				String ik = "";
					ik = CoreClient.getIK(CoreClient.VIEW_PENDING,
							profileOwnerID);
				Object value = coreClient.doRead(this.dbclient, query, ik,
						unmarshallBuffer);
				if (value != null) {
					synchronized (value) {
						Vector<HashMap<String, ByteIterator>> retValue = (Vector<HashMap<String, ByteIterator>>) value;
						for (HashMap<String, ByteIterator> v : retValue) {
							HashMap<String, ByteIterator> copy = new HashMap<String, ByteIterator>();
							ObjectByteIterator.deepCopy(v, copy);
							result.add(copy);
						}						
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
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();				
					iks.add(CoreClient
							.getIK(CoreClient.VIEW_PROFILE, inviteeID));
					iks.add(CoreClient
							.getIK(CoreClient.VIEW_PENDING, inviteeID));
				String dml = CoreClient.getDML(CoreClient.REJECT, invitorID,
						inviteeID);
				res = coreClient.doDMLRefresh(this.dbclient, dml, iks, unmarshallBuffer);
			}
		} else {
			res = this.dbclient.rejectFriend(invitorID, inviteeID);
		}
		return res;
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient.getIK(CoreClient.VIEW_PROFILE, inviteeID));
				iks.add(CoreClient.getIK(CoreClient.VIEW_PENDING, inviteeID));

				String dml = CoreClient.getDML(CoreClient.INVITE, 
						invitorID, inviteeID);
				res = coreClient.doDMLRefresh(this.dbclient, dml, iks, unmarshallBuffer);
			}
		} else {
			res = this.dbclient.inviteFriend(invitorID, inviteeID);
		}
		return res;
	}

	@Override
	public int thawFriendship(int invitorID, int inviteeID) {
		int res = NOT_SUCCESS;
		if (enableCache) {
			if (invitorID >= 0 && inviteeID >= 0) {
				Set<String> iks = new HashSet<String>();
				iks.add(CoreClient
						.getIK(CoreClient.VIEW_PROFILE, invitorID));
				iks.add(CoreClient
						.getIK(CoreClient.VIEW_PROFILE, inviteeID));

				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, invitorID));
				iks.add(CoreClient.getIK(CoreClient.LIST_FRIEND, inviteeID));

				String dml = CoreClient.getDML(CoreClient.THAW, invitorID,
						inviteeID);
				res = coreClient.doDMLRefresh(this.dbclient, dml, iks, unmarshallBuffer);

			}
		} else {
			res = this.dbclient.thawFriendship(invitorID, inviteeID);
		}
		return res;

	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		return this.dbclient.insertEntity(entitySet, entityPK, values,
				insertImage);
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		return this.dbclient.viewTopKResources(requesterID, profileOwnerID, k,
				result);
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return this.getCreatedResources(creatorID, result);
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		return this.viewCommentOnResource(requesterID, profileOwnerID,
				resourceID, result);
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		return this.dbclient.postCommentOnResource(commentCreatorID,
				resourceCreatorID, resourceID, values);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		return this.dbclient.delCommentOnResource(resourceCreatorID,
				resourceID, manipulationID);
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		return this.dbclient.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		return this.dbclient.CreateFriendship(friendid1, friendid2);
	}

	@Override
	public void createSchema(Properties props) {
		this.dbclient.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int inviteeid,
			Vector<Integer> pendingIds) {
		return this.dbclient.queryPendingFriendshipIds(inviteeid, pendingIds);
	}

	@Override
	public int queryConfirmedFriendshipIds(int profileId,
			Vector<Integer> confirmedIds) {
		return this.dbclient.queryConfirmedFriendshipIds(profileId,
				confirmedIds);
	}

	@Override
	public Connection getConnection() {
		return this.dbclient.getConnection();
	}
}
