package KosarIQClient;

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import relational.JdbcDBClient;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class JdbcDBClientDelegate extends DB {

	private final JdbcDBClient dbclient = new JdbcDBClient();

	private static final IQClientWrapper client = new IQClientWrapper();

	private static boolean initialized = false;

	private final byte[] unmarshallBuffer = new byte[1024 * 1024 * 5];

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
		synchronized (client) {
			if (!initialized) {
				client.initialize();
				initialized = true;
			}
		}
		while (!this.dbclient.init())
			;
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
		if (invitorID >= 0 && inviteeID >= 0) {
			Set<String> iks = new HashSet<String>();
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					invitorID, inviteeID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID, invitorID));

			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					invitorID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID));

			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PENDING,
					inviteeID));

			iks.add(IQClientWrapper.getIK(IQClientWrapper.LIST_FRIEND,
					invitorID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.LIST_FRIEND,
					inviteeID));

			String dml = IQClientWrapper.getDML(IQClientWrapper.ACCEPT,
					invitorID, inviteeID);
			res = client.doDML(this.dbclient, dml, iks);

		}
		return res;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		int res = -1;
		if (requesterID >= 0 && profileOwnerID >= 0) {
			String query = IQClientWrapper.getQuery(
					IQClientWrapper.VIEW_PROFILE, requesterID, profileOwnerID,
					insertImage);
			String ik = "";
			if (requesterID == profileOwnerID) {
				ik = IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
						profileOwnerID);
			} else {
				ik = IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
						requesterID, profileOwnerID);
			}
			Object value = client.doRead(this.dbclient, query, ik,
					this.unmarshallBuffer);
			if (value != null) {
				HashMap<String, ByteIterator> retValue = (HashMap<String, ByteIterator>) value;
				ObjectByteIterator.deepCopy(retValue, result);
				res = 0;
			}
		}
		return res;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int res = -1;
		if (requesterID >= 0 && profileOwnerID >= 0) {
			String query = IQClientWrapper.getQuery(
					IQClientWrapper.LIST_FRIEND, requesterID, profileOwnerID,
					insertImage);
			String ik = "";
			ik = IQClientWrapper.getIK(IQClientWrapper.LIST_FRIEND,
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
		return res;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		int res = -1;
		if (profileOwnerID >= 0) {
			String query = IQClientWrapper.getQuery(
					IQClientWrapper.VIEW_PENDING, profileOwnerID, insertImage);
			String ik = "";
			ik = IQClientWrapper.getIK(IQClientWrapper.VIEW_PENDING,
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
		return res;
	}

	@Override
	public int rejectFriend(int invitorID, int inviteeID) {
		int res = -1;
		long st = System.nanoTime();
		if (invitorID >= 0 && inviteeID >= 0) {
			Set<String> iks = new HashSet<String>();
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PENDING,
					inviteeID));
			String dml = IQClientWrapper.getDML(IQClientWrapper.REJECT,
					invitorID, inviteeID);
			res = client.doDML(this.dbclient, dml, iks);

		}
		return res;
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		int res = -1;
		if (invitorID >= 0 && inviteeID >= 0) {
			Set<String> iks = new HashSet<String>();
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PENDING,
					inviteeID));

			String dml = IQClientWrapper.getDML(IQClientWrapper.INVITE,
					invitorID, inviteeID);
			res = client.doDML(this.dbclient, dml, iks);

		}
		return res;
	}

	@Override
	public int thawFriendship(int invitorID, int inviteeID) {
		int res = -1;
		if (invitorID >= 0 && inviteeID >= 0) {
			Set<String> iks = new HashSet<String>();
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					invitorID, inviteeID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID, invitorID));

			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					invitorID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.VIEW_PROFILE,
					inviteeID));

			iks.add(IQClientWrapper.getIK(IQClientWrapper.LIST_FRIEND,
					invitorID));
			iks.add(IQClientWrapper.getIK(IQClientWrapper.LIST_FRIEND,
					inviteeID));

			String dml = IQClientWrapper.getDML(IQClientWrapper.THAW,
					invitorID, inviteeID);
			res = client.doDML(this.dbclient, dml, iks);

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
