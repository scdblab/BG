package MySQL;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import static MySQL.Basic1R1TMemcachedClient.*;

public class Basic1R1TMemcachedMySQLClient extends DB {

	private Basic1R1TMySQLClient mysqlClient = new Basic1R1TMySQLClient();
	private Basic1R1TMemcachedClient memcachedClient = new Basic1R1TMemcachedClient();

	@Override
	public boolean init() throws DBException {
		mysqlClient.setProperties(getProperties());
		memcachedClient.setProperties(getProperties());
		mysqlClient.init();
		memcachedClient.init();
		return super.init();
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		mysqlClient.cleanup(warmup);
		memcachedClient.cleanup(warmup);
		super.cleanup(warmup);
	}

	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values,
			boolean insertImage) {
		mysqlClient.insertEntity(entitySet, entityPK, values, insertImage);
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result,
			boolean insertImage, boolean testMode) {
		int ret = memcachedClient.viewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
		if (ret == SUCCESS) {
			return ret;
		}
		ret = mysqlClient.viewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
		memcachedClient.insertUserProfile(profileOwnerID, result);
		return ret;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID, Set<String> fields,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		int ret = memcachedClient.listFriends(requesterID, profileOwnerID, fields, result, insertImage, testMode);
		if (ret == SUCCESS) {
			return ret;
		}
		ret = mysqlClient.listFriends(requesterID, profileOwnerID, fields, result, insertImage, testMode);
		memcachedClient.insertFriends(profileOwnerID, result);
		return ret;
	}

	@Override
	public int viewFriendReq(int profileOwnerID, Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		int ret = memcachedClient.viewFriendReq(profileOwnerID, results, insertImage, testMode);
		if (ret == SUCCESS) {
			return ret;
		}
		ret = mysqlClient.viewFriendReq(profileOwnerID, results, insertImage, testMode);
		memcachedClient.insertPendingFriends(profileOwnerID, results);
		return ret;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		memcachedClient.acceptFriend(inviterID, inviteeID);
		mysqlClient.acceptFriend(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		memcachedClient.rejectFriend(inviterID, inviteeID);
		mysqlClient.rejectFriend(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		memcachedClient.inviteFriend(inviterID, inviteeID);
		mysqlClient.inviteFriend(inviterID, inviteeID);
		return 0;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int getCreatedResources(int creatorID, Vector<HashMap<String, ByteIterator>> result) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID,
			Vector<HashMap<String, ByteIterator>> result) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID, int manipulationID) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		memcachedClient.thawFriendship(friendid1, friendid2);
		mysqlClient.thawFriendship(friendid1, friendid2);
		return 0;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		return mysqlClient.getInitialStats();
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		mysqlClient.CreateFriendship(friendid1, friendid2);
		return 0;
	}

	@Override
	public void createSchema(Properties props) {
		mysqlClient.createSchema(props);
	}

	@Override
	public int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) {
		throw new IllegalAccessError("Not implemented");
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) {
		throw new IllegalAccessError("Not implemented");
	}

}
