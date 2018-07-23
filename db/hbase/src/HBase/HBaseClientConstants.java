package HBase;

import org.apache.hadoop.hbase.util.Bytes;

public interface HBaseClientConstants {

	public static final String MemberTable = "members";
	public static final String[] MemberCF = { "attributes", "thumbImg", "profilelImg" };
	public static final String MemberSnap = "member_snap";

	public static final String PendingFriendTable = "pending_friends";
	public static final String[] PendingFriendCF = { "pendingFriends"};
	public static final String PendingFriendSnap = "pending_friend_snap";
	
	public static final String ConfirmedFriendTable = "confirmed_friends";
	public static final String[] ConfirmedFriendCF = { "confirmedFriends"};
	public static final String ConfirmedFriendSnap = "confirmed_friend_snap";
	
	public static final String ResourceOwnerTable = "resource_owner";
	public static final String[] ResourceOwnerCF = { "resources" };
	public static final String ResourceOwnerSnap = "resource_owner_snap";
	
	public static final String ResourceTable = "resources";
	public static final String[] ResourceCF = { "resourceAttribute",
			"manipulations" };
	public static final String ResourceSnap = "resource_snap";

	public static final String ManipulationTable = "manipulations";
	public static final String[] ManipulationCF = { "attributes" };
	public static final String ManipulationSnap = "manipulation_snap";
	
	
	public static final byte[] PendingFriendsCount = Bytes.toBytes("pendingcount");
	public static final byte[] ConfirmedFriendsCount = Bytes.toBytes("friendcount");
	public static final byte[] ResourceCount = Bytes.toBytes("resourcecount");
	
	//zookeeper ip
	public static final String ZOOKEEPER_IP = "zookeeperip" ;
	public static final String ZOOKEEPER_IP_DEFAULT =  "10.0.0.180";
	
	//zookeeper client port
	public static final String ZOOKEEPER_PORT = "zookeeperport" ;
	public static final String ZOOKEEPER_PORT_DEFAULT =  "2181";
	
	public static final String USERCOUNT = "usercount";
	public static final String USEROFFSET = "useroffset";


}
