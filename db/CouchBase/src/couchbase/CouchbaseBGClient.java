package couchbase;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
//import javax.xml.bind.DatatypeConverter;

import java.lang.Thread.*;

import java.lang.reflect.Type;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.base.StringByteIterator;

import com.couchbase.client.*;
import com.couchbase.client.protocol.views.*;
import net.spy.memcached.internal.OperationFuture;

import net.spy.memcached.PersistTo;
import net.spy.memcached.compat.log.*;
import com.google.gson.*;
import com.google.gson.reflect.TypeToken;



public class CouchbaseBGClient extends DB implements CouchbaseBGClientConstant {

	List<URI> uris = new LinkedList<URI>();
	boolean initialized = false;
	CouchbaseClient client = null;
	CouchbaseClient user_client = null;
	CouchbaseClient res_client = null;
	CouchbaseClient manip_client = null;
	CouchbaseClient photo_client = null;
	private Properties props;
	static final String DEFAULT_PROP = "";
	Gson gson = null;
	static final String KEY_CONF_FRIEND = "ConfFriend";
	static final String KEY_PEND_FRIEND = "PendFriend";

	static class UserObj {
		int userid;
		String username;
	}

	public boolean init() throws DBException {
		System.out.println("in INIT **************.");
		File file = new File("err.txt");
		FileOutputStream fos;
		try {
			fos = new FileOutputStream(file);
			PrintStream ps = new PrintStream(fos);
			System.setErr(ps);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (initialized) {
			return true;
		}
		props = getProperties();
		String url = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String bucket = props.getProperty(CONNECTION_BUCKET, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);

		gson = new Gson();

		try {
			uris.add(URI.create(url));

			CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
			cfb.setOpTimeout(20000); // wait up to 10 seconds for an operation to succeed
			//cfb.setOpQueueMaxBlockTime(5000); // wait up to 5 seconds when trying to enqueue an operation
			user_client = new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "users", ""));
			res_client = new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "resources", ""));
			manip_client = new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "manipulation", ""));
			photo_client = new CouchbaseClient(cfb.buildCouchbaseConnection(uris, "photos", ""));

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		initialized = true;
		return true;
	
	}

	public void cleanup(boolean warmup) {
		System.out.println("shutdown coucebase client connection");
		user_client.shutdown(1, TimeUnit.SECONDS);
		res_client.shutdown(1, TimeUnit.SECONDS);
		manip_client.shutdown(1, TimeUnit.SECONDS);
		photo_client.shutdown(1, TimeUnit.SECONDS);
		initialized = false;
	}

	/**
	 * This function is called in the load phase which is executed using the -load or -loadindex argument.
	 * It is used for inserting users and resources.
	 * Any field/value pairs in the values HashMap for an entity will be written 
	 * into the specified entity set with the specified entity key.
	 *
	 * @param entitySet The name of the entity set with the following two possible values:  users and resources.
	 * BG passes these values in lower case.  The implementation may manipulate the case to tailor it for the purposes of a data store.
	 * @param entityPK The primary key of the entity to insert.
	 * @param values A HashMap of field/value pairs to insert for the entity, these pairs are the other attributes for an entity and their values.
	 * The profile image is identified with the "pic" key attribute and the thumbnail image is identified with the "tpic" key attribute.
	 * @param insertImage Identifies if images should be inserted for users.
	 * if set to true the code should populate each entity with an image; the size of the image is specified
	 * using the imageSize parameter. 
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * The code written for this function call should insert the entity and its attributes.
	 * The code is responsible for inserting the PK and the other attributes in the appropriate order.
	 */
	public int insertEntity(String entitySet, String entityPK, HashMap<String,ByteIterator> values, boolean insertImage) 
	{
		try {
			CouchbaseClient client = getClient(entitySet);

			HashMap<String, String> info = new HashMap<String, String>();
			for (String k : values.keySet()) {
				if (! (k.toString().equalsIgnoreCase("pic") || k.toString().equalsIgnoreCase("tpic"))) {
					info.put(k, values.get(k).toString());
				}
			}

			ArrayList<String> conf_arr = new ArrayList<String>();
			String conf_str = gson.toJson(conf_arr);
			if (entitySet.equalsIgnoreCase("users")) {
				info.put(KEY_CONF_FRIEND, conf_str);
				info.put(KEY_PEND_FRIEND, conf_str);
			}

			String json_str = gson.toJson(info);

			OperationFuture<Boolean> setOp = client.set(entityPK.toString(), EXP_TIME, json_str); 
			if(!setOp.get().booleanValue()) {
				System.out.println("insertEntity failed: bucket: " + entitySet + "entity: " + entityPK);
				return -1;
			}

			if(entitySet.equalsIgnoreCase("users") && insertImage) {
				byte[] pic = ((ObjectByteIterator)values.get("pic")).toArray();
				byte[] tpic = ((ObjectByteIterator)values.get("tpic")).toArray();


				setOp = photo_client.set("tpic"+entityPK, EXP_TIME, tpic);

				if(!setOp.get().booleanValue()) {
					System.out.println("insertphoto tpic failed: bucket: " + entitySet + "entity: " + entityPK);
					System.out.println("err info: " + setOp.get());
					return -1;
				}

				setOp = photo_client.set("pic"+entityPK, EXP_TIME, pic);
				//setOp = photo_client.set("pic"+entityPK, EXP_TIME, pic, PersistTo.MASTER);
				if(!setOp.get().booleanValue()) {
					System.out.println("insertphoto pic failed: bucket: " + entitySet + "entity: " + entityPK);
					System.out.println("err info: " + setOp.get());
					return -1;
				}
			}

		
		} catch (InterruptedException e) {
			e.printStackTrace();
			return -1;
		} catch (ExecutionException e) {
			e.printStackTrace();
			return -1;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE insertEntity " + entitySet + " with key " + entityPK);

		return 0;
	}

	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * Get the profile object for a user. 
	 * @param requesterID Unique identifier for the requester.
	 * @param profileOwnerID unique profile owner's identifier.
	 * @param result A HashMap with all data  returned. These data are different user information within the profile
	 * such as friend count, friend request count, etc.
	 * @param insertImage Identifies if the users have images in the database. If set to true the images for the users will be
	 * retrieved.
	 * @param testMode If set to true images will be retrieved and stored on the file system.
	 * While running benchmarks this field should be set to false.
	 * @return 0 on success a non-zero error code on error.  See this class's description for a discussion of error codes. 
	 * 
	 * The code written for this function retrieves the user's profile details, friendcount (number of friends for that user)
	 *  and resourcecount (number of resources inserted on that user's wall).
	 * In addition if the requesterID is equal to the profileOwnerID, the pendingcount (number of pending friend requests)
	 *  needs to be returned as well.
	 * 
	 * If the insertImage is set to true, the image for the profileOwnerID will be rertrieved.
	 * The insertImage should be set to true only if the user entity has an image in the database.
	 * 
	 * The friendcount, resourcecount, pendingcount should be put into the results HashMap
	 * with the following keys: "friendcount", "resourcecount" and "pendingcount", respectively. 
	 * Lack of these attributes or not returning the attribute keys
	 * in lower case causes BG to raise exceptions.
	 * In addition, all other attributes of the profile need to be added to the result hashmap with the 
	 * attribute names being the keys and the attribute values being the values in the hashmap.
	 * 
	 * If images exist for users, they should be converted to bytearrays and added to the result hashmap.
	 * 
	 */  
	
	public  int viewProfile( int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, 
							boolean insertImage, boolean testMode) 
	{
		if (requesterID < 0 || profileOwnerID < 0) {
			return -1;
		}
		
		try {
			String json_str = (String) user_client.get(Integer.toString(profileOwnerID));
			Type type = new TypeToken<HashMap<String, String>>(){}.getType();

			HashMap<String, String> hmap = gson.fromJson(json_str, type);
			//HashMap<String, String> hmap = gson.fromJson(json_str, new TypeToken<Map<String, String>>(){}.getType());

			String conf_str = hmap.get(KEY_CONF_FRIEND);
			String [] conf_arr = gson.fromJson(conf_str, String[].class);

			String pend_str = hmap.get(KEY_PEND_FRIEND);
			String [] pend_arr = gson.fromJson(pend_str, String[].class);

			hmap.remove(KEY_CONF_FRIEND);
			hmap.remove(KEY_PEND_FRIEND);

			for (String k : hmap.keySet()) {
				result.put(k, new ObjectByteIterator(hmap.get(k).getBytes()));
			}

			result.put("friendcount", new ObjectByteIterator(Integer.toString(conf_arr.length).getBytes()));
			if (requesterID == profileOwnerID)
				result.put("pendingcount", new ObjectByteIterator(Integer.toString(pend_arr.length).getBytes()));

			//TODO		
			View vw = res_client.getView("resources", "by_creator");
			Query qy = new Query();
			qy.setKey("\""+profileOwnerID+"\"");
			ViewResponse res = res_client.query(vw, qy);
			String resource_cnt = "";
			for (ViewRow row : res) {
				resource_cnt = row.getValue();
			}
			result.put("resourcecount", new ObjectByteIterator(resource_cnt.getBytes()));

			if (insertImage) {
				//Map<String, Object> pmap = photo_client.getBulk("pic"+profileOwnerID, "tpic"+profileOwnerID);
				Map<String, Object> pmap = photo_client.getBulk("pic"+profileOwnerID, "tpic"+profileOwnerID);
				result.put("pic", new ObjectByteIterator((byte[])pmap.get("pic"+profileOwnerID)));
				result.put("tpic", new ObjectByteIterator((byte[])pmap.get("tpic"+profileOwnerID)));
			}

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

	
		System.out.println("NOTICE viewProfile ok with " + profileOwnerID);
		return 0;
	}

	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * gets the list of friends for a member.
	 * @param requesterID The unique identifier of the user who wants to view profile owners friends.
	 * @param profileOwnerID The id of the profile for which the friends are listed.
	 * @param fields Contains the attribute names required for each friend. 
	 * This can be set to null to retrieve all the friend information.
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one friend.
	 * @param insertImage If set to true the thumbnail images for the friends will be retrieved from the data store.
	 * @param testMode If set to true the thumbnail images of friends will be written to the file system.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function should retrieve the list of friends for the profileOwnerID.
	 * The information retrieved per friend depends on the fields specified in the fields set. if fields is set to null
	 * all profile information for the friends is retrieved and the result hashmap is populated.
	 * The friend's unique id should be inserted with the "userid" key into the result hashmap. The lack of this attribute or the lack of 
	 * the attribute key in lower case causes BG to raise exceptions.
	 * In addition if the insertImage flag is set to true, the thumbnails for each friend's profile
	 * should be retrieved and inserted into the result hashmap using the "pic" key.
	 */
	public  int listFriends(int requesterID, int profileOwnerID, Set<String> fields, 
				Vector<HashMap<String,ByteIterator>> result,  boolean insertImage, boolean testMode) 
	{
		if (profileOwnerID < 0 || requesterID < 0) { 
			return -1;
		}

		try {
			String uid = Integer.toString(profileOwnerID);
			String user_json = (String)user_client.get(uid);
			if (user_json == "") {
				System.out.println("user not exist: " + uid);
				return 0;
			}

			HashMap<String, String> user_map = getMapFromJson(user_json);
			ArrayList<String> conf_arr = getArrFromJson(user_map.get(KEY_CONF_FRIEND));
			if (conf_arr.size() == 0) {
				return 0;
			}

			Map<String, Object> friends = user_client.getBulk(conf_arr);
			if (fields == null) {
				fields = user_map.keySet();
				fields.remove(KEY_CONF_FRIEND);
				fields.remove(KEY_PEND_FRIEND);
			}

			for (String k : friends.keySet()) {
				String frd_json = (String) friends.get(k);
				HashMap<String, String> frd_map = getMapFromJson(frd_json);
				frd_map.remove(KEY_CONF_FRIEND);
				frd_map.remove(KEY_PEND_FRIEND);
				HashMap<String, ByteIterator> frd_res = new HashMap<String, ByteIterator>();
				frd_res.put("userid", new ObjectByteIterator(k.getBytes()));
				for (String field : fields) {
					frd_res.put(field, new ObjectByteIterator(frd_map.get(field).getBytes()));
				}

				if (insertImage) {
					Map<String, Object> pmap = photo_client.getBulk("pic"+k, "tpic"+k);
					frd_res.put("pic", new ObjectByteIterator((byte[])pmap.get("pic"+k)));
					frd_res.put("tpic", new ObjectByteIterator((byte[])pmap.get("tpic"+k)));
				}

				result.add(frd_res);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		System.out.println("NOTICE listFriends ok with " + profileOwnerID);
		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * gets the list of pending friend requests for a member.
	 * These are the requests that are generated for the profileOwnerID
	 * but have not been accepted or rejected.
	 * @param profileOwnerID The profile owner's unique identifier.
	 * @param results A vector of hashmaps where every hashmap belongs to one inviter.
	 * @param insertImage If set to true the images for the friends will be retrieved from the data store.
	 * @param testMode If set to true the thumbnail images of friends will be written to the file system.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function should retrieve the list of pending invitations for the profileOwnerID's profile,
	 * all profile information for the pending friends is retrieved and the result hashmap is populated.
	 * The unique id of the friend generating the request should be added using the "userid" key to the hasmap. 
	 * The lack of this attribute or the lack of 
	 * the attribute key in lower case causes BG to raise exceptions.
	 * In addition if the insertImage flag is set to true, the thumbnails for each pending friend profile
	 * should be retrieved and inserted into the result hashmap.
	 */
	public  int viewFriendReq(int profileOwnerID, Vector<HashMap<String,ByteIterator>> results, 
								boolean insertImage, boolean testMode) 
	{
		if (profileOwnerID < 0) return -1;

		try {
			String uid = Integer.toString(profileOwnerID);
			String user_json = (String)user_client.get(uid);
			if (user_json == "") {
				System.out.println("user not exist: " + uid);
				return 0;
			}

			HashMap<String, String> user_map = getMapFromJson(user_json);
			ArrayList<String> pend_arr = getArrFromJson(user_map.get(KEY_PEND_FRIEND));
			if (pend_arr.size() == 0) {
				return 0;
			}

			Map<String, Object> friends = user_client.getBulk(pend_arr);
			for (String k : friends.keySet()) {
				String frd_json = (String) friends.get(k);
				HashMap<String, String> frd_map = getMapFromJson(frd_json);
				frd_map.remove(KEY_CONF_FRIEND);
				frd_map.remove(KEY_PEND_FRIEND);
				HashMap<String, ByteIterator> frd_res = new HashMap<String, ByteIterator>();
				frd_res.put("userid", new ObjectByteIterator(k.getBytes()));
				for (String field : frd_map.keySet()) {
					frd_res.put(field, new ObjectByteIterator(frd_map.get(field).getBytes()));
				}

				if (insertImage) {
					Map<String, Object> pmap = photo_client.getBulk("pic"+k, "tpic"+k);
					frd_res.put("pic", new ObjectByteIterator((byte[])pmap.get("pic"+k)));
					frd_res.put("tpic", new ObjectByteIterator((byte[])pmap.get("tpic"+k)));
				}

				results.add(frd_res);
			}
		
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE viewFriendReq ok with " + profileOwnerID);
		return 0;
	}

	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Accepts a pending friend request.
	 * This action can only be done by the invitee.
	 * @param inviterID The unique identifier of the inviter (the user who generates the friend request).
	 * @param inviteeID The unique identifier of the invitee (the person who receives the friend request).
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function should accept the friendship request 
	 * and generate a friendship relationship between inviteeID and inviterID.
	 * The friendship relationship is symmetric, so if A is friends with B, B also should
	 * be friends with A.
	 */
	public  int acceptFriend(int inviterID, int inviteeID) 
	{
		if(inviterID < 0 || inviteeID < 0) {
			return -1;
		}

		try {
			String uid1 = Integer.toString(inviteeID);
			String uid2 = Integer.toString(inviterID);

			String uid1_str = (String)user_client.get(uid1);
			HashMap<String, String> uid1_map = getMapFromJson(uid1_str);

			ArrayList<String> uid1_arr = getArrFromJson(uid1_map.get(KEY_PEND_FRIEND));
			int idx = uid1_arr.indexOf(uid2);
			if (idx == -1) {
				System.out.println("ERROR acceptFriend error , invitee " + inviteeID + " doesnot have inviter " + inviterID + " as pending friend.");
			} else {
				uid1_arr.remove(idx);
				uid1_map.put(KEY_PEND_FRIEND, gson.toJson(uid1_arr));
				OperationFuture<Boolean> op = user_client.set(uid1, EXP_TIME, gson.toJson(uid1_map));
				if (!op.get().booleanValue()) {
					System.out.println("acceptFriend set failed");
					return -1;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		int res = CreateFriendship(inviterID, inviteeID);
		if (res != 0) {
			System.out.println("acceptFriend CreateFriendship failed");
			return -1;
		}

		System.out.println("NOTICE acceptFriend ok with " + inviteeID + " and " + inviterID);
		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Rejects a pending friend request.
	 * This action can only be done by the invitee.
	 * @param inviterID The unique identifier of the inviter (The person who generates the friend request).
	 * @param inviteeID The unique identifier of the invitee (The person who receives the friend request).
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function should remove the friend request generated by 
	 * the inviterID without creating any friendship relationship between the inviterID and the inviteeID.
	 * 
	 */
	
	public  int rejectFriend(int inviterID, int inviteeID) 
	{
		if(inviterID < 0 || inviteeID < 0) {
			return -1;
		}

		String uid1 = Integer.toString(inviteeID);
		String uid2 = Integer.toString(inviterID);
		try {

			String uid1_str = (String)user_client.get(uid1);
			HashMap<String, String> uid1_map = getMapFromJson(uid1_str);

			ArrayList<String> uid1_arr = getArrFromJson(uid1_map.get(KEY_PEND_FRIEND));
			int idx = uid1_arr.indexOf(uid2);
			if (idx == -1) {
				System.out.println("ERROR rejectFriend failed, invitee " + inviteeID + " doesnot have inviter " + inviterID + " as pending friend.");
			} else { 
				uid1_arr.remove(idx);
				uid1_map.put(KEY_PEND_FRIEND, gson.toJson(uid1_arr));
				OperationFuture<Boolean> op = user_client.set(uid1, EXP_TIME, gson.toJson(uid1_map));
				if (!op.get().booleanValue()) {
					System.out.println("acceptFriend set failed");
					return -1;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE rejectFriend ok with " + uid1 + " and " + uid2);
		return 0;
	}

	/**
	 * This function is called both in the benchmarking phase executed with the -t argument and the load phase executed either
	 * using -load or the -loadindex argument.
	 * 
	 * Generates a friend request which can be considered as generating a pending friendship.
	 *
	 * @param inviterID The unique identifier of the inviter (the person who generates the friend request).
	 * @param inviteeID The unique identifier of the invitee (the person who receives the friend request).
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function should generate a friend request invitation which is 
	 * extended by the inviterID to the inviteeID.
	 */
	public  int inviteFriend(int inviterID, int inviteeID) 
	{
		if (inviterID < 0 || inviteeID < 0) {
			return -1;
		}

		try {
			//put inviterID into inviteeID's pending list
			String json = (String) user_client.get(Integer.toString(inviteeID));
			Type type = new TypeToken<HashMap<String, String>>(){}.getType();
			HashMap<String, String> user_map = gson.fromJson(json, type);

			String pendf = user_map.get(KEY_PEND_FRIEND);
			ArrayList<String> pend_arr = gson.fromJson(pendf, ArrayList.class);

			pend_arr.add(Integer.toString(inviterID));
			String pend_new = gson.toJson(pend_arr);
			user_map.put(KEY_PEND_FRIEND, pend_new);

			String json_new = gson.toJson(user_map);
			
			OperationFuture<Boolean> op = user_client.set(Integer.toString(inviteeID), EXP_TIME, json_new);
			if (!op.get().booleanValue()) {
				System.out.println("set user friend failed");
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
		
		System.out.println("NOTICE inviteFriend ok with " + inviterID + " and " + inviteeID);
		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Thaws a friendship. 
	 * @param friendid1 The unique identifier of the person who wants to remove a friend.
	 * @param friendid2 The unique identifier of the friend to be removed.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written in this function terminates the friendship relationship between friendid1 and friendid2.
	 * The friendship relationship should be removed for both friendid1 and friendid2.
	 * The friendship/thaw friendship relationship is symmetric so if A is not friends with B, B also will not be friends with A.
	 * 
	 */
	public  int thawFriendship(int friendid1, int friendid2) 
	{
		if (friendid1 < 0 || friendid2 < 0) {
			return -1;
		}

		String uid1 = Integer.toString(friendid1);
		String uid2 = Integer.toString(friendid2);
		try {

			String uid1_str = (String)user_client.get(uid1);
			HashMap<String, String> uid1_map = getMapFromJson(uid1_str);

			ArrayList<String> uid1_arr = getArrFromJson(uid1_map.get(KEY_CONF_FRIEND));
			int idx = uid1_arr.indexOf(uid2);
			if (idx == -1) {
				System.out.println("ERROR " + uid2+" is not a friend of " + uid1);
			} else {
				uid1_arr.remove(idx);
				uid1_map.put(KEY_CONF_FRIEND, gson.toJson(uid1_arr));
				OperationFuture<Boolean> op = user_client.set(uid1, EXP_TIME, gson.toJson(uid1_map));
				if (!op.get().booleanValue()) {
					System.out.println("thawfriend set uid1 failed: " + uid1);
					return -1;
				}
			}


			String uid2_str = (String)user_client.get(uid2);
			HashMap<String, String> uid2_map = getMapFromJson(uid2_str);

			ArrayList<String> uid2_arr = getArrFromJson(uid2_map.get(KEY_CONF_FRIEND));
			idx = uid2_arr.indexOf(uid1);
			if (idx == -1) {
				System.out.println("ERROR " + uid1+" is not a friend of " + uid2);
			} else {
				uid2_arr.remove(idx);
				uid2_map.put(KEY_CONF_FRIEND, gson.toJson(uid2_arr));
				OperationFuture<Boolean> op = user_client.set(uid2, EXP_TIME, gson.toJson(uid2_map));
				if (!op.get().booleanValue()) {
					System.out.println("thawfrind set uid2 failed: " + uid2);
					return -1;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}


		System.out.println("NOTICE thrawFriend ok with " + uid1 + " and " + uid2);
		return 0;
	}
	
	
	/**
	 * This function is called in the load phase which is executed using the -load or -loadindex argument
	 * Creates a confirmed friendship between friendid1 and friendid2.
	 * @param friendid1 The unique identifier of the first member.
	 * @param friendid2 The unique identifier of the second member.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written in this function generates a friendship relationship between friendid1 and friendid2
	 * The friendship relationship is symmetric, so if A is friends with B, B is also friends with A.
	 */
	public  int CreateFriendship(int friendid1, int friendid2) 
	{
		if (friendid1 < 0 || friendid2 < 0) {
			return -1;
		}

		String uid1 = Integer.toString(friendid1);
		String uid2 = Integer.toString(friendid2);
		try {

			String uid1_str = (String)user_client.get(uid1);
			HashMap<String, String> uid1_map = getMapFromJson(uid1_str);

			ArrayList<String> uid1_arr = getArrFromJson(uid1_map.get(KEY_CONF_FRIEND));
			if (uid1_arr.indexOf(uid2) != -1) {
				System.out.println("ERROR " + uid2+" is already friend of " + uid1);
			} else {
				uid1_arr.add(uid2);
				uid1_map.put(KEY_CONF_FRIEND, gson.toJson(uid1_arr));
				OperationFuture<Boolean> op = user_client.set(uid1, EXP_TIME, gson.toJson(uid1_map));
				if (!op.get().booleanValue()) {
					System.out.println("CreateFriendship set failed");
					return -1;
				}
			}


			String uid2_str = (String)user_client.get(uid2);
			HashMap<String, String> uid2_map = getMapFromJson(uid2_str);

			ArrayList<String> uid2_arr = getArrFromJson(uid2_map.get(KEY_CONF_FRIEND));
			if (uid2_arr.indexOf(uid1) != -1) {
				System.out.println("ERROR " + uid1+" is already friend of " + uid2);
			} else {
				uid2_arr.add(uid1);
				uid2_map.put(KEY_CONF_FRIEND, gson.toJson(uid2_arr));
				OperationFuture<Boolean> op = user_client.set(uid2, EXP_TIME, gson.toJson(uid2_map));
				if (!op.get().booleanValue()) {
					System.out.println("CreateFriendship set failed");
					return -1;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE CreateFriend ok with " + uid1 + " and " + uid2);
		return 0;
	}

	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * gets the top k resources posted on a member's wall.
	 * These can be created by the user on her own wall or by other users on this user's wall.
	 * @param requesterID The unique identifier of the user who wants to view profile owners resources.
	 * @param profileOwnerID The profile owner's unique identifier.
	 * @param k The number of resources requested.
	 * @param result A vector of all the resource entities, every resource entity is a hashmap containing resource attributes.
	 * This hashmap should have the resource id identified by "rid" and the wall user id identified by "walluserid". The lack of these attributes
	 * or not having these keys in lower case may cause BG to raise exceptions.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	
	public  int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String,ByteIterator>> result) 
	{
		if (requesterID < 0 || profileOwnerID < 0 || k < 0) return -1;

		String uid = Integer.toString(profileOwnerID);

		try {
			View view = res_client.getView("resources", "res_walluser");
			Query query = new Query();
			query.setKey("\""+uid+"\"");
			//query.setKey("uid"+uid);
			query.setDescending(true);
			query.setLimit(k);
			query.setIncludeDocs(true);

			ViewResponse res = res_client.query(view, query);

			for (ViewRow row: res) {
				String rid = row.getId();
				String doc = (String) row.getDocument();
				HashMap<String, String> doc_map = getMapFromJson(doc);
				HashMap<String, ByteIterator> hmap = new HashMap<String, ByteIterator>();
				hmap.put("rid", new ObjectByteIterator(rid.getBytes()));
				for (String field: doc_map.keySet()) {
					hmap.put(field, new ObjectByteIterator(doc_map.get(field).getBytes()));
				}

				result.add(hmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE viewTopKresource ok with " + uid);
		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * gets the  resources  created by a user, the created resources may be posted either on 
	 * the user's wall or on another user's wall.
	 * @param creatorID The unique identifier of the user who created the resources.
	 * @param result A vector of all the resource entities, every entity is a hashmap containing the attributes of a resource and their values
	 * without considering the comments on the resource.
	 * Every resource should have a unique identifier which will be copied into the hashmap using the "rid" key and a unique creator which will be copied into the "creatorid" key. Lack of this key in lower case
	 * may cause BG to raise exceptions.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	
	public  int getCreatedResources(int creatorID, Vector<HashMap<String,ByteIterator>> result) 
	{
		if (creatorID < 0) return -1;

		String uid = Integer.toString(creatorID);

		try {
			View view = res_client.getView("resources", "res_creator");
			Query query = new Query();
			query.setKey("\""+uid+"\"");
			query.setDescending(true);
			query.setIncludeDocs(true);

			ViewResponse res = res_client.query(view, query);

			for (ViewRow row: res) {
				String rid = row.getId();
				String doc = (String) row.getDocument();
				HashMap<String, String> doc_map = getMapFromJson(doc);
				HashMap<String, ByteIterator> hmap = new HashMap<String, ByteIterator>();
				hmap.put("rid", new ObjectByteIterator(rid.getBytes()));
				for (String field: doc_map.keySet()) {
					hmap.put(field, new ObjectByteIterator(doc_map.get(field).getBytes()));
				}

				result.add(hmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE getCreatedResources ok with " + uid);
		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * gets the comments for a resource and all the comment's details.
	 * @param requesterID The unique identifier of the user who wants to view the comments posted on the profileOwnerID's resource.
	 * @param profileOwnerID The profile owner's unique identifier (owner of the resource).
	 * @param resourceID The resource's unique identifier.
	 * @param result A vector of all the comment entities for a specific resource, each comment and its details are
	 * specified as a hashmap.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * 
	 * The code written for this function, gets the resourceid of a resource and returns all the comments posted on that resource
	 * and their details. This information should be put into the results Vector.
	 */
	public  int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String,ByteIterator>> result) 
	{
		if (requesterID < 0 ||  profileOwnerID < 0 || resourceID < 0) return -1;

		try {
			View view = manip_client.getView("manipulation", "by_rsc");
			Query query = new Query();
			query.setKey("\""+resourceID+"\"");
			query.setDescending(true);
			query.setIncludeDocs(true);

			ViewResponse res = manip_client.query(view, query);

			for (ViewRow row: res) {
				//String mid = row.getId();
				String doc = (String) row.getDocument();
				HashMap<String, String> doc_map = getMapFromJson(doc);
				HashMap<String, ByteIterator> hmap = new HashMap<String, ByteIterator>();
				//hmap.put("mid", new ObjectByteIterator(rid.getBytes()));
				for (String field: doc_map.keySet()) {
					hmap.put(field, new ObjectByteIterator(doc_map.get(field).getBytes()));
				}

				result.add(hmap);
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE viewCommentOnResource ok with resourceID " + resourceID);
		return 0;
	}
	

	
	/**
	 * This function is called in the benchmarking phase executed with the -t argument.
	 * 
	 * posts/creates a comment on a specific resource.
	 * Every comment created is inserted into the manipulation entity set. 
	 * @param commentCreatorID The unique identifier of the user who is creating the comment.
	 * @param resourceCreatorID The resource creator's unique identifier (owner of the resource).
	 * @param resourceID The resource's unique identifier.
	 * @param values The values for the comment which contains the unique identifier of the comment identified by "mid".
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * The code written in this function, creates a comment on the resource identified with
	 * resourceID and created by profileOwnerID.
	 * 
	 */
	public  int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String,ByteIterator> values) 
	{
		if (commentCreatorID < 0 || resourceCreatorID < 0 || resourceID < 0) return -1;

		try {
			HashMap<String, String> info = new HashMap<String,String>();
			for (String k : values.keySet()) {
				info.put(k, values.get(k).toString());
			}

			info.put("creatorid", Integer.toString(resourceCreatorID));
			info.put("rid", Integer.toString(resourceID));
			info.put("modifierid", Integer.toString(commentCreatorID));
			String json_str = gson.toJson(info);

			String mid = values.get("mid").toString();
			OperationFuture<Boolean> setOp = manip_client.set(mid, EXP_TIME, json_str); 
			if(!setOp.get().booleanValue()) {
				System.out.println("postCommentOnResource failed: resID " + resourceID);
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE postCommentOnResource ok with resourceID " + resourceID);
		return 0;
	}

	/**
	 * This function is called in the benchmarking phase executed with the -t argument.
	 * deletes a specific comment on a specific resource.
	 * @param resourceCreatorID The resource creator's unique identifier (owner of the resource).
	 * @param resourceID The resource's unique identifier.
	 * @param manipulationID The unique identifier of the comment to be deleted.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * The code written in this function, deletes a comment identified by manipulationID on the resource identified with
	 * resourceID and created by the resourceCreatorID.
	 * 
	 */
	public  int delCommentOnResource(int resourceCreatorID, int resourceID,int manipulationID)
	{
		if (manipulationID < 0 || resourceCreatorID < 0 || resourceID < 0) return -1;

		try {
			OperationFuture<Boolean> setOp = manip_client.delete(Integer.toString(manipulationID)); 
			if(!setOp.get().booleanValue()) {
				System.out.println("delCommentOnResource failed: manipulationID " + manipulationID);
				return -1;
			}
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}

		System.out.println("NOTICE delCommentOnResource ok with resourceID " + resourceID);
		return 0;
	}

	/**
	 * This function is called in the load phase which is executed using the -load or -loadindex argument
	 * 
	 * returns DB's initial statistics.
	 * These statistics include number of users, average number of friends per user,
	 * average number of pending friend requests per user, and number of resources per user.
	 * The initial statistics are queried and inserted into a hashmap.
	 * This hashmap should contain the following attributes:
	 * "usercount", "resourcesperuser", "avgfriendsperuser", "avgpendingperuser" 
	 * The lack of these attributes or the lack of the keys in lower case causes BG to raise exceptions.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public  HashMap<String, String> getInitialStats() {
		HashMap<String,String> result = new HashMap<String, String>();
		try {
			//user count
			Map<SocketAddress, Map<String, String>> user_stat = (Map<SocketAddress, Map<String, String>>)user_client.getStats();
			
			int user_cnt = 0;
			for (SocketAddress k : user_stat.keySet()) {
				user_cnt += Integer.parseInt(user_stat.get(k).get("curr_items_tot"));
			}

			result.put("usercount", Integer.toString(user_cnt));
			//avg friend count
			String user_info = (String)user_client.get("0");

			//Type type = new TypeToken<HashMap<String, String>>(){}.getType();
			//HashMap<String, String> hmap = gson.fromJson(user_info, type);
			HashMap<String, String> hmap = gson.fromJson(user_info, HashMap.class);
			String conf_str = hmap.get(KEY_CONF_FRIEND);

			String [] conf_arr = gson.fromJson(conf_str, String[].class);
			result.put("avgfriendsperuser", Integer.toString(conf_arr.length));
			
			String pend_str = hmap.get(KEY_PEND_FRIEND);
			String [] pend_arr = gson.fromJson(pend_str, String[].class);
			result.put("avgpendingperuser", Integer.toString(pend_arr.length));

			//view
			View vw = res_client.getView("resources", "by_creator");
			Query qy = new Query();
			qy.setKey("\""+1+"\"");
			ViewResponse res = res_client.query(vw, qy);
			String resource_cnt = "";
			for (ViewRow row : res) {
				resource_cnt = row.getValue();
			}
			result.put("resourcesperuser", resource_cnt);

		} catch (Exception e) {
			e.printStackTrace();
		}

		return result;
	}
	
	/**
	 * This function is called in the schema creation phase which is executed with the -schema argument.
	 * Creates the data store schema which will then be populated in the load phase.
	 * Depending on the type of datastore, the code for creating index structures may also be provided 
	 * within this function call.
	 * 
	 * @param props The properties of BG.
	 * 
	 * BG dictates a fixed conceptual schema. This schema consists of three entity sets:
	 * users, resources, manipulations
	 * The attributes for each entity set are as follows
	 * 1) users (userid, username, pw, fname, lname, gender,
		dob, jdate, ldate, address, email, tel, tpic, pic)
		tpic and pic are available if there are images inserted for users.
	 * 2) resources (rid, creatorid,  walluserid, type, body, doc)
	 * 3) manipulations (mid, creatorid, rid, modifierid, timestamp, type, content)
	 */
	public  void createSchema(Properties props) {}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Queries the inviterid's of pending friendship requests for a member specified by memberID.
	 * @param memberID The unique identifier of the user.
	 * @param pendingIds Is a vector of all the member ids that have created a friendship invitation for memberID.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public  int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds) 
	{
		if(memberID < 0) return -1;

		String user_json = (String) user_client.get(Integer.toString(memberID));
		if (user_json == "") {
			System.out.println("user not exist: " + memberID);
			return -1;
		}

		HashMap<String, String> user_map = getMapFromJson(user_json);
		ArrayList<String> pend_arr = getArrFromJson(user_map.get(KEY_PEND_FRIEND));
		for (String k : pend_arr) {
			pendingIds.add(Integer.parseInt(k));
		}

		return 0;
	}
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Queries the friendids of confirmed friendships for a member specified by memberID.
	 * @param memberID The unique identifier of the user.
	 * @param confirmedIds Is a vector of all the member ids that have a friendship relationship with memberID.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public  int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds) 
	{
		if(memberID < 0) return -1;

		String user_json = (String) user_client.get(Integer.toString(memberID));
		if (user_json == "") {
			System.out.println("user not exist: " + memberID);
			return -1;
		}

		HashMap<String, String> user_map = getMapFromJson(user_json);
		ArrayList<String> pend_arr = getArrFromJson(user_map.get(KEY_CONF_FRIEND));
		for (String k : pend_arr) {
			confirmedIds.add(Integer.parseInt(k));
		}

		return 0;
	}

	/**
	 * This function is called in the load with index creation phase which is executed with the -loadindex argument.
	 * 
	 * May be used with data stores that support creation of indexes after the load phase is completed.
	 * @param props The properties of BG.
	 */
	public void buildIndexes(Properties props){
	}

	public CouchbaseClient getClient(String bucket) {
		if (bucket.equalsIgnoreCase("users")) {
			return this.user_client;
		}
		if (bucket.equalsIgnoreCase("resources")) {
			return this.res_client;
		}
		if (bucket.equalsIgnoreCase("manipulation")) {
			return this.manip_client;
		}

		System.out.println("invalid bucket name" + bucket);
		return null;
	}

	public HashMap<String, String> getMapFromJson(String json) {
		return gson.fromJson(json, HashMap.class);
	}

	public ArrayList<String> getArrFromJson(String json) {
		return gson.fromJson(json, ArrayList.class);
	}

	@Override
	public int viewNewsFeed(int userID, int limit, long start, long end) {
		// TODO Auto-generated method stub
		return 0;
	}

}
