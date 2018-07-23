/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package edu.usc.bg.base;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * A layer for accessing a database to be benchmarked. Each thread in the client
 * will be given its own instance of whatever DB class is to be used in the test.
 * This class should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 * Note that BG does not make any use of the return codes returned by this class.
 * Instead, it keeps a count of the return values and presents them to the user.
 * 
 * The semantics of methods vary from database to database.  
 */
public abstract class DB
{
	/**
	 * Properties for configuring this DB.
	 */
	Properties _p=new Properties();

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_p=p;

	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _p; 
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 * This method should be called once by any thread to start communication with the database.
	 * The code written for this function should initiate the thread's communication with the database.
	 * @return true if the connection to the data store was successful.
	 * 
	 */
	public boolean init() throws DBException
	{
		return true;
	}

	/**
	 * Cleanup any state for this DB.
	 * @param warmup This flag identifies if the thread calling it is in the warm up phase.
	 * In the warm up phase the threads do not issue updates, this phase can be used for warming up caches.
	 * If the warm up is set to true, the cache would not be restarted at the end of the warmup phase
	 * and only the warm up thread's connections to the database will be recycled.
	 * Called once per DB instance; there is one DB instance per client thread.
	 * This method should be called once the thread needs to end its communication with the data store.
	 * The code written for this function should close up the connection of the thread with the database
	 * and clean up the database instance.
	 */
	public void cleanup(boolean warmup) throws DBException
	{
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
	 * using the imagesize parameter. 
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 * The code written for this function call should insert the entity and its attributes.
	 * The code is responsible for inserting the PK and the other attributes in the appropriate order.
	 */
	public abstract int insertEntity(String entitySet, String entityPK, HashMap<String,ByteIterator> values, boolean insertImage);

	
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
	
	public abstract int viewProfile( int requesterID, int profileOwnerID, HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode);

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
	 * should be retrieved and inserted into the result hashmap using the "tpic" key.
	 */
	public abstract int listFriends(int requesterID, int profileOwnerID, Set<String> fields, Vector<HashMap<String,ByteIterator>> result,  boolean insertImage, boolean testMode);
	
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
	public abstract int viewFriendReq(int profileOwnerID, Vector<HashMap<String,ByteIterator>> results, boolean insertImage, boolean testMode);

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
	public abstract int acceptFriend(int inviterID, int inviteeID);
	
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
	
	public abstract int rejectFriend(int inviterID, int inviteeID);

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
	public abstract int inviteFriend(int inviterID, int inviteeID);
	
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
	
	public abstract int viewTopKResources(int requesterID, int profileOwnerID, int k, Vector<HashMap<String,ByteIterator>> result);
	
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
	
	public abstract int getCreatedResources(int creatorID, Vector<HashMap<String,ByteIterator>> result);
	
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
	public abstract int viewCommentOnResource(int requesterID, int profileOwnerID, int resourceID, Vector<HashMap<String,ByteIterator>> result);
	
	
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
	public abstract int postCommentOnResource(int commentCreatorID, int resourceCreatorID, int resourceID, HashMap<String,ByteIterator> values);

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
	public abstract int delCommentOnResource(int resourceCreatorID, int resourceID,int manipulationID);
	
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
	public abstract int thawFriendship(int friendid1, int friendid2);
	
//	
//	/**
//	 * This function is called in the benchmarking phase which is executed with the -t argument.
//	 * 
//	 * Computes the length of the shortest path between the requesterID and the profileID.
//	 * @param requesterID The unique identifier of the first member, this is the member invoking the action.
//	 * @param profileID The unique identifier of the member the requeterID's distance is being computed from.
//	 * @return -1 on error, 0 if there is no path between requesterID and profileID, >0  if there is a path between them (if the two members are
//	 * friends the length of the path is 1).
//	 */
//	public abstract int getShortestPathLength(int requesterID, int profileID);
//	
//	public abstract int viewNewsFeed(int userID, int limit, long start, long end);
	
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
	public abstract HashMap<String, String> getInitialStats();
	
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
	public abstract int CreateFriendship(int friendid1, int friendid2);
	
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
	 * The attributes for each entity set are as follows;
	 * 1) users (userid, username, pw, fname, lname, gender,
		dob, jdate, ldate, address, email, tel, tpic, pic)
		tpic and pic are available if there are images inserted for users.
	 * 2) resources (rid, creatorid,  walluserid, type, body, doc)
	 * 3) manipulations (mid, creatorid, rid, modifierid, timestamp, type, content)
	 */
	public abstract void createSchema(Properties props);
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Queries the inviterid's of pending friendship requests for a member specified by memberID.
	 * @param memberID The unique identifier of the user.
	 * @param pendingIds Is a vector of all the member ids that have created a friendship invitation for memberID.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int queryPendingFriendshipIds(int memberID, Vector<Integer> pendingIds);
	
	/**
	 * This function is called in the benchmarking phase which is executed with the -t argument.
	 * 
	 * Queries the friendids of confirmed friendships for a member specified by memberID.
	 * @param memberID The unique identifier of the user.
	 * @param confirmedIds Is a vector of all the member ids that have a friendship relationship with memberID.
	 * @return Zero on success, a non-zero error code on error.  See this class's description for a discussion of error codes.
	 */
	public abstract int queryConfirmedFriendshipIds(int memberID, Vector<Integer> confirmedIds);

	/**
	 * This function is called in the load with index creation phase which is executed with the -loadindex argument.
	 * 
	 * May be used with data stores that support creation of indexes after the load phase is completed.
	 * @param props The properties of BG.
	 */
	public void buildIndexes(Properties props){
	}
	
	
	/**
	 * This function is called in the -dropfriends phase and checks to see if the schema was created
	 * for all tables
	 * 
	 * 
	 * 
	 */
	public boolean schemaCreated(){
		return true;
	}
	
	
	/**
	 * This function is called in the -dropfriends phase and drops all the friendships
	 * 
	 * 
	 * 
	 */
	public void reconstructSchema(){
	}
	
	
	/**
	 * This function checks to see if the appropriate number of users and resources are available
	 * 
	 * 
	 * 
	 */
	public boolean dataAvailable(){
		return true;
	}
	
	public Connection getConnection() {
		return null;
	}
}
