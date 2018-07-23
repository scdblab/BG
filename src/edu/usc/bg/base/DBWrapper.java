/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import edu.usc.bg.measurements.MyMeasurement;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
	DB _db;
	MyMeasurement _measurements;

	public DBWrapper(DB db)
	{
		_db=db;
		_measurements=MyMeasurement.getMeasurements(Double.parseDouble(getProperties().getProperty(Client.EXPECTED_LATENCY_PROPERTY, Client.EXPECTED_LATENCY_PROPERTY_DEFAULT)));
	}

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public boolean init() throws DBException
	{
		return _db.init();
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup(boolean warmup) throws DBException
	{
		_db.cleanup(warmup);
	}

	/**
	 * Insert an entity in the database. Any field/value pairs in the specified values HashMap will 
	 * be written into the entity with the specified
	 * entityPK as its primary key.
	 *
	 * @param entitySet The name of the entity.
	 * @param entityPK The entity primary key of the entity to insert.
	 * @param values A HashMap of field/value pairs to insert for the entity.
	 * @param insertImage Identifies if images need to be inserted.
	 * @return Zero on success, a non-zero error code on error.
	 */
	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String,ByteIterator> values, boolean insertImage)
	{
		long st=System.nanoTime();
		int res=_db.insertEntity(entitySet,entityPK,values, insertImage);
		long en=System.nanoTime();
		_measurements.measure("INSERT",(int)((en-st)/1000));
		_measurements.reportReturnCode("INSERT",res);
		return res;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		//int res = 0;
		long st=System.nanoTime();
		int res=_db.viewProfile(requesterID, profileOwnerID, result, insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("PROFILE",(int)((en-st)/1000));
		_measurements.reportReturnCode("PROFILE",res);
		return res;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,  boolean insertImage, boolean testMode) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.listFriends(requesterID, profileOwnerID, fields, result, insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("FRIENDS",(int)((en-st)/1000));
		_measurements.reportReturnCode("FRIENDS",res);
		return res;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values,  boolean insertImage, boolean testMode) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.viewFriendReq(profileOwnerID, values,  insertImage, testMode);
		long en=System.nanoTime();
		_measurements.measure("PENDING",(int)((en-st)/1000));
		_measurements.reportReturnCode("PENDING",res);
		return res;
	}

	@Override
	public int acceptFriend(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.acceptFriend(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("ACCEPT",(int)((en-st)/1000));
		_measurements.reportReturnCode("ACCEPT",res);
		return res;
	}

	@Override
	public int rejectFriend(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.rejectFriend(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("REJECT",(int)((en-st)/1000));
		_measurements.reportReturnCode("REJECT",res);
		return res;
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.inviteFriend(invitorID, inviteeID);
		long en=System.nanoTime();
		_measurements.measure("INV",(int)((en-st)/1000));
		_measurements.reportReturnCode("INV",res);
		return res;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.thawFriendship(friendid1, friendid2);
		long en=System.nanoTime();
		_measurements.measure("UNFRIEND",(int)((en-st)/1000));
		_measurements.reportReturnCode("UNFRIEND",res);
		return res;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.viewTopKResources(requesterID, profileOwnerID, k, result);
		long en=System.nanoTime();
		_measurements.measure("GETTOPRES",(int)((en-st)/1000));
		_measurements.reportReturnCode("GETTOPRES",res);
		return res;	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.viewCommentOnResource(requesterID, profileOwnerID, resourceID, result);
		long en=System.nanoTime();
		_measurements.measure("GETRESCOMMENT",(int)((en-st)/1000));
		_measurements.reportReturnCode("GETRESCOMMENT",res);
		return res;	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> values) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.postCommentOnResource(commentCreatorID, profileOwnerID, resourceID, values);
		long en=System.nanoTime();
		_measurements.measure("POSTCOMMENT",(int)((en-st)/1000));
		_measurements.reportReturnCode("POSTCOMMENT",res);
		return res;
	}
	
	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		//int res=0;
		long st=System.nanoTime();
		int res=_db.delCommentOnResource(resourceCreatorID, resourceID, manipulationID);
		long en=System.nanoTime();
		_measurements.measure("DELCOMMENT",(int)((en-st)/1000));
		_measurements.reportReturnCode("DELCOMMENT",res);
		return res;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String > stats = new HashMap<String, String>();
		stats =_db.getInitialStats();
		return stats;
	}

	public int CreateFriendship(int memberA, int memberB){
		long st=System.nanoTime();
		int res=_db.CreateFriendship(memberA, memberB);
		long en=System.nanoTime();
		_measurements.measure("CREATEFRIENDSHIP",(int)((en-st)/1000));
		_measurements.reportReturnCode("CREATEFRIENDSHIP",res);
		return res;
	}
	
//	@Override
//	public int getShortestPathLength(int requesterID, int profileID) {
//		long st=System.nanoTime();
//		int res=_db.getShortestPathLength(requesterID, profileID);
//		long en=System.nanoTime();
//		_measurements.measure("SHORTESTPATH",(int)((en-st)/1000));
//		_measurements.reportReturnCode("SHORTESTPATH",res);
//		return res;
//	}
//
//	@Override
//	public int listCommonFriends(int requesterID, int profileID, int l,
//			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
//			boolean insertImage, boolean testMode) {
//		long st=System.nanoTime();
//		int res=_db.listCommonFriends(requesterID,  profileID, l,
//				fields, result, insertImage, testMode);
//		long en=System.nanoTime();
//		_measurements.measure("COMMONFRNDS",(int)((en-st)/1000));
//		_measurements.reportReturnCode("COMMONFRNDS",res);
//		return res;
//	}
//
//	@Override
//	public int listFriendsOfFriends(int requesterID, int profileID,
//			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
//			boolean insertImage, boolean testMode) {
//		long st=System.nanoTime();
//		int res=_db.listFriendsOfFriends(requesterID, profileID, fields, result, 
//				insertImage, testMode);
//		long en=System.nanoTime();
//		_measurements.measure("FRNDSOFRNDS",(int)((en-st)/1000));
//		_measurements.reportReturnCode("FRNDSOFRNDS",res);
//		return res;
//	}


	@Override
	public void createSchema(Properties props) {	
		_db.createSchema(props);
	}
	
	@Override
	public boolean schemaCreated() {	
		return _db.schemaCreated();
	}
	
	@Override
	public void reconstructSchema() {	
		_db.reconstructSchema();
	}
	
	@Override
	public boolean dataAvailable() {	
		return _db.dataAvailable();
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int res = _db.getCreatedResources(creatorID, result);
		return res;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		int res = _db.queryPendingFriendshipIds(memberID, pendingIds);
		return res;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		int res = _db.queryConfirmedFriendshipIds(memberID, confirmedIds);
		return res;
	}
	
	public void buildIndexes(Properties props){
		_db.buildIndexes(props);
	}


	
}
