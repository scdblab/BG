/**                                                                                                                                                                                
 * Copyright (c) 2012 USC Database Laboratory All rights reserved. 
 *
 * Authors:  Jia Li, Sumita Barahmand and Shahram Ghandeharizadeh                                                                                                                            
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

package HBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class HBaseClient_GVP_BATCH extends DB implements HBaseClientConstants {

	public static final int SUCCESS = 0;
	public static final int ERROR = -1;

	private HBaseAdmin hAdmin;
	private HTable hTableMember;
	private HTable hTablePendingFriend;
	private HTable hTableConfirmedFriend;
	private HTable hTableResourceOwner;
	private HTable hTableResource;
	private HTable hTableManipulation;
	private Configuration hbaseConf;
	private Properties p;
	
	private int userCount=0;
	private int userOffset=0;
	private int regionServerCount=0;

	
	/* 
	 * Initialized the HBase Connection, need to set the zookeeperIP and zookeeperPort arguments
	 * for the BGClient. 
	 * Also need to set the usercount and useroffset parameters, which will be used to pre splitting the table into several regions,
	 * to balance the load for all the regionservers.
	 */
	
	@Override
	public boolean init() throws DBException {
		try {
			p = getProperties();
			hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", p.getProperty(ZOOKEEPER_IP, ZOOKEEPER_IP_DEFAULT));
			hbaseConf.set("hbase.zookeeper.property.clientPort",p.getProperty(ZOOKEEPER_PORT, ZOOKEEPER_PORT_DEFAULT));
			userCount = Integer.parseInt(p.getProperty(USERCOUNT));
			userOffset = Integer.parseInt(p.getProperty(USEROFFSET));
			hAdmin = new HBaseAdmin(hbaseConf);
			ClusterStatus status = hAdmin.getClusterStatus();
			regionServerCount = status.getServersSize();
			if(hAdmin.tableExists(MemberTable)) hTableMember = new HTable(hbaseConf, MemberTable);
			if(hAdmin.tableExists(PendingFriendTable)) hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			if(hAdmin.tableExists(ConfirmedFriendTable)) hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			if(hAdmin.tableExists(ResourceTable)) hTableResource = new HTable(hbaseConf, ResourceTable);
			if(hAdmin.tableExists(ResourceOwnerTable)) hTableResourceOwner = new HTable(hbaseConf, ResourceOwnerTable);
			if(hAdmin.tableExists(ManipulationTable)) hTableManipulation = new HTable(hbaseConf, ManipulationTable);

			Logger.getRootLogger().setLevel(Level.WARN);

		} catch (IOException e) {
			e.printStackTrace(System.out);
			return false;
		}
		return true;
	}

	
	/*
	 * close all the hbase connections
	 */
	@Override
	public void cleanup(boolean warmup) throws DBException {
		try {
			if (hAdmin != null)
				hAdmin.close();
			if (hTableMember != null)
				hTableMember.close();
			if (hTablePendingFriend != null)
				hTablePendingFriend.close();
			if (hTableConfirmedFriend != null)
				hTableConfirmedFriend.close();
			if (hTableResource != null)
				hTableResource.close();
			if (hTableResourceOwner != null)
				hTableResourceOwner.close();
			if (hTableManipulation != null)
				hTableManipulation.close();
			if (hbaseConf != null)
				hbaseConf.clear();

		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		// TODO Auto-generated method stub
		if (entitySet == null)
			return -1;
		if (entityPK == null)
			return -1;

		if (entitySet.equalsIgnoreCase("users")) {
			try {
				if (hTableMember == null) {
					hTableMember = new HTable(hbaseConf, MemberTable);
				}

				Put p = new Put(Bytes.toBytes(Integer.parseInt(entityPK)));
				for (Entry<String, ByteIterator> entry : values.entrySet()) {
					if (entry.getKey() != "pic" && entry.getKey() != "tpic") {
						p.add(Bytes.toBytes("attributes"), Bytes.toBytes(entry
								.getKey()), entry.getValue().toArray());
					}
				}
				
				// insert image data
				if (insertImage ){  
					if(values.containsKey("pic")){
						byte[] imageByteArray = values.get("pic").toArray();
						p.add(Bytes.toBytes("profilelImg"), Bytes
								.toBytes("data"), imageByteArray); 
					}
					if(values.containsKey("tpic")){
						byte[] imageByteArray = values.get("tpic").toArray();
						p.add(Bytes.toBytes("thumbImg"), Bytes
								.toBytes("data"), imageByteArray); 
					}
				}


				// initialize three count

				p.add(Bytes.toBytes("attributes"), PendingFriendsCount, Bytes.toBytes(((long)(0))) );
				p.add(Bytes.toBytes("attributes"), ConfirmedFriendsCount, Bytes.toBytes(((long)(0))) );
				p.add(Bytes.toBytes("attributes"), ResourceCount, Bytes.toBytes(((long)(0))) );

				hTableMember.put(p);


			} catch (IOException e) {
				e.printStackTrace(System.out);
				return -1;
			}

		} else if (entitySet.equalsIgnoreCase("resources")) {
			try {
				if (hTableResource == null) {
					hTableResource = new HTable(hbaseConf, ResourceTable);
				}

				byte[] walluserid = Bytes.toBytes(Integer.valueOf(Bytes.toString(values.get("walluserid").toArray()))); 
				// change the id to integer, then change it to byte array
				Put p1 = new Put(Bytes.toBytes(Integer.parseInt(entityPK)));
				for (Entry<String, ByteIterator> entry : values.entrySet()) {
					if (entry.getKey() != "walluserid") {
						p1.add(Bytes.toBytes("resourceAttribute"), Bytes
								.toBytes(entry.getKey()), entry.getValue()
								.toArray());
					} else {
						p1.add(Bytes.toBytes("resourceAttribute"),
								Bytes.toBytes(entry.getKey()), walluserid);
					}
				}
				hTableResource.put(p1);

				if (hTableResourceOwner == null) {
					hTableResourceOwner = new HTable(hbaseConf,
							ResourceOwnerTable);
				}

				Put p2 = new Put(walluserid);
				p2.add(Bytes.toBytes("resources"), Bytes.toBytes(Integer.parseInt(entityPK)),
						Bytes.toBytes(""));
				hTableResourceOwner.put(p2);

				// increase resource count;
				if(hTableMember == null){
					hTableMember = new HTable(hbaseConf, MemberTable);
				}
				Increment resourceCountIncreament = new Increment(walluserid);
				resourceCountIncreament.addColumn(Bytes.toBytes("attributes"), ResourceCount, (long)(1));
				hTableMember.increment(resourceCountIncreament);

			} catch (Exception e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}

		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {

		try{
			if (hTableMember == null) {
				hTableMember = new HTable(hbaseConf, MemberTable);
			}

			Get getInfo = new Get(Bytes.toBytes(profileOwnerID));
			getInfo.addFamily(Bytes.toBytes("attributes"));
			if(insertImage==true){
				getInfo.addFamily(Bytes.toBytes("profilelImg"));
			}
			Result queryResult = hTableMember.get(getInfo);

			// fetch the basic info
			Map<byte[], byte[]> infoMap = queryResult.getFamilyMap(Bytes.toBytes("attributes"));
			for(Entry<byte[], byte[]> entry : infoMap.entrySet()){
				String key = new String(entry.getKey());
				if(key.equals("pw")==false){
					if(key.equals("resourcecount") || key.equals("friendcount") || key.equals("pendingcount")){
						if(key.equals("pendingcount") && requesterID != profileOwnerID) continue;
						result.put(key, new ObjectByteIterator(Bytes.toBytes(String.valueOf(Bytes.toLong(entry.getValue()))))) ;
					}else{
						result.put(key, new ObjectByteIterator(entry.getValue()));
					}
				}
			}

			if(insertImage){
				byte[] imageValue = queryResult.getValue(Bytes.toBytes("profilelImg"), Bytes.toBytes("data"));
				result.put("pic", new ObjectByteIterator(imageValue));
			}
		}catch(IOException e){
			e.printStackTrace(System.out);
		}
		return 0;
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		// TODO Auto-generated method stub

		if(requesterID<0 || profileOwnerID<0) return ERROR;
		try{

			if(hTableConfirmedFriend == null){
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}

			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}


			Get getFriendID = new Get(Bytes.toBytes(profileOwnerID));

			Map<byte[], byte[]> friends = hTableConfirmedFriend.get(getFriendID).getFamilyMap(Bytes.toBytes("confirmedFriends"));

			// for each friends, get the info using batch query
			if(friends!=null){
				List<Get> friendInfoGets = new LinkedList<Get>();
				for(Entry<byte[], byte[]> entry : friends.entrySet()){
					byte[] friendID = entry.getKey();
					Get get = new Get(friendID);
					get.addFamily(Bytes.toBytes("attributes"));
					if(insertImage){
						get.addFamily(Bytes.toBytes("thumbImg"));
					}
					friendInfoGets.add(get);
				}

				Result[] getResults = hTableMember.get(friendInfoGets);
				for(Result friendInfo : getResults){
					byte[] friendID = friendInfo.getRow();
					HashMap<String, ByteIterator> infoMap = new HashMap<String, ByteIterator>();
					infoMap.put("userid", new ObjectByteIterator(friendID));

					if(fields!=null){
						Map<byte[],byte[]> attributeMap = friendInfo.getFamilyMap(Bytes.toBytes("attributes"));
						for(Entry<byte[], byte[]> entry : attributeMap.entrySet()){
							String key = Bytes.toString(entry.getKey());
							if(fields.contains( key ) && !key.equals("pwd")){
								infoMap.put( key, new ObjectByteIterator(entry.getValue()));
							}
						}
						if(insertImage){
							byte[] image = friendInfo.getValue(Bytes.toBytes("thumbImg"), Bytes.toBytes("data"));
							infoMap.put("pic", new ObjectByteIterator(image));
						}
					}else{
						
						Map<byte[],byte[]> attributeMap = friendInfo.getFamilyMap(Bytes.toBytes("attributes"));
						for(Entry<byte[], byte[]> entry : attributeMap.entrySet()){
							String key = Bytes.toString(entry.getKey());
							if(!key.equals("pwd")){
								if(key.equals("resourcecount")==false && key.equals("friendcount")==false && key.equals("pendingcount")==false){
									infoMap.put(key, new ObjectByteIterator(entry.getValue()));
								}
							}
						}
						if(insertImage){
							byte[] image = friendInfo.getValue(Bytes.toBytes("thumbImg"), Bytes.toBytes("data"));
							infoMap.put("pic", new ObjectByteIterator(image));
						}
						
						
					}
					result.add(infoMap);
				}
			}
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}
		return SUCCESS;
		
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		if(profileOwnerID<0) return ERROR;
		try{

			if(hTablePendingFriend == null){
				hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			}

			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}


			Get getFriendID = new Get(Bytes.toBytes(profileOwnerID));

			Map<byte[], byte[]> friends = hTablePendingFriend.get(getFriendID).getFamilyMap(Bytes.toBytes("pendingFriends"));

			// for each friends, get his info using bath query
			if(friends!=null){
				List<Get> friendInfoGets = new LinkedList<Get>();
				for(Entry<byte[], byte[]> entry : friends.entrySet()){
					byte[] friendID = entry.getKey();
					Get get = new Get(friendID);
					get.addFamily(Bytes.toBytes("attributes"));
					if(insertImage){
						get.addFamily(Bytes.toBytes("thumbImg"));
					}
					friendInfoGets.add(get);
				}

				Result[] getResults = hTableMember.get(friendInfoGets);
				for(Result friendInfo : getResults){
					byte[] friendID = friendInfo.getRow();
					HashMap<String, ByteIterator> infoMap = new HashMap<String, ByteIterator>();
					infoMap.put("userid", new ObjectByteIterator(friendID));
						
					Map<byte[],byte[]> attributeMap = friendInfo.getFamilyMap(Bytes.toBytes("attributes"));
					for(Entry<byte[], byte[]> entry : attributeMap.entrySet()){
						String key = Bytes.toString(entry.getKey());
						if(!key.equals("pwd")){
							if(key.equals("resourcecount")==false && key.equals("friendcount")==false && key.equals("pendingcount")==false){
								infoMap.put(key, new ObjectByteIterator(entry.getValue()));
							}
						}
					}
					if(insertImage){
						byte[] image = friendInfo.getValue(Bytes.toBytes("thumbImg"), Bytes.toBytes("data"));
						infoMap.put("pic", new ObjectByteIterator(image));
					}
						
					results.add(infoMap);
				}
			}
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}
		return SUCCESS;
	
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		try{
			if(hTablePendingFriend == null){
				hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			}

			Get checkInvitation = new Get(Bytes.toBytes(inviteeID));
			checkInvitation.addColumn(Bytes.toBytes("pendingFriends"), Bytes.toBytes(inviterID));

			if(hTablePendingFriend.get(checkInvitation).isEmpty()){
				System.err.println("No such invitation " + inviterID + " " + inviteeID);
				return ERROR;
			}


			if(hTableConfirmedFriend == null){
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}

			List<Put> putList = new ArrayList<Put>();
			Put put1 = new Put(Bytes.toBytes(inviterID));
			put1.add( Bytes.toBytes("confirmedFriends"), Bytes.toBytes(inviteeID), Bytes.toBytes(""));
			putList.add(put1);

			Put put2 = new Put(Bytes.toBytes(inviteeID));
			put2.add( Bytes.toBytes("confirmedFriends"), Bytes.toBytes(inviterID), Bytes.toBytes(""));
			putList.add(put2);

			hTableConfirmedFriend.put(putList);

			Delete del = new Delete(Bytes.toBytes(inviteeID));
			del.deleteColumn(Bytes.toBytes("pendingFriends"), Bytes.toBytes(inviterID));
			hTablePendingFriend.delete(del);


			// Update Friend Count
			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}
			Increment inviterIncrement = new Increment(Bytes.toBytes(inviterID));
			inviterIncrement.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(1));

			Increment inviteeIncrement = new Increment(Bytes.toBytes(inviteeID));
			inviteeIncrement.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(1));
			inviteeIncrement.addColumn(Bytes.toBytes("attributes"), PendingFriendsCount, (long)(-1));

			List<Increment> listIncrement = new LinkedList<Increment>();
			listIncrement.add(inviterIncrement);
			listIncrement.add(inviteeIncrement);

			Object[] result = new Object[listIncrement.size()];

			hTableMember.batch(listIncrement, result);

		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {

		if(inviterID<0 || inviteeID<0){
			return ERROR;
		}

		try{
			if(hTablePendingFriend == null){
				hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			}

			Delete del = new Delete(Bytes.toBytes(inviteeID));
			del.deleteColumn(Bytes.toBytes("pendingFriends"), Bytes.toBytes(inviterID));
			hTablePendingFriend.delete(del);

			// update the count
			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}

			Increment inviteeIncrement = new Increment(Bytes.toBytes(inviteeID));
			inviteeIncrement.addColumn(Bytes.toBytes("attributes"), PendingFriendsCount, (long)(-1));
			hTableMember.increment(inviteeIncrement);

		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {

		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
		try {

			if (hTablePendingFriend == null) {
				hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			}
			Put p = new Put(Bytes.toBytes(inviteeID));
			p.add(Bytes.toBytes("pendingFriends"), Bytes.toBytes(inviterID),
					Bytes.toBytes(""));
			hTablePendingFriend.put(p);

			// update the count
			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}
			Increment inviteeIncrement = new Increment(Bytes.toBytes(inviteeID));
			inviteeIncrement.addColumn(Bytes.toBytes("attributes"), PendingFriendsCount, (long)(1));
			hTableMember.increment(inviteeIncrement);

		} catch (Exception e) {
			e.printStackTrace(System.out);
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {

		if(requesterID<0 || profileOwnerID<0){
			return ERROR;
		}
		try{
			if(hTableResourceOwner == null){
				hTableResourceOwner = new HTable(hbaseConf, ResourceOwnerTable);
			}
			if(hTableResource == null){
				hTableResource = new HTable(hbaseConf, ResourceTable);
			}

			Get getResourceID = new Get(Bytes.toBytes(profileOwnerID));
			Result resourceIDs = hTableResourceOwner.get(getResourceID);
			NavigableMap<byte[], byte[]> map = resourceIDs.getFamilyMap(Bytes.toBytes("resources"));
			
			if(map!=null){
				LinkedList<Get> ResourceGets = new LinkedList<Get>();
				int count = 0;
				for(byte[] rid : map.descendingKeySet()){
					if(count>=k) break;
					Get getResourceInfo = new Get(rid);
					getResourceInfo.addFamily(Bytes.toBytes("resourceAttribute"));
					ResourceGets.add(getResourceInfo);
					count++;
				}
				
				Result[] resourceInfos = hTableResource.get(ResourceGets);
				
				for(Result resourceInfo : resourceInfos){
					HashMap<String, ByteIterator> item = new HashMap<String, ByteIterator>();
					item.put("rid", new ObjectByteIterator(resourceInfo.getRow()));
					for(Cell cell : resourceInfo.listCells()){
						item.put(Bytes.toString(cell.getQualifierArray()), new ObjectByteIterator(cell.getValueArray()));
					}
					result.add(item);
				}
			}
		}catch(Exception e){
			e.printStackTrace(System.out);
			return ERROR;
		}
		return SUCCESS;
		
	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		//TODO: To be Implemented
		return SUCCESS;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		
		if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return ERROR;

		try{
			if(hTableResource == null){
				hTableResource = new HTable(hbaseConf, ResourceTable);
			}
			if(hTableManipulation == null){
				hTableManipulation = new HTable(hbaseConf, ManipulationTable);
			}

			Get getManipulationID = new Get(Bytes.toBytes(resourceID));
			getManipulationID.addFamily(Bytes.toBytes("manipulations"));
			Result manipulationIDs = hTableResource.get(getManipulationID);
			CellScanner midScanner = manipulationIDs.cellScanner();

			List<Get> getManipulations = new LinkedList<Get>();
			
			while(midScanner.advance()){
				Cell cell = midScanner.current();
				byte[] mid = cell.getValueArray();			
				
				Get getManipulationInfo = new Get(Bytes.add(Bytes.toBytes(resourceID), mid)); // concatenate two bytes array
				getManipulationInfo.addFamily(Bytes.toBytes("attributes"));
				getManipulations.add(getManipulationInfo);
			}
			
			Result [] manipulationResult = hTableManipulation.get(getManipulations);
			for(Result manipulationInfo : manipulationResult){
				CellScanner manipulationScanner = manipulationInfo.cellScanner();
				HashMap<String, ByteIterator> item = new HashMap<String, ByteIterator>();
				item.put("mid", new ObjectByteIterator(manipulationInfo.getRow()));
				while(manipulationScanner.advance()){
					Cell attribute = manipulationScanner.current();
					item.put(Bytes.toString(attribute.getQualifierArray()), new ObjectByteIterator(attribute.getValueArray()));
				}
				result.add(item);
			}

		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}
		return SUCCESS;
		
	}

	@Override
	public int postCommentOnResource(int commentCreatorID,
			int resourceCreatorID, int resourceID,
			HashMap<String, ByteIterator> values) {
		// TODO Auto-generated method stub
		try{
			if(hTableResource == null){
				hTableResource = new HTable(hbaseConf, ResourceTable);
			}
			if(hTableManipulation == null){
				hTableManipulation = new HTable(hbaseConf, ManipulationTable);
			}
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		try{
			Get checkResource = new Get(Bytes.toBytes(resourceID));
			if(hTableResource.get(checkResource).isEmpty()) return ERROR;
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		try{
			byte[] mid = Bytes.toBytes(Integer.parseInt(Bytes.toString(values.get("mid").toArray())));
			Put putManipulation = new Put(Bytes.add(Bytes.toBytes(resourceID), mid));
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("creatorid"), Bytes.toBytes(resourceCreatorID));
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("rid"), Bytes.toBytes(resourceID));
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("modifierid"), Bytes.toBytes(commentCreatorID));
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("timestamp"), values.get("timestamp").toArray());
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("type"), values.get("type").toArray());
			putManipulation.add(Bytes.toBytes("attributes"), Bytes.toBytes("content"), values.get("content").toArray());
			hTableManipulation.put(putManipulation);

			Put putMid = new Put(Bytes.toBytes(resourceID));
			putMid.add(Bytes.toBytes("manipulations"), mid, Bytes.toBytes(""));
			hTableResource.put(putMid);
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		return 0;
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		try{
			if(hTableResource == null){
				hTableResource = new HTable(hbaseConf, ResourceTable);
			}
			if(hTableManipulation == null){
				hTableManipulation = new HTable(hbaseConf, ManipulationTable);
			}
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		try{
			Delete delMid = new Delete(Bytes.toBytes(resourceID));
			delMid.deleteColumn(Bytes.toBytes("manipulations"), Bytes.toBytes(manipulationID));
			hTableResource.delete(delMid);

			Delete delManipulation = new Delete(Bytes.add(Bytes.toBytes(resourceID), Bytes.toBytes(manipulationID)));
			hTableManipulation.delete(delManipulation);
		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}


		return SUCCESS;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		if (friendid1 < 0 || friendid2 < 0)
			return ERROR;

		try{
			if(hTableConfirmedFriend == null){
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}


			List<Delete> deleteList = new ArrayList<Delete>();

			Delete del1 = new Delete(Bytes.toBytes(friendid1));
			del1.deleteColumn(Bytes.toBytes("confirmedFriends"), Bytes.toBytes(friendid2));
			deleteList.add(del1);

			Delete del2 = new Delete(Bytes.toBytes(friendid2));
			del2.deleteColumn(Bytes.toBytes("confirmedFriends"), Bytes.toBytes(friendid1));
			deleteList.add(del2);

			hTableConfirmedFriend.delete(deleteList);

			// update count
			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}
			Increment Increment1 = new Increment(Bytes.toBytes(friendid1));
			Increment1.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(-1));

			Increment Increment2 = new Increment(Bytes.toBytes(friendid2));
			Increment2.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(-1));

			List<Increment> listIncrement = new LinkedList<Increment>();
			listIncrement.add(Increment1);
			listIncrement.add(Increment2);

			Object[] result = new Object[listIncrement.size()];

			hTableMember.batch(listIncrement, result);

		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public HashMap<String, String> getInitialStats() {
		// TODO Auto-generated method stub
		HashMap<String, String> result = new HashMap<String, String>();
		try {

			// count number of members
			if (hTableMember == null) {
				hTableMember = new HTable(hbaseConf, MemberTable);
			}
			Scan memberScan = new Scan();
			memberScan.addFamily(Bytes.toBytes("attributes"));
			ResultScanner memberScanner = hTableMember.getScanner(memberScan);
			int userCount = 0;
			long resouceCount = 0;
			long pendingCount = 0;
			long confirmedCount = 0;

			Result record = null;
			while ((record = memberScanner.next())!=null) {
				userCount++;
				resouceCount += Bytes.toLong(record.getValue(Bytes.toBytes("attributes"), ResourceCount));
				pendingCount += Bytes.toLong(record.getValue(Bytes.toBytes("attributes"), PendingFriendsCount));
				confirmedCount += Bytes.toLong(record.getValue(Bytes.toBytes("attributes"), ConfirmedFriendsCount));
			}
			result.put("usercount", String.valueOf(userCount));
			double resouecePerUser = 0;
			if (userCount != 0) {
				resouecePerUser = resouceCount * 1.0 / userCount;
			}

			double avgfriendsperuser = 0;
			double avgpendingperuser = 0;
			if (userCount != 0) {
				avgfriendsperuser = confirmedCount * 1.0 / userCount;
				avgpendingperuser = pendingCount * 1.0 / userCount;
			}
			result.put("resourcesperuser", String.valueOf(resouecePerUser));
			result.put("avgfriendsperuser", String.valueOf(avgfriendsperuser));
			result.put("avgpendingperuser", String.valueOf(avgpendingperuser));

			return result;


		} catch (Exception e) {
			e.printStackTrace(System.out);
			return null;
		}
	}

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		// TODO Auto-generated method stub
		int retVal = SUCCESS;
		if (friendid1 < 0 || friendid2 < 0)
			return ERROR;

		try {
			if (hTableConfirmedFriend == null) {
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}
			Put p1 = new Put(Bytes.toBytes(friendid1));
			p1.add(Bytes.toBytes("confirmedFriends"), Bytes.toBytes(friendid2),
					Bytes.toBytes(""));
			hTableConfirmedFriend.put(p1);

			Put p2 = new Put(Bytes.toBytes(friendid2));
			p2.add(Bytes.toBytes("confirmedFriends"), Bytes.toBytes(friendid1),
					Bytes.toBytes(""));
			hTableConfirmedFriend.put(p2);

			//update count
			if(hTableMember == null){
				hTableMember = new HTable(hbaseConf, MemberTable);
			}
			Increment Increment1 = new Increment(Bytes.toBytes(friendid1));
			Increment1.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(1));
			//			hTableMember.increment(Increment1);

			Increment Increment2 = new Increment(Bytes.toBytes(friendid2));
			Increment2.addColumn(Bytes.toBytes("attributes"), ConfirmedFriendsCount, (long)(1));
			//			hTableMember.increment(Increment2);

			List<Increment> listIncrement = new LinkedList<Increment>();
			listIncrement.add(Increment1);
			listIncrement.add(Increment2);

			Object[] result = new Object[listIncrement.size()];
			hTableMember.batch(listIncrement, result);




		} catch (IOException e) {
			e.printStackTrace(System.out);
			return ERROR;
		} catch (InterruptedException e) {
			e.printStackTrace();
			return ERROR;
		} 

		return retVal;
	}

	@Override
	public void createSchema(Properties props) {

		// pre split the member table, confirmed_friends table, pending_friends table 
		// to make them distributed evenly among all the regionservers.
		// have not implemented the pre-splitting for the resources and manipulations.
		createSpanningTable(MemberTable, MemberCF, userOffset,userCount, regionServerCount);
		createSpanningTable(PendingFriendTable, PendingFriendCF,userOffset, userCount, regionServerCount);
		createSpanningTable(ConfirmedFriendTable, ConfirmedFriendCF, userOffset, userCount, regionServerCount);
		createTable(ResourceTable, ResourceCF);
		createTable(ResourceOwnerTable, ResourceOwnerCF);
		createTable(ManipulationTable, ManipulationCF);
	}
	
	public void createSpanningTable(String tablename, String[] cfs, int start, int end, int numRegions){
		try {
			if (hAdmin.tableExists(tablename)) {
				hAdmin.disableTable(tablename);
				hAdmin.deleteTable(tablename);
			}

			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			
			if(numRegions>1){
				byte[][] split = new byte[numRegions-1][];
				int step = (end-start)/numRegions;
				for(int i=1; i<=numRegions-1; i++){
					split[i-1] = Bytes.toBytes(start + i*step);
				}
				
				hAdmin.createTable(tableDesc, split);
			}else{
				hAdmin.createTable(tableDesc);
			}
			
			
			
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createTable(String tablename, String[] cfs) {
		try {
			if (hAdmin.tableExists(tablename)) {
				hAdmin.disableTable(tablename);
				hAdmin.deleteTable(tablename);
			}

			HTableDescriptor tableDesc = new HTableDescriptor(
					TableName.valueOf(tablename));
			for (int i = 0; i < cfs.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
			}
			hAdmin.createTable(tableDesc);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {

		if (memberID < 0)
			return ERROR;

		try {
			if (hTablePendingFriend == null) {
				hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
			}
			Get get = new Get(Bytes.toBytes(memberID));
			get.addFamily(Bytes.toBytes("pendingFriends"));
			Result rs = hTablePendingFriend.get(get);
			for (Cell cell : rs.listCells()) {
				pendingIds.add(Bytes.toInt(cell.getQualifierArray()));
			}

		} catch (IOException e) {
			e.printStackTrace(System.out);
			return ERROR;
		}

		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {

		if (memberID < 0)
			return ERROR;

		try {
			if (hTableConfirmedFriend == null) {
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}
			Get get = new Get(Bytes.toBytes(memberID));
			get.addFamily(Bytes.toBytes("confirmedFriends"));
			Result rs = hTableConfirmedFriend.get(get);
			for (Cell cell : rs.listCells()) {
				confirmedIds.add(Bytes.toInt(cell.getQualifierArray()));
			}

		} catch (IOException e) {
			e.printStackTrace(System.out);
			return ERROR;
		}
		return 0;
	}
}
