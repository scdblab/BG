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

public class HBaseClient_BVP_BATCH extends DB implements HBaseClientConstants {

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

	@Override
	public boolean init() throws DBException {
		try {
			p = getProperties();
			hbaseConf = HBaseConfiguration.create();
			hbaseConf.set("hbase.zookeeper.quorum", p.getProperty(ZOOKEEPER_IP, ZOOKEEPER_IP_DEFAULT));
			hbaseConf.set("hbase.zookeeper.property.clientPort",p.getProperty(ZOOKEEPER_PORT, ZOOKEEPER_PORT_DEFAULT));
			hAdmin = new HBaseAdmin(hbaseConf);
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
					result.put(key, new ObjectByteIterator(entry.getValue()));
				}
			}
			
			if(insertImage){
				byte[] imageValue = queryResult.getValue(Bytes.toBytes("profilelImg"), Bytes.toBytes("data"));
				result.put("pic", new ObjectByteIterator(imageValue));
			}

			
//			 removed since these count is already included in the member table
			if(hTableResourceOwner == null){
				hTableResourceOwner = new HTable(hbaseConf, ResourceOwnerTable);
			}
			Get getRid = new Get(Bytes.toBytes(profileOwnerID));
			Result rids = hTableResourceOwner.get(getRid);
			Map resourceMap = rids.getFamilyMap(Bytes.toBytes("resources"));
			int numResource = 0;
			if(resourceMap!=null){
				numResource = resourceMap.size(); 
			}
			result.put("resourcecount", new ObjectByteIterator(Bytes.toBytes(String.valueOf(numResource))));

			if(hTableConfirmedFriend ==null){
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}
			Get getConfirmedFriend = new Get(Bytes.toBytes(profileOwnerID));
			Result confirmedFriends = hTableConfirmedFriend.get(getConfirmedFriend);
			int numConfirmedFriends = 0;
			if(confirmedFriends.isEmpty()==false){
				Map map = confirmedFriends.getFamilyMap(Bytes.toBytes("confirmedFriends"));
				if(map!=null) numConfirmedFriends = map.size();
			}
			result.put("friendcount", new ObjectByteIterator(Bytes.toBytes(String.valueOf(numConfirmedFriends))));
			
			if(requesterID == profileOwnerID){
				int numPendingFriends = 0;
				if(hTablePendingFriend ==null){
					hTablePendingFriend = new HTable(hbaseConf, PendingFriendTable);
				}
				Get getPendingFriend = new Get(Bytes.toBytes(profileOwnerID));
				Result pendingFriends = hTablePendingFriend.get(getPendingFriend);
				
				if(pendingFriends.isEmpty()==false){
					Map map = pendingFriends.getFamilyMap(Bytes.toBytes("pendingFriends"));
					if(map!=null) numPendingFriends = map.size();
				}
				result.put("pendingcount", new ObjectByteIterator(Bytes.toBytes(String.valueOf(numPendingFriends))));
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

			// for each friends, get the info
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
								infoMap.put( key, new ObjectByteIterator(entry.getValue()));
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
		// TODO Auto-generated method stub
		
		
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

			// for each friends, get his info
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
							infoMap.put( key, new ObjectByteIterator(entry.getValue()));
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
			
			
		}catch(IOException e){
			e.printStackTrace(System.out);
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

		}catch(IOException e){
			e.printStackTrace(System.out);
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		// TODO Auto-generated method stub

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
			
		} catch (Exception e) {
			e.printStackTrace(System.out);
			return ERROR;
		}

		return SUCCESS;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
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
		// TODO Auto-generated method stub

		return SUCCESS;
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		// TODO Auto-generated method stub
		
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
			

		}catch(IOException e){
			e.printStackTrace(System.out);
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
			ResultScanner memberScanner = hTableMember.getScanner(memberScan);
			int userCount = 0;
			while (memberScanner.next() != null) {
				userCount++;
			}
			result.put("usercount", String.valueOf(userCount));

			// count number of resources per user;
			if (hTableResource == null) {
				hTableResource = new HTable(hbaseConf, ResourceTable);
			}
			Scan resourceScan = new Scan();
			ResultScanner resouceScanner = hTableResource
					.getScanner(resourceScan);
			int resouceCount = 0;
			while (resouceScanner.next() != null) {
				resouceCount++;
			}
			double resouecePerUser = 0;
			if (userCount != 0) {
				resouecePerUser = resouceCount * 1.0 / userCount;
			}
			result.put("resourcesperuser", String.valueOf(resouecePerUser));

			// count number of friend per users.
			int pendingFriendCount = 0;
			int confirmedFriendCount = 0;
			
			if (hTableConfirmedFriend == null) {
				hTableConfirmedFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}
			Scan confirmedFriendScan = new Scan();
			ResultScanner confirmedFriendScanner = hTableConfirmedFriend.getScanner(confirmedFriendScan);
			
			for (Result rs = confirmedFriendScanner.next(); rs != null; rs = confirmedFriendScanner
					.next()) {
				confirmedFriendCount += rs.getFamilyMap(
						Bytes.toBytes("confirmedFriends")).size();
			}
			
			if (hTablePendingFriend == null) {
				hTablePendingFriend = new HTable(hbaseConf, ConfirmedFriendTable);
			}
			Scan pendingFriendScan = new Scan();
			ResultScanner pendingFriendScanner = hTableConfirmedFriend.getScanner(pendingFriendScan);
			
			for (Result rs = pendingFriendScanner.next(); rs != null; rs = pendingFriendScanner
					.next()) {
				pendingFriendCount += rs.getFamilyMap(
						Bytes.toBytes("pendingFriends")).size();
			}
			
			double avgfriendsperuser = 0;
			double avgpendingperuser = 0;
			if (userCount != 0) {
				avgfriendsperuser = confirmedFriendCount * 1.0 / userCount;
				avgpendingperuser = pendingFriendCount * 1.0 / userCount;
			}
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
			
		} catch (IOException e) {
			e.printStackTrace(System.out);
			return ERROR;
		} 

		return retVal;
	}

	@Override
	public void createSchema(Properties props) {
		// TODO Auto-generated method stub
		createTable(MemberTable, MemberCF);
		createTable(PendingFriendTable, PendingFriendCF);
		createTable(ConfirmedFriendTable, ConfirmedFriendCF);
		createTable(ResourceTable, ResourceCF);
		createTable(ResourceOwnerTable, ResourceOwnerCF);
		createTable(ManipulationTable, ManipulationCF);
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
