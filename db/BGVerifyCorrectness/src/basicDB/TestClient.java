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


//fake client with deterministic service time for sanity check
package basicDB;


import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class TestClient extends DB {

	private static int NumServers = 1;
	private static Semaphore Server = new Semaphore(NumServers, true);
	private static int ServiceTime = 1; //in msec 

	public boolean init() throws DBException {
		// initialize MongoDb driver
		if (ServiceTime < 200)
			System.out.println("Warning:  Observed service times will not be accurate.  With 100 msec, expect 10% error.");
		return true;
	}
	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	
	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		try {
			int frndCount = 0;
			int pendCount = 0;
			int resCount = 0;
			result.put("friendcount", new ObjectByteIterator(Integer.toString(frndCount).getBytes())) ;
			result.put("pendingcount", new ObjectByteIterator(Integer.toString(pendCount).getBytes())) ;
			result.put("resourcecount", new ObjectByteIterator(Integer.toString(resCount).getBytes())) ;	

			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		} catch (Exception e) {
			System.out.println(e.toString());
			return -1;
		}
	}

	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> values, boolean insertImage, boolean testMode) {
		if(profileOwnerID < 0)
			return -1;

		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int acceptFriend(int invitorID, int inviteeID) {
		//delete from pending of the invitee
		//add to confirmed of both invitee and invitor
		if(invitorID < 0 || inviteeID < 0)
			return -1;

		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int rejectFriend(int invitorID, int inviteeID) {
		//remove from pending of invitee
		if(invitorID < 0 || inviteeID < 0)
			return -1;

		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int inviteFriend(int invitorID, int inviteeID) {
		//add to pending for the invitee
		if(invitorID < 0 || inviteeID < 0)
			return -1;
		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		try {
			//Do work for 500 msec
			//Get the current time and compute when work is complete
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		} catch (Exception e) {
			System.out.println(e.toString());
			return -1;
		}	
	}

	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;
		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}

	}
	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> commentValues) {
		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;
		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		if(resourceCreatorID < 0 || manipulationID < 0 || resourceID < 0)
			return -1;
		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}


	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		//delete from both their confFriends
		if(friendid1 < 0 || friendid2 < 0)
			return -1;

		try {
			Server.acquire();
			Thread.sleep(ServiceTime);
			Server.release();
			return 0;
		}catch (InterruptedException e) {
			e.printStackTrace(System.out);
			return -1;
		}
	}

	@Override
	public HashMap<String, String> getInitialStats() {

		HashMap<String, String> stats = new HashMap<String, String>();
		stats.put("usercount", "0");
		stats.put("avgfriendsperuser", "0");
		stats.put("avgpendingperuser", "0");
		stats.put("resourcesperuser", "0");			

		return stats;
	}

	@Override
	public int CreateFriendship(int memberA, int memberB) {
		return 0;
	}

	public int queryPendingFriendships(int userOffset, int userCount, HashMap<Integer, Vector<Integer>> pendingFrnds){
		return 0;

	}

	@Override
	public void createSchema(Properties props) {

	}

	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		return 0;
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		return 0;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		return 0;
	}


}