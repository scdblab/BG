package edu.usc.bg.server;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

//import edu.usc.bg.Distribution;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.workloads.CoreWorkload;



/**
 * The TokenCacheWorker is a worker thread that acquires a semaphore
 * dictating that there is work to be done. 
 */
public class TokenWorker extends Thread{
	
	public static DBPool dbPool=null;
	public static int numDBConnections=1000;//101;//10000;//230;
	final public static int TIME_COMMAND=20;
	final public static int LOG_COMMAND=30;
	final public static int DELEGATE_COMMAND=80;
	final public static int DELEGATE_2_COMMAND = 90;
	final public static int DELEGATE_0_COMMAND=81;
	final public static int UPDATE_STATE_COMMAND=50;
	final public static int ACQUIRE_COMMAND=60;
	final public static int RELEASE_COMMAND=70;
	final public static int THAW_FRIENDSHIP_ACTION_CODE=6;
	final public static int INVITE_FRIEND_ACTION_CODE=2;
	final public static int REJECT_FRIEND_ACTION_CODE=3;
	final public static int ACCEPT_FRIEND_ACTION_CODE=4;
	final public static int VIEW_PROFILE_ACTION_CODE=5;
	public final static int LIST_FRIENDS_ACTION_CODE = 7;
	public final static int VIEW_PENDINGS_ACTION_CODE = 8;
	private int _threadId;
	int workerID = -1;
	Semaphore testSem = new Semaphore(0, true);
	//boolean verbose = false;
	Random rand = new Random();
	//	final int DEFAULT_INIT_ARRAY_SIZE = 3;
	public static AtomicLong workCount= new AtomicLong(0); // for testing
	public static CoreWorkload coreWorkload=(CoreWorkload)Client.workload;
	//	public static Workload coreWorkload= Client.workload;
	//DB db = null;
	boolean insertImage = false;
	private long numRequestsProcessed;
	private double maxNumReqInQ=0;
	private double maxDiffWorkers_Req=0;
	
	// log record
	BufferedWriter updateLog = null;
	BufferedWriter readLog = null;
	HashMap<String, Integer> friendshipInfo; // keep a track of all friendids
	// for a user
	HashMap<String, Integer> pendingInfo; // keep a track of all friendids that
// have generated pending request for
// this user
	
	/**
	 * @param workerID
	 * @param threadId the thread id will be added by the {@link Client#threadCount}
	 */
	public TokenWorker(int workerID, int threadId) {
		this.setName("WorkerID "+ workerID);
		this.workerID = workerID;
		setNumRequestsProcessed(0);
		this._threadId = Client.threadCount + threadId;
		friendshipInfo = new HashMap<String, Integer>();
		pendingInfo = new HashMap<String, Integer>();
		try {
		
			int machineid = Client.machineid;
			String dir = Client.logDir;
			//update file
			if (CoreWorkload.enableLogging){
			File ufile = new File(dir+"/update"+machineid+"-"+ _threadId + ".txt");
			FileWriter ufstream = new FileWriter(ufile);
			updateLog = new BufferedWriter(ufstream);
			//read file
			File rfile = new File(dir+"/read"+machineid+"-"+_threadId + ".txt");
			FileWriter rfstream = new FileWriter(rfile);
			readLog = new BufferedWriter(rfstream);
			}
			} catch (IOException e) {
			e.printStackTrace(System.out);
		}
	}
	
//	public void setDB(DB db) {
////		this.db = db;
//		
//	}
	public void setInsertImgProperty(boolean insertImage)
	{
		this.insertImage = insertImage;
		
	}

	public void run() {
		// initialize log records buffer
		StringBuilder updateTestLog = new StringBuilder();
		StringBuilder readTestLog = new StringBuilder();
		int seqID = 0; // needed for determining staleness in
		// granularity of users
		try {
			Client.releaseWorkers.acquire();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			System.out.println("Error: Workers waiting for client workload to be intialized");

			e1.printStackTrace();
		}
		//		long EndTime = (System.currentTimeMillis())+ 10*1000;
		//		boolean flag=true;
		//		while (Client.workload==null)
		//		{
		//			try {
		//				if (System.currentTimeMillis()>EndTime)
		//				{
		//					if (flag)
		//						// print after 10 seconds if workload is null
		//						System.out.println("Worker is waiting for client workload object to intialize");
		//				flag=false;
		//				}
		//				 
		//				sleep(100);
		//			} catch (Exception e) {
		//				// TODO Auto-generated catch block
		//				e.printStackTrace();
		//				System.out.println("Error Worker Sleep");
		//			}
		//		} // end while

		if (coreWorkload==null)
			coreWorkload=(CoreWorkload)Client.workload;
		
		
		
		while(BGServer.ServerWorking) {
			updateTestLog.delete(0, updateTestLog.length());
			readTestLog.delete(0, readTestLog.length());
			
			try {
				//Yaz:Acquire the semaphore for work to be done.
				//				BGServer.requestsSemaphores[workerID].acquire();
				BGServer.requestsSemaphores.get(workerID).acquire();
				
			} catch (InterruptedException e) {
				System.out.println("Error: TokenCacheWorker - Could not acquire Semaphore");
				e.printStackTrace();
			}

			//We put this condition boolean here so TokenCacheWorkers 
			//may close without performing these actions
			
//			if(BGServer.ServerWorking) {
				//Get the token object.  Since objects are put in this data structure
				//before the semaphore is released, there should never be a null pointer exception.
				TokenObject tokenObject = null;
				SocketIO socket = null;
				byte[] requestArray = null;

				//synchronized(BGServer.requestsToProcesdskp[jp8im88h) {
//				tokenObject = (TokenObject) BGServer.requestsToProcess.get(workerID).poll();
				tokenObject = BGServer.requestsToProcess.get(workerID).poll();
				long numReqQ=BGServer.requestsToProcess.get(workerID).size();
				long numWorkersWaiting=BGServer.requestsSemaphores.get(workerID).getQueueLength();
				setMaxNumReqInQ(Math.max(getMaxNumReqInQ(), numReqQ));
				setMaxDiffWorkers_Req(Math.max(getMaxDiffWorkers_Req(), numReqQ-numWorkersWaiting));
//				if(BGServer.requestsToProcess.get(workerID).size()>BGServer.NumThreadsPerSemaphore)
//				{
//					System.out.println("Number of requests exceeds the number of currently available workers: "+BGServer.NumThreadsPerSemaphore+" workers vs "+ BGServer.requestsToProcess.get(workerID).size()+" requests");
//					System.exit(0);
//				}
				//	}
				if(tokenObject != null) {
					socket = tokenObject.getSocket();
					requestArray = tokenObject.getRequestArray();
					int command = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 0, 4)).getInt();
					setNumRequestsProcessed(getNumRequestsProcessed() + 1);
					if (DELEGATE_COMMAND == command || DELEGATE_2_COMMAND == command || DELEGATE_0_COMMAND==command) {
						// delegate action
						delegateWork(socket, requestArray, command, updateTestLog, readTestLog, seqID);
					} else {
						///do work
						doWork(socket, requestArray, updateTestLog, readTestLog, seqID);
					}
					try {
						if(updateTestLog != null && updateTestLog.length() != 0) {
							updateLog.write(updateTestLog.toString());
//							updateLog.flush();
							seqID++;
						} //in warmup phase
						if(readTestLog != null && readTestLog.length() != 0) {
							readLog.write(readTestLog.toString());
//							readLog.flush();
							seqID++;
						} //in warmup phase
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}//end if tokenObject != null
		//	}//end if
		} //end while
//		
//		try {
//			if (Client.BENCHMARKING_MODE==Client.DELEGATE)
//			{
//			db.cleanup(false);
//			this.interrupt();
//			}
//		} catch (DBException e) {
//			e.printStackTrace();
//		}
		
		if (updateLog != null) {
			try {
				updateLog.flush();
				this.updateLog.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (this.readLog != null) {
			try {
				readLog.flush();
				this.readLog.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (BGServer.verbose);
		//	System.out.println("Worker " + workerID + " finished.");
	}

	
	
	
	public void doWork(SocketIO socket, byte[] requestArray, StringBuilder updateLog
			, StringBuilder readLog, int seqID) 
	{

		// switch cmd
		int command = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 0, 4)).getInt();
		int clientId, m1,m2,action,serverId, reqid;
		int actionCode,result;
		RequestParameters r1;
		int i;
		long startTime,endTime,time;
		ByteBuffer bb;
		endTime=System.nanoTime();
		int profilekeyname,keyname,friendCount,pendingCount;
		String actionType;
		switch (command)
		{
		
		case LOG_COMMAND:
			CoreWorkload.readsExist = true;
			actionCode = ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			switch (actionCode)
			{
			case VIEW_PROFILE_ACTION_CODE:
				 profilekeyname=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,8 , 12)).getInt();
				 keyname=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,12 , 16)).getInt();
				startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,16 , 24)).getLong();
				 friendCount=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,24 ,28 )).getInt();
				 pendingCount=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,28 ,32 )).getInt();
				 actionType = "GetProfile";
				if(keyname == profilekeyname) {
					readLog.append("READ,PENDFRND,"+seqID+","+this._threadId+","+profilekeyname+","+startTime+","+endTime+","+pendingCount+ "," + actionType +"\n");
				}
				readLog.append("READ,ACCEPTFRND,"+seqID+","+this._threadId+","+profilekeyname+","+startTime+","+endTime+","+friendCount + "," + actionType +"\n");


				break;
				
			case VIEW_PENDINGS_ACTION_CODE:
				 keyname=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,8 , 12)).getInt();
				 int w=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,24 , 28)).getInt();
				if (CoreWorkload.enableLogging&& w!=1){
				actionType = "GetPendingFriends";
				startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,12 , 20)).getLong();
				pendingCount=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,20 ,24 )).getInt();
				readLog.append("READ,PENDFRND,"+seqID+","+this._threadId+","+keyname+","+startTime+","+endTime+","+pendingCount+ "," + actionType +"\n");
				}
				if (CoreWorkload.lockReads)
					coreWorkload.deactivateUser(keyname);
				break;

			case LIST_FRIENDS_ACTION_CODE:
				actionType = "GetFriends";
				 profilekeyname=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,8 , 12)).getInt();
				startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,12 , 20)).getLong();
				friendCount=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,20 ,24 )).getInt();
				readLog.append("READ,ACCEPTFRND,"+seqID+","+_threadId+","+profilekeyname+","+startTime+","+endTime+","+friendCount+ "," + actionType +"\n");
				break;
			default:
				System.out.println("Unkown Action Code");
					
			
			}
			try {
				socket.writeInt(1);
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				System.out.println("Error sending log response "+ e3.getMessage());
				e3.printStackTrace();
			}
		break; //log command
		case TIME_COMMAND: //  get time
			
			
			if (requestArray.length>4) // increment user reference
			{
				m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 4, 8)).getInt();
//				coreWorkload.incrUserRef(m1);
			}
	
			try {
				socket.writeLong(System.nanoTime());
			} catch (IOException e) {
				// TODO Auto-generated catch block
			
				e.printStackTrace();
			}
		



			break;
		case UPDATE_STATE_COMMAND: // update state and generate log records no rele
			actionCode = ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			m1 =ByteBuffer.wrap(Arrays.copyOfRange(requestArray,8 , 12)).getInt();
			m2 =ByteBuffer.wrap(Arrays.copyOfRange(requestArray,12 , 16)).getInt();
			int numFriendsForOtherUserTillNow = 0;
//			if(friendshipInfo.get(m1)!= null){
//				numFriendsForOtherUserTillNow = friendshipInfo.get(m1);
//			}
//			friendshipInfo.put(Integer.toString(m1), (numFriendsForOtherUserTillNow+1));
			
			if(CoreWorkload.enableLogging){
				if (actionCode==TokenWorker.ACCEPT_FRIEND_ACTION_CODE)
				{
					startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,16 , 24)).getLong();
					actionType = "AcceptFriend";
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+_threadId+","+m1+","+startTime+","+endTime+","+(numFriendsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
					CoreWorkload.updatesExist=true;
				}
				
			}
			acceptRejectInvitationUpdateInviterState(actionCode, m1, m2);
			
			try {
				socket.writeInt(1);
			} catch (IOException e3) {
				// TODO Auto-generated catch block
				e3.printStackTrace();
			} // send confirm
			break;
		case 61: // command acquire 61, member id
			m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			result=0;
			//			result=coreWorkload.activateUser(m1);
			socket.sendValue(result);


			break;
		case ACQUIRE_COMMAND: //s command acquire 60,action code, member.
			actionCode=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
			i=0;
			switch (actionCode)
			{
			case VIEW_PENDINGS_ACTION_CODE:
				m1=coreWorkload.activateUser(m1);
				bb = ByteBuffer.allocate(12);
				bb.putInt(m1);
				bb.putLong(System.nanoTime());
				try {
					socket.writeBytes(bb.array());
//					System.out.println("sending array"+bb.array().length);
				} catch (IOException e4) {
					// TODO Auto-generated catch block
					e4.printStackTrace();
				}
				bb.clear();
				
				break;
			case VIEW_PROFILE_ACTION_CODE:
			case LIST_FRIENDS_ACTION_CODE:
				try {
					
					m1=coreWorkload.activateUser(m1);
					
					socket.writeLong(m1);
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
				break;
			case REJECT_FRIEND_ACTION_CODE: // reject Friend
			case ACCEPT_FRIEND_ACTION_CODE:
				//  sends command,action code,invitee

				int []inviter_invitee=acquireAcceptRejectFriend_invitee(m1);
				bb = ByteBuffer.allocate(16);
				bb.putInt(inviter_invitee[0]);
				bb.putInt(inviter_invitee[1]);
				bb.putLong(System.nanoTime());

				try {
					socket.writeBytes(bb.array());
				} catch (IOException e2) {
					System.out.println("Error in Sending response to request ACQUIRE for Action Accept-Reject FriendInvitee " + e2.getMessage());
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				bb.clear();

				break;

			case INVITE_FRIEND_ACTION_CODE : // invite friend
				if (requestArray.length<13) // only sends command,action code,inviter
				{ // acquire invitor and need invitee id
					int []invitor_invitee=acquireInviteFriend_invitor(m1);
					bb = ByteBuffer.allocate(8);
					bb.putInt(invitor_invitee[0]);
					bb.putInt(invitor_invitee[1]);

					try {
						socket.writeBytes(bb.array());
					} catch (IOException e2) {
						System.out.println("Error in Sending response to request ACQUIRE for Action Invite Friend-inviter " + e2.getMessage());
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					bb.clear();



					//socket.sendValue(i);  // -1 fail otherwise return invitee id
				}

				else
				{ // acquire invitee. 60,action code, invitor, invitee
					m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
					startTime=acquireMember( m2); // fail=-1

				try {
					socket.writeLong(startTime);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				}
				break;

			case THAW_FRIENDSHIP_ACTION_CODE:
				if (requestArray.length<13) // only sends command,action code,friend1
				{ // acquire friend1 and need friend2 id
					int []friend1Friend2=acquireThawFriendship(m1);
					bb = ByteBuffer.allocate(16);
					bb.putInt(friend1Friend2[0]);
					bb.putInt(friend1Friend2[1]);
					bb.putLong(System.nanoTime());

					try {
						socket.writeBytes(bb.array());
					} catch (IOException e2) {
						System.out.println("Error in Sending response to request ACQUIRE for Action Thaw Friendship " + e2.getMessage());
						// TODO Auto-generated catch block
						e2.printStackTrace();
					}
					bb.clear();



					//socket.sendValue(i);  // -1 fail otherwise return invitee id
				}

				else
				{ // acquire friend2 60,action code, friend1, friend2
					m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
					startTime=acquireMember( m2); // fail=-1

				try {
					socket.writeLong(startTime);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				}
				break;
				default:
				System.out.println("Unkown Action Code");

			} // end  action switch


			break; // end case acquire

		case RELEASE_COMMAND: // release 70, action code, 0 or 1,[members] 
			actionCode=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			result=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
			i=0;
			switch (actionCode)
			{
			case VIEW_PROFILE_ACTION_CODE:
			case LIST_FRIENDS_ACTION_CODE:
//				System.out.println("user T "+ result+ " is dea");
				coreWorkload.deactivateUser(result);
				try {
					socket.writeInt(1);
				} catch (IOException e3) {
					// TODO Auto-generated catch block
					e3.printStackTrace();
				}
				break;
			case THAW_FRIENDSHIP_ACTION_CODE:
				m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
				m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 16, 20)).getInt();
				int numFriendsForThisUserTillNow = 0;
//				if(friendshipInfo.get(Integer.toString(m1))!= null){
//					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(m1));
//				}
//				friendshipInfo.put(Integer.toString(m1), (numFriendsForThisUserTillNow-1));
				if (CoreWorkload.enableLogging&& result>=0)
				{
				actionType = "Unfriendfriend";
				startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,20 , 28)).getLong();
				updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+_threadId+","+m1+","+startTime+","+endTime+","+(numFriendsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
				CoreWorkload.updatesExist = true;
				}
				releaseThawFriendship(result,m1,m2);
				
				break;
			case REJECT_FRIEND_ACTION_CODE:
			case ACCEPT_FRIEND_ACTION_CODE:
				m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
				m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 16, 20)).getInt();
				 numFriendsForThisUserTillNow = 0;
//				if(friendshipInfo.get(Integer.toString(m2))!= null){
//					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(m2));
//				}
//				friendshipInfo.put(Integer.toString(m2), (numFriendsForThisUserTillNow+1));
				int numPendingsForThisUserTillNow = 0;
//				if(pendingInfo.get(Integer.toString(m2))!= null){
//					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(m2));
//				}
//				pendingInfo.put(Integer.toString(m2), (numPendingsForThisUserTillNow-1));
				if (CoreWorkload.enableLogging&& result>=0)
				{
				startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,20 , 28)).getLong();
				//log invitee
	
				if (actionCode==TokenWorker.ACCEPT_FRIEND_ACTION_CODE)
				{
					actionType = "AcceptFriend";
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+_threadId+","+m2+","+startTime+","+endTime+","+(numFriendsForThisUserTillNow+1)+",I"+ "," + actionType +"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+_threadId+","+m2+","+startTime+","+endTime+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");

				}
				else
				{
					actionType = "RejectFriend";
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+_threadId+","+m2+","+startTime+","+endTime+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");

					
				}
					CoreWorkload.updatesExist = true;
				
				}
				releaseAcceptRejectFriend_invitee( actionCode, result,m1,m2);
				
				break;
			case INVITE_FRIEND_ACTION_CODE : //invite friend
				if (requestArray.length<21) // release inviter
				{ 
					m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
					m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 16, 20)).getInt();
					releaseInviteFriend_invitor( result,m1,m2);
					

				}

				else
				{ //Release invitee
					
				
					m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
					m2=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 16, 20)).getInt();
					startTime=ByteBuffer.wrap(Arrays.copyOfRange(requestArray,20 , 28)).getLong();
					// log action

					int numPendingsForOtherUserTillNow = 0;
//					if(pendingInfo.get(Integer.toString(invitee))!= null){
//						numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(invitee));
//					}
//					pendingInfo.put(Integer.toString(invitee), (numPendingsForOtherUserTillNow+1));
					if(CoreWorkload.enableLogging)
					{
						actionType = "InviteFriends";
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+_threadId+","+m2+","+startTime+","+endTime+","+(numPendingsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
						CoreWorkload.updatesExist= true;
					}
					releaseInviteFriend_invitee( result,m1,m2);

				}
				break;
				default:
				{
					System.out.println("Unkown Action Code");
					System.exit(0);
				}

			} // end  action switch
			try {
				socket.writeInt(1);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			break; // Release command



		case 21: // for testing 
			try {
				workCount.incrementAndGet();

				//if (workCount.get()%100000==0)
				//System.out.println("Processing " + workCount.get());
				sleep(100);

				//System.out.println("Num work " +workCount.incrementAndGet());
			} catch (Exception e1) {
				System.out.println("Sleep");
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}


			break;


		case 1: // Test Acquire

			clientId= ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
			m1=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
			try
			{

				// log action
				//System.out.println("Logging "+ BGServer.CLIENT_ID + " , " + m1);
				//				PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter( BGServer.LogFileName, true)));

				//  out.println(BGServer.CLIENT_ID + " , " + m1);
				//out.close();

				///////////////
				//BGServer.RecievedRequests.remove(reqid);

				//				PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter( BGServer.LogFileName, true)));
				//				   out1.println("Worker start processing request from  " +clientId + " , member " + m1);
				//				    out1.close();

				//BGServer.Members.put(r1.getMember2(), 'n');
				int r=socket.sendValue(1);
				//  int r=socket.writeInt(1);
				// System.out.println("Released");
				if (r!=1)
					System.out.println("Error: Accquire Response not sent");



			}
			catch(Exception ex)
			{
				System.out.println("Acquire Request not exist");
			}

			break;	



		default:
			System.out.println("Error:Unknown Command Code");
			//System.exit(1);


		} //end outer switch 

	} // end do work
	
	

	private void delegateWork(SocketIO socket, byte[] requestArray, int command
			, StringBuilder updateLog, StringBuilder readLog, int seqID) {
		int actionCode = ByteBuffer.wrap(Arrays.copyOfRange(requestArray,4 , 8)).getInt();
		boolean insertImage;
		boolean warmup;
		int requestId;
		int ret = -1;
		switch (actionCode)
		{
		case VIEW_PROFILE_ACTION_CODE :
			insertImage = CoreWorkload.convert(requestArray[8]);
			warmup = CoreWorkload.convert(requestArray[9]);
			int profilekeyname = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 10, 14)).getInt();
			requestId=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 14, 18)).getInt();
			if (command==DELEGATE_COMMAND)
			ret = delegateViewProfile(profilekeyname, insertImage, warmup, readLog, seqID,requestId);
			else{ //DELEGATE_0_COMMAND//Requester delegate
				requestId=coreWorkload.activateUser(requestId);
				int profileOwner=CoreWorkload.membersOwners[profilekeyname];
				if (profileOwner==Client.machineid)
				{ // profile owner is local
					ret = delegateViewProfile(profilekeyname, insertImage, warmup, readLog, seqID,requestId);

				}
				else{ // delegate to profile owner
					
					SocketIO soc;
					try {
						soc = BGServer.SockPoolMapWorkload.get(profileOwner).getConnection();
					// send delegate command to target server
					ByteBuffer bb = ByteBuffer.allocate(4 + 4 + 1 + 1 + 4 + 4);
					bb.putInt(TokenWorker.DELEGATE_COMMAND);
					bb.putInt(TokenWorker.VIEW_PROFILE_ACTION_CODE);
					bb.put(CoreWorkload.convert(insertImage));
					bb.put(CoreWorkload.convert(warmup));
					bb.putInt(profilekeyname);
					bb.putInt(requestId);
					soc.writeBytes(bb.array());
					bb.clear();
					ret = soc.readInt();
					BGServer.SockPoolMapWorkload.get(profileOwner).checkIn(soc);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
				
				coreWorkload.deactivateUser(requestId);
			}
			break;
		case LIST_FRIENDS_ACTION_CODE:
			insertImage = CoreWorkload.convert(requestArray[8]);
			warmup = CoreWorkload.convert(requestArray[9]);
			requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 10, 14)).getInt();
			profilekeyname=ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 14, 18)).getInt();
			if (command==DELEGATE_COMMAND)
			ret = delegateListFriends(requestId, insertImage, warmup, readLog, seqID,profilekeyname);
			else{ //Requester delegate DELEGATE_0_COMMAND
				requestId=coreWorkload.activateUser(requestId);
				int profileOwner=CoreWorkload.membersOwners[profilekeyname];
				if (profileOwner==Client.machineid)
				{ // profile owner is local
				ret = delegateListFriends(requestId, insertImage, warmup, readLog, seqID,profilekeyname);

					
				}
				else{ // delegate to profile owner
					SocketIO soc;
					try {
						soc = BGServer.SockPoolMapWorkload.get(profileOwner).getConnection();
						ByteBuffer bb = ByteBuffer.allocate(18);
						bb.putInt(TokenWorker.DELEGATE_COMMAND);
						bb.putInt(TokenWorker.LIST_FRIENDS_ACTION_CODE);
						bb.put(CoreWorkload.convert(insertImage));
						bb.put(CoreWorkload.convert(warmup));
						bb.putInt(requestId);
						bb.putInt(profilekeyname);
						soc.writeBytes(bb.array());
						bb.clear();
						ret = soc.readInt();
						BGServer.SockPoolMapWorkload.get(profileOwner).checkIn(soc);

						

					} catch (ConnectException e) {
						System.out.println("Error: sending delegate list friend: " + e.getMessage());
					} catch (IOException e) {
						System.out.println("Error: sending delegate list friend: " + e.getMessage());
					}

					
				}
				coreWorkload.deactivateUser(requestId);
				
				
			}
			
			break;
		case VIEW_PENDINGS_ACTION_CODE:
			insertImage = CoreWorkload.convert(requestArray[8]);
			warmup = CoreWorkload.convert(requestArray[9]);
			requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 10, 14)).getInt();
			if (CoreWorkload.lockReads)
				requestId=coreWorkload.activateUser(requestId);
			ret = delegateViewPendingFriends(requestId, insertImage, warmup, readLog, seqID);
			if (CoreWorkload.lockReads)
				coreWorkload.deactivateUser(requestId);
			break;
		case ACCEPT_FRIEND_ACTION_CODE :
		case REJECT_FRIEND_ACTION_CODE:
			if (DELEGATE_COMMAND == command) {
				warmup = CoreWorkload.convert(requestArray[8]);
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 9, 13)).getInt();
				ret = delegateAcceptRejectInvitation(actionCode, requestId, warmup, updateLog, seqID);
			} else if (DELEGATE_2_COMMAND == command) {
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
				int requesteeId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
				ret = delegate2AcceptRejectInvitation(actionCode, requestId, requesteeId, updateLog, seqID);
			}
			break;
		case INVITE_FRIEND_ACTION_CODE :
			if (DELEGATE_COMMAND == command) {
				warmup = CoreWorkload.convert(requestArray[8]);
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 9, 13)).getInt();
				ret = delegateInviteFriend(requestId, warmup, seqID, updateLog);
			} else if (DELEGATE_2_COMMAND == command) {
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
				int requesteeId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
				ret = delegate2InviteFriend(requestId, requesteeId, seqID, updateLog);
			}
			break;
		case THAW_FRIENDSHIP_ACTION_CODE :
			if (DELEGATE_COMMAND == command) {
				warmup = CoreWorkload.convert(requestArray[8]);
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 9, 13)).getInt();
				ret = delegateThawFriendship(requestId, warmup, updateLog, seqID);
			} else if (DELEGATE_2_COMMAND == command) {
				requestId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 8, 12)).getInt();
				int thawedUserId = ByteBuffer.wrap(Arrays.copyOfRange(requestArray, 12, 16)).getInt();
				ret = delegate2ThawFriendship(requestId, thawedUserId, updateLog, seqID);
			}
			break;
		default:
			System.out.println("Error:Unknown Action Code");
		}
		
		try {
			socket.writeInt(ret);
		} catch (IOException e) {
			System.out.println("Error in sending delegate response"+ e.getMessage());
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public static  long releaseThawFriendship(int result, int m1, int m2) {
		// TODO Auto-generated method stub
		if (result<0)
		{
			coreWorkload.deactivateUser(m1);
			return -1;
		}
		else
		{
			long endTime=System.nanoTime();
			coreWorkload.deRelateUsers_oneSide(m1, m2);
			try {
				CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(m1)%CoreWorkload.numShards].acquire();
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
			
			String val = CoreWorkload.acceptedFrnds[CoreWorkload.memberIdxs.get(m1)].remove(m2);
			if (val == null) {
				System.out.println("BGCoreWorkload releaseThawFriendship: cannot remove "+m2+" from "+m1);
			}

			CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(m1)%CoreWorkload.numShards].release();
			
			
			coreWorkload.deactivateUser(m1);
			return endTime;
		}
	}

	

	public static int[] acquireThawFriendship(int keyname) {
		// TODO Auto-generated method stub
		int []friend1_friend2={-1,-1};
		keyname = coreWorkload.activateUser(keyname);
		if(keyname == -1)
			return friend1_friend2;
		coreWorkload.incrUserRef(keyname);
		try {
			CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(keyname)%CoreWorkload.numShards].acquire();
		} catch (InterruptedException e) {
			e.printStackTrace(System.out);
		}
		if(CoreWorkload.acceptedFrnds[CoreWorkload.memberIdxs.get(keyname)].size() > 0 ){
			int auserid = -1;	
			Set<Integer> keys = CoreWorkload.acceptedFrnds[CoreWorkload.memberIdxs.get(keyname)].keySet();
			Iterator<Integer> it = keys.iterator();
			auserid = it.next();	
			friend1_friend2[0]= keyname;
			friend1_friend2[1]=auserid;
			CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(keyname)%CoreWorkload.numShards].release();
			return friend1_friend2;
		}
		else
		{
			CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(keyname)%CoreWorkload.numShards].release();
			coreWorkload.deactivateUser(keyname);
			return friend1_friend2;

		}





	}

	public  static long releaseAcceptRejectFriend_invitee(int actionCode,int result, int auserid, int keyname) {
		// TODO Auto-generated method stub

		// invitee=keyname
		// inviter= auserid
		long endUpdatea=0;
		if (result>=0)
		{
		//remove from the list coz it has been rejected
		Vector<Integer> ids =CoreWorkload.pendingFrnds[CoreWorkload.memberIdxs.get(keyname)];
		//		ids.removeElement((Integer)auserid);
		ids.remove(ids.size()-1);
		if(CoreWorkload.enforceFriendship){
			try {
				CoreWorkload.withPend.acquire();
			} catch (Exception e) {
				e.printStackTrace();
			}
			int numPending = CoreWorkload.usersWithpendingFrnds.get(keyname);
			if(numPending - 1 == 0){
				CoreWorkload.usersWithpendingFrnds.remove(keyname);
			}else{
				CoreWorkload.usersWithpendingFrnds.put(keyname, numPending-1);
			}
			CoreWorkload.withPend.release();
		}


		endUpdatea = System.nanoTime();
		if (actionCode==REJECT_FRIEND_ACTION_CODE)
		{
			coreWorkload.deRelateUsers_oneSide(keyname, auserid );

		}
		else if (actionCode==ACCEPT_FRIEND_ACTION_CODE)
		{
			try {
				CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(keyname)%CoreWorkload.numShards].acquire();
				CoreWorkload.acceptedFrnds[CoreWorkload.memberIdxs.get(keyname)].put(auserid,"");
				CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(keyname)%CoreWorkload.numShards].release();
				coreWorkload.relateUsers_oneSide(keyname, auserid );
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}

		}
		coreWorkload.deactivateUser(keyname);
		return endUpdatea;
	}




	public  static int[] acquireAcceptRejectFriend_invitee(int invitee) {
		// TODO Auto-generated method stub
		int []inviter_invitee={-1,-1};
		invitee = coreWorkload.activateUser(invitee);
		if(invitee == -1)
			return inviter_invitee;
		coreWorkload.incrUserRef(invitee);

		int inviter = -1;
		Vector<Integer> ids =CoreWorkload.pendingFrnds[CoreWorkload.memberIdxs.get(invitee)];
		if(ids.size() <= 0 && CoreWorkload.enforceFriendship){
			//pick another user with pending friends that is not active
			try {
				CoreWorkload.withPend.acquire();
			} catch (Exception e) {
				e.printStackTrace();
			}
			Set<Integer> allUsers = CoreWorkload.usersWithpendingFrnds.keySet();
			if(allUsers.size() > 0){
				int explored = allUsers.size();
				Iterator<Integer> allUsersIt = allUsers.iterator();
				while(explored>0 )
				{
					int newkeyname = -1;
					if(allUsersIt.hasNext()){
						newkeyname = allUsersIt.next();
						explored--;
					}
					if (newkeyname!=-1){
					if(coreWorkload.isActive(newkeyname) != -1){
						coreWorkload.deactivateUser(invitee);
						invitee = newkeyname;
						ids =CoreWorkload.pendingFrnds[CoreWorkload.memberIdxs.get(invitee)];
						coreWorkload.incrUserRef(invitee);
						break;
					}}
				}
			}
			CoreWorkload.withPend.release();
		}
		if(ids.size() > 0){ //should always be true unless no one has pending requests
			inviter = ids.get(ids.size()-1);	
			inviter_invitee[0]=inviter; // inviter 
			inviter_invitee[1]=invitee; // invitee

		}
		else
		{
//			if (BGServer.verbose)
//			System.out.println("no user with pending friendship");
			coreWorkload.deactivateUser(invitee);
		}

		return inviter_invitee;
	}

	public static void acceptRejectInvitationUpdateInviterState(int actionCode, int inviter, int invitee) {
		// TODO Auto-generated method stub
		switch (actionCode)
		{
		case REJECT_FRIEND_ACTION_CODE:
			// update inviter: derelate inviter and invitee
			coreWorkload.deRelateUsers_oneSide(inviter, invitee);

			break;

		case ACCEPT_FRIEND_ACTION_CODE:
			try {
				CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(inviter)%CoreWorkload.numShards].acquire();
				CoreWorkload.acceptedFrnds[CoreWorkload.memberIdxs.get(inviter)].put(invitee,"");
				CoreWorkload.aFrnds[CoreWorkload.memberIdxs.get(inviter)%CoreWorkload.numShards].release();
				coreWorkload.relateUsers_oneSide(inviter, invitee );
			} catch (InterruptedException e) {
				e.printStackTrace();
			}



			break;
		}
	}

	public static long releaseInviteFriend_invitee( int result, int invitor,
			int invitee) {
		// TODO Auto-generated method stub
		long endUpdatei = System.nanoTime();
		if (result<0)
		{ // action was not performed
			coreWorkload.deactivateUser(invitee);
			return 1;

		}
		coreWorkload.relateUsers_oneSide(invitee, invitor );

		//update state 

		CoreWorkload.pendingFrnds[CoreWorkload.memberIdxs.get(invitee)].add(invitor);
		if(CoreWorkload.enforceFriendship){
			try {
				CoreWorkload.withPend.acquire();
			} catch (Exception e) {
				e.printStackTrace();
			}
			if(CoreWorkload.usersWithpendingFrnds.get(invitee) == null)
				CoreWorkload.usersWithpendingFrnds.put(invitee, 1);
			else{
				CoreWorkload.usersWithpendingFrnds.put(invitee, CoreWorkload.usersWithpendingFrnds.get(invitee)+1);
			}
			CoreWorkload.withPend.release();
		
		
		
		
		
		
		
		
		
		
		}
		//		int numPendingsForOtherUserTillNow = 0;
		//		if(Distribution.pendingInfo.get(Integer.toString(invitee))!= null){
		//			numPendingsForOtherUserTillNow = Distribution.pendingInfo.get(Integer.toString(invitee));
		//		}
		//		coreWorkload.pendingInfo.put(Integer.toString(invitee), (numPendingsForOtherUserTillNow+1));
		//		
		//	
		//		if(CoreWorkload.enableLogging){
		//			updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+noRelId+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
		//			updatesExist= true;
		//		}


		///////////////////////////////////////
		coreWorkload.deactivateUser(invitee);


		return endUpdatei;

	}

	public static long releaseInviteFriend_invitor( int result, int invitor,
			int invitee) {
		// TODO Auto-generated method stub
		long l=1;
		if (result<0)
		{ // action was not performed
			coreWorkload.deactivateUser(invitor);
			return l;

		}

		// action is performed
		coreWorkload.relateUsers_oneSide(invitor, invitee );
		coreWorkload.deactivateUser(invitor);
		return l;
	}

	public static long acquireMember( int m1) {
		// TODO Auto-generated method stub

		// if member is not active return current time, Otherwise, return -1
		long startTime;
		m1=coreWorkload.isActive(m1);
		if (m1==-1)
			startTime=-1; // member can't be acquires send -1
		else
		{ 
			startTime= System.nanoTime();
		}
		return startTime;

	}

	public static int[] acquireInviteFriend_invitor( int keyname) {
		// TODO Auto-generated method stub
		int []invitor_invitee={-1,-1};
		//		String actionType = "InviteFriends";
		//		int numOpsDone = 0;
		//		int keyname = CoreWorkload.buildKeyName(CoreWorkload.usercount);
		keyname = coreWorkload.activateUser(keyname);
		if(keyname == -1)
			return invitor_invitee;
		coreWorkload.incrUserRef(keyname);
		int noRelId = -1;
		noRelId = coreWorkload.viewNotRelatedUsers(keyname);
		if(noRelId== -1){ // not related user is not found
			coreWorkload.deactivateUser(keyname);
			return invitor_invitee;
		}

		// The blow code prevents contacting the same BGClient again if the invitee belongs to it as well
		//		if (noRelId%BGServer.NumOfClients==BGServer.CLIENT_ID)
		//		{ // noRelId belongs to this node as well
		//			noRelId=coreWorkload.isActive(noRelId);
		//			if(noRelId== -1){ //member is busy.  two people shouldn't invite each other at the same time
		//				coreWorkload.deactivateUser(keyname);
		//				return invitor_invitee;
		//		}
		//			
		//			
		//		}
		invitor_invitee[0]=keyname;
		invitor_invitee[1]=noRelId;
		return invitor_invitee;

	}

// Delegate Functions

	public int delegateAcceptRejectInvitation(int action, int member, boolean warmup, StringBuilder updateLog, int seqID) {  
		if (CoreWorkload.commandLineMode) {
			if (ACCEPT_FRIEND_ACTION_CODE == action) {
				System.out.println("Received Delegate Accept Friend action for inviter " + member);
			} else if (REJECT_FRIEND_ACTION_CODE == action) {
				System.out.println("Received Delegate Reject Friend action for inviter " + member);
			}
		}
		DB db=dbPool.getConnection();
		int ret = acceptRejectInvitationWithLocalInvitee(action, db, pendingInfo, friendshipInfo, member, warmup, updateLog, _threadId, seqID);
		dbPool.checkIn(db);
		if (CoreWorkload.commandLineMode) {
			if (ACCEPT_FRIEND_ACTION_CODE == action) {
				System.out.println("Delegate Accept Friend action performed for inviter " + member);
			} else if (REJECT_FRIEND_ACTION_CODE == action) {
				System.out.println("Delegate Reject Friend action performed for inviter " + member);
			}
		}
		return ret;
	}
	
	private int delegate2AcceptRejectInvitation(int action, int inviter, int invitee, StringBuilder updateLog, int seqID) {
		if (CoreWorkload.commandLineMode) {
			if (ACCEPT_FRIEND_ACTION_CODE == action) {
				System.out.println("Received Delegate 2 Accept Friend action for inviter " + inviter + " invitee " + invitee);
			} else if (REJECT_FRIEND_ACTION_CODE == action) {
				System.out.println("Received Delegate 2 Reject Friend action for inviter " + inviter + " invitee " + invitee);
			}
		}
		DB db=dbPool.getConnection();
		int ret = -1;
		if (ACCEPT_FRIEND_ACTION_CODE == action) {
			
			long startUpdatea = System.nanoTime();
			ret = db.acceptFriend(inviter, invitee);
			if (ret >= 0) {
				int numFriendsForOtherUserTillNow = 0;
//				if(friendshipInfo.get(inviter)!= null){
//					numFriendsForOtherUserTillNow = friendshipInfo.get(inviter);
//				}
//				friendshipInfo.put(Integer.toString(inviter), (numFriendsForOtherUserTillNow+1));
				
				long endUpdatea = System.nanoTime();
				if(CoreWorkload.enableLogging){
					String actionType = "AcceptFriend";
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+_threadId+","+inviter+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
					CoreWorkload.updatesExist = true;
				}
			}
		} else if (REJECT_FRIEND_ACTION_CODE == action) {
			ret = db.rejectFriend(inviter, invitee);
		}
		dbPool.checkIn(db);
		if  (ret < 0) {
			System.out.println("Error: query db for accept invitation: " + ret);
			System.exit(0);
		} else {
			acceptRejectInvitationUpdateInviterState(action, inviter, invitee);
			
		}
		if (CoreWorkload.commandLineMode) {
			if (ACCEPT_FRIEND_ACTION_CODE == action) {
				System.out.println("Delegate 2 Accept Friend action performed for inviter " + inviter + " invitee " + invitee);
			} else if (REJECT_FRIEND_ACTION_CODE == action) {
				System.out.println("Delegate 2 Reject Friend action performed for inviter " + inviter + " invitee " + invitee);
			}
		}
		return ret;
	}

	public int delegateInviteFriend(int invitor, boolean warmup, int seqID, StringBuilder updateTestLog) {
		String actionType = "InviteFriends";
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received Delegate Invite Friend action for inviter " + invitor);
		}
		DB db=dbPool.getConnection();
		int ret = inviteFriendWithLocalInviter(db, pendingInfo, invitor, warmup, _threadId, seqID, updateTestLog);
		dbPool.checkIn(db);
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegate Invite Friend action performed for inviter " + invitor);
		}
		return ret;
	}
	
	/**
	 * @param socket
	 * @param invitor
	 * @param invitee
	 * @return 0 if successful, a negative number if fail 
	 */
	public int delegate2InviteFriend(int invitor, int invitee, int seqID, StringBuilder updateLog) {
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received Delegate 2 Invite Friend action for inviter " + invitor + " and invitee " + invitee);
		}
		int retVal = 0;
		retVal = coreWorkload.isActive(invitee);
//		System.out.println("delegate 2 invite ");
		if (retVal != -1) {
			// perform db if invitee is not busy
			DB db=dbPool.getConnection();
			long startUpdatei = System.nanoTime();
			retVal = db.inviteFriend(invitor, invitee);
			dbPool.checkIn(db);
			TokenWorker.releaseInviteFriend_invitee(retVal, invitor, invitee);
			if (retVal < 0) {
				retVal = -1;
				System.out.println("Error: quering db for inviting friend, error code: " + retVal);
				System.exit(0);
			} else {
				long endUpdatei = System.nanoTime();
				int numPendingsForOtherUserTillNow = 0;
//				if(pendingInfo.get(Integer.toString(invitee))!= null){
//					numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(invitee));
//				}
//				pendingInfo.put(Integer.toString(invitee), (numPendingsForOtherUserTillNow+1));
				String actionType = "InviteFriends";
				if(CoreWorkload.enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+_threadId+","+invitee+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
					CoreWorkload.updatesExist= true;
				}
			}
		}
		
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegate 2 Invite Friend action performed for inviter " + invitor + " and invitee " + invitee);
		}
		return retVal;
	}
	
	public int delegateThawFriendship(int member, boolean warmup, StringBuilder updateLog, int seqID) {
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received Delegate Thaw Friend action for request user id " + member);
		}
		DB db=dbPool.getConnection();
		int ret = thawFriendWithLocalUser(db, friendshipInfo, member, warmup, updateLog, _threadId, seqID);
		dbPool.checkIn(db);
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegate Thaw Friend action performed for inviter " + member);
		}
		return ret;
	}
	
	private int delegate2ThawFriendship(int userId, int thawedUserId, StringBuilder updateLog, int seqID) {
		String actionType = "Unfriendfriend";
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received Delegate 2 Thaw Friend action for request user id " + userId + " thawed user id" + thawedUserId);
		}
		int ret = 0;
		ret = coreWorkload.isActive(thawedUserId);
		if (ret != -1) {
			DB db=dbPool.getConnection();
			long startUpdater = System.nanoTime();
			ret = db.thawFriendship(userId, thawedUserId);
			dbPool.checkIn(db);
			if (ret < 0) {
				ret = -1;
				System.out.println("Error: quering db for thaw friend, error code: " + ret);
				System.exit(0);
			}
			releaseThawFriendship(ret, thawedUserId, userId);
			if (ret >= 0) {
				int numFriendsForOtherUserTillNow = 0;
//				if(friendshipInfo.get(thawedUserId)!= null){
//					numFriendsForOtherUserTillNow = friendshipInfo.get(thawedUserId);
//				}
//				friendshipInfo.put(Integer.toString(thawedUserId), (numFriendsForOtherUserTillNow-1));
				long endUpdater = System.nanoTime();
				if(CoreWorkload.enableLogging){
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+_threadId+","+thawedUserId+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+ "," + actionType +"\n");
					CoreWorkload.updatesExist = true;
				}
			}
		}
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegate 2 Thaw Friend action performed for request user id " + userId + " thawed user id " + thawedUserId);
		}
		return ret;
	}


	public int delegateViewProfile(int member, boolean insertImage, boolean warmup, StringBuilder readLog, int seqID,int user) {
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received a delegation request to  perform View Profile action on member:" +member);
		}
		String actionType = "GetProfile";

		HashMap<String,ByteIterator> pResult=new HashMap<String,ByteIterator>();
//		coreWorkload.incrUserRef(member);
		int profileOwner = member;
		long startReadp = System.nanoTime();
		DB db=dbPool.getConnection();
		int ret = db.viewProfile(user, profileOwner, pResult, insertImage, false);
		dbPool.checkIn(db);
		if(ret < 0){
			System.out.println("There is an exception in getProfile.");
			System.exit(0);
		}
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegated View Profile action performed on member: " + member);
		}
		long endReadp = System.nanoTime();
		if(!warmup && CoreWorkload.enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+_threadId+","+profileOwner+","+startReadp+","+endReadp+","+pResult.get("friendcount")+ "," + actionType +"\n");
			if(user == profileOwner) {
				readLog.append("READ,PENDFRND,"+seqID+","+_threadId+","+profileOwner+","+startReadp+","+endReadp+","+pResult.get("pendingcount")+ "," + actionType +"\n");
			}
			CoreWorkload.readsExist = true;
		}
		return ret;
	}

	/**
	 * @param action
	 * @param member
	 * @return 0 if successful, negative value if failed
	 */
	public int delegateListFriends(int member, boolean insertImage, boolean warmup, StringBuilder readLog, int seqID,int profilekeyname) {
		String actionType = "GetFriends";
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received delegate request to  perform List Friend action on member:" +profilekeyname);
		}
//		coreWorkload.incrUserRef(member);
//		int profilekeyname = coreWorkload.buildKeyName(coreWorkload.usercount);
		Vector<HashMap<String,ByteIterator>> fResult = new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		DB db=dbPool.getConnection();
		int ret = db.listFriends(member, profilekeyname, null, fResult,  insertImage, false);
		dbPool.checkIn(db);
		if(ret < 0){
			System.out.println("There is an exception in listFriends.");
			System.exit(0);
		} else {
			ret = 0;
		}
		long endReadf = System.nanoTime();
		if(!warmup && CoreWorkload.enableLogging){
			readLog.append("READ,ACCEPTFRND,"+seqID+","+_threadId+","+profilekeyname+","+startReadf+","+endReadf+","+fResult.size()+ "," + actionType +"\n");
			CoreWorkload.readsExist = true;
		}
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegate action List Friends performed on member " + profilekeyname);
		}
		return ret;
	}

	/**
	 * @param action
	 * @param member
	 * @return 0 on success and negative value if failed
	 */
	public int delegateViewPendingFriends(int member, boolean insertImage, boolean warmup, StringBuilder readLog, int seqID) {
		String actionType = "GetPendingFriends";
		if (CoreWorkload.lockReads)
			member=coreWorkload.activateUser(member);
		if (CoreWorkload.commandLineMode) {
			System.out.println("Received a delegation request to  perform View Friend Request action on member:" +member);
		}
		coreWorkload.incrUserRef(member);
		Vector<HashMap<String,ByteIterator>> pResult=new Vector<HashMap<String,ByteIterator>>();
		long startReadf = System.nanoTime();
		DB db=dbPool.getConnection();
		int ret = db.viewFriendReq(member,pResult, insertImage, false);
		dbPool.checkIn(db);
		if(ret < 0){
			System.out.println("There is an exception in viewFriendReq.");
			System.exit(0);
		}
		long endReadf = System.nanoTime();
		if (CoreWorkload.lockReads)
			coreWorkload.deactivateUser(member);
		if(!warmup && CoreWorkload.enableLogging){
			readLog.append("READ,PENDFRND,"+seqID+","+_threadId+","+member+","+startReadf+","+endReadf+","+pResult.size()+ "," + actionType +"\n");
			CoreWorkload.readsExist = true;
		}
		if (CoreWorkload.commandLineMode) {
			System.out.println("Delegated action View Friends Request performed on member " + member);
		}
		return ret;
	}
	
	public static int thawFriendWithLocalUser(DB db,
			HashMap<String, Integer> friendshipInfo, int userId, boolean warmup, StringBuilder updateLog, int threadId, int seqID) {
		int thawedUserId;
		String actionType = "Unfriendfriend";
		int []friend1_friend2=TokenWorker.acquireThawFriendship(userId);
		userId = friend1_friend2[0];
		thawedUserId = friend1_friend2[1];
		if (CoreWorkload.commandLineMode) {
			System.out.println("generate thawed user id" + thawedUserId + " for thaw friendship action");
		}
		if (userId == -1 || thawedUserId == -1) {
			// there is no available userId or the userId has no related user. 
			return -1;
		} else {
			if (warmup) {
				coreWorkload.deactivateUser(userId);
				return -1;
			}
			int thawedUserMachineId = thawedUserId % Client.numBGClients;
			int ret = -1;
			if (thawedUserMachineId == Client.machineid) {
				// the thawed user is local. 
				ret = coreWorkload.isActive(thawedUserId);
				long startUpdater = System.nanoTime();
				if (ret != -1) {
					// perform db if thawed user is not busy
					ret = db.thawFriendship(userId, thawedUserId);
					if (ret < 0) {
						ret = -1;
						System.out.println("Error: quering db for inviting friend, error code: " + ret);
						System.exit(0);
					}
					// update information. 
					releaseThawFriendship(ret, thawedUserId, userId);
				}
				releaseThawFriendship(ret, userId, thawedUserId);
				if (ret >= 0) {
					int numFriendsForThisUserTillNow = 0;
//					if(friendshipInfo.get(Integer.toString(userId))!= null){
//						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(userId));
//					}
//					friendshipInfo.put(Integer.toString(userId), (numFriendsForThisUserTillNow-1));

					int numFriendsForOtherUserTillNow = 0;
//					if(friendshipInfo.get(thawedUserId)!= null){
//						numFriendsForOtherUserTillNow = friendshipInfo.get(thawedUserId);
//					}
//					friendshipInfo.put(Integer.toString(thawedUserId), (numFriendsForOtherUserTillNow-1));
					long endUpdater = System.nanoTime();
					if(CoreWorkload.enableLogging){
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+userId+","+startUpdater+","+endUpdater+","+(numFriendsForOtherUserTillNow-1)+",D"+ "," + actionType +"\n");
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+thawedUserId+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
						CoreWorkload.updatesExist = true;
					}
				}
			} else {
				// the thawed user id is not local, talk to the thawed user node
				if (CoreWorkload.commandLineMode) {
					System.out.println("Delegate 2 thaw friendship action to machine " + thawedUserMachineId);
				}
				try {
					SocketIO inviteeSocket = BGServer.SockPoolMapWorkload.get(thawedUserMachineId).getConnection();
					ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 4);
					buffer.putInt(TokenWorker.DELEGATE_2_COMMAND);
					buffer.putInt(TokenWorker.THAW_FRIENDSHIP_ACTION_CODE);
					buffer.putInt(userId);
					buffer.putInt(thawedUserId);
					inviteeSocket.writeBytes(buffer.array());
					buffer.clear();
					long startUpdater = System.nanoTime();
					ret = inviteeSocket.readInt();
					TokenWorker.releaseThawFriendship(ret, userId, thawedUserId);
					BGServer.SockPoolMapWorkload.get(thawedUserMachineId).checkIn(inviteeSocket);
					if (ret >= 0) {
						int numFriendsForThisUserTillNow = 0;
//						if(friendshipInfo.get(Integer.toString(userId))!= null){
//							numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(userId));
//						}
//						friendshipInfo.put(Integer.toString(userId), (numFriendsForThisUserTillNow-1));

						long endUpdater = System.nanoTime();
						if(CoreWorkload.enableLogging){
							updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+userId+","+startUpdater+","+endUpdater+","+(numFriendsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
							CoreWorkload.updatesExist = true;
						}
					}
				} catch (ConnectException e) {
					System.out.println("Error: sending delegate 2 command for invitee: " + e.getMessage());
				} catch (IOException e) {
					System.out.println("Error: sending delegate 2 command for invitee: " + e.getMessage());
				}
			}
			if (ret < 0) {
				return -1;
			} else {
				return 0;
			}
		}
	}
	

	/**
	 * @param db
	 * @param pendingInfo
	 * @param invitor
	 * @return -1 if there is no available inviter or the inviter has related to all other user or the action is failed <br/>
	 * 			0 on success
	 * 			
	 * 			
	 */
	public static int inviteFriendWithLocalInviter(DB db,
			HashMap<String, Integer> pendingInfo, int invitor, boolean warmup, int threadid, int seqID, StringBuilder updateTestLog) {
		int invitee;
		int []invitor_invitee=TokenWorker.acquireInviteFriend_invitor(invitor);
		invitor = invitor_invitee[0];
		invitee = invitor_invitee[1];
		if (CoreWorkload.commandLineMode) {
			System.out.println("generate invitee " + invitee + " for invite friend action");
		}
		if (invitor == -1 || invitee == -1) {
			// there is no available inviter or the inviter has related to all other user. 
			return -1;
		} else {
			if (warmup) {
				coreWorkload.deactivateUser(invitor);
				return -1;
			}
			int inviteeMachineId = invitee % Client.numBGClients;
			int ret = -1;
			if (inviteeMachineId == Client.machineid) {
				// the invitee is local. 
				ret = coreWorkload.isActive(invitee);
				if (ret != -1) {
					// perform db if invitee is not busy
					long startUpdatei = System.nanoTime();
					ret = db.inviteFriend(invitor, invitee);
					if (ret < 0) {
						ret = -1;
						System.out.println("Error: quering db for inviting friend, error code: " + ret);
						System.exit(0);
					}
					TokenWorker.releaseInviteFriend_invitee(ret, invitor, invitee);
					long endUpdatei = System.nanoTime();
					if (CoreWorkload.enableLogging) {
						int numPendingsForOtherUserTillNow = 0;
//						if(pendingInfo.get(Integer.toString(invitee))!= null){
//							numPendingsForOtherUserTillNow = pendingInfo.get(Integer.toString(invitee));
//						}
//						pendingInfo.put(Integer.toString(invitee), (numPendingsForOtherUserTillNow+1));
						String actionType = "InviteFriends";
						updateTestLog.append("UPDATE,PENDFRND,"+seqID+","+threadid+","+invitee+","+startUpdatei+","+endUpdatei+","+(numPendingsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
					}
				} 
				TokenWorker.releaseInviteFriend_invitor(ret, invitor, invitee);
			} else {
				// the invitee is not local, talk to the invitee's node
				try {
					if (CoreWorkload.commandLineMode) {
						System.out.println("Delegating 2 Invite Friend action for invitee " + invitee + " to machine " + inviteeMachineId);
					}
					SocketIO inviteeSocket = BGServer.SockPoolMapWorkload.get(inviteeMachineId).getConnection();
					ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + 4);
					buffer.putInt(TokenWorker.DELEGATE_2_COMMAND);
					buffer.putInt(TokenWorker.INVITE_FRIEND_ACTION_CODE);
					buffer.putInt(invitor);
					buffer.putInt(invitee);
					inviteeSocket.writeBytes(buffer.array());
					buffer.clear();
					
					ret = inviteeSocket.readInt();
					TokenWorker.releaseInviteFriend_invitor(ret, invitor, invitee);
					BGServer.SockPoolMapWorkload.get(inviteeMachineId).checkIn(inviteeSocket);
				} catch (ConnectException e) {
					System.out.println("Error: sending delegate 2 command for invitee: " + e.getMessage());
				} catch (IOException e) {
					System.out.println("Error: sending delegate 2 command for invitee: " + e.getMessage());
				}
			}
			if (ret < 0) {
				return -1;
			} else {
				return 0;
			}
		}
	}

	public static int acceptRejectInvitationWithLocalInvitee(int action, DB db, HashMap<String, Integer> pendingInfo
			, HashMap<String, Integer> friendshipInfo, int invitee, boolean warmup, StringBuilder updateLog, int threadId, int seqID) {
		int ret = -1;
		int[] inviter_invitee = TokenWorker.acquireAcceptRejectFriend_invitee(invitee);
		int inviter = inviter_invitee[0];
		invitee = inviter_invitee[1];
		if (CoreWorkload.commandLineMode) {
			if (ACCEPT_FRIEND_ACTION_CODE == action) {
				System.out.println("Generate inviter" + inviter +  " Accept Friend Action ");
			} else if (REJECT_FRIEND_ACTION_CODE == action) {
				System.out.println("Generate inviter" + inviter +  " Reject Friend Action ");
			}
		}
		if (inviter == -1 || invitee == -1) {
			return -1;
		} else {
			if (warmup) {
				coreWorkload.deactivateUser(invitee);
				return -1;
			}
		}
		int inviterMachineId = inviter % Client.numBGClients;
		if (inviterMachineId == Client.machineid) {
			// inviter is local
			long startUpdatea = System.nanoTime();
			if (TokenWorker.REJECT_FRIEND_ACTION_CODE == action) {
				ret = db.rejectFriend(inviter, invitee);
			} else if (TokenWorker.ACCEPT_FRIEND_ACTION_CODE == action) {
				ret = db.acceptFriend(inviter, invitee);
			}
			if (ret < 0) {
				System.out.println("There is an exception in acceptFriend.");
				System.exit(0);
			} else {
				TokenWorker.acceptRejectInvitationUpdateInviterState(action, inviter, invitee);
			}
			TokenWorker.releaseAcceptRejectFriend_invitee(action, ret, inviter, invitee);
			if (TokenWorker.ACCEPT_FRIEND_ACTION_CODE == action) {
				int numFriendsForThisUserTillNow = 0;
//				if(friendshipInfo.get(Integer.toString(invitee))!= null){
//					numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(invitee));
//				}
//				friendshipInfo.put(Integer.toString(invitee), (numFriendsForThisUserTillNow+1));


				int numFriendsForOtherUserTillNow = 0;
//				if(friendshipInfo.get(inviter)!= null){
//					numFriendsForOtherUserTillNow = friendshipInfo.get(inviter);
//				}
//				friendshipInfo.put(Integer.toString(inviter), (numFriendsForOtherUserTillNow+1));
				
				int numPendingsForThisUserTillNow = 0;
//				if(pendingInfo.get(Integer.toString(invitee))!= null){
//					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(invitee));
//				}
//				pendingInfo.put(Integer.toString(invitee), (numPendingsForThisUserTillNow-1));
				
				long endUpdatea = System.nanoTime();
				if(CoreWorkload.enableLogging){
					String actionType = "AcceptFriend";
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+inviter+","+startUpdatea+","+endUpdatea+","+(numFriendsForOtherUserTillNow+1)+",I"+ "," + actionType +"\n");
					updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+ "," + actionType +"\n");
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
					CoreWorkload.updatesExist = true;
				}
			} else if (TokenWorker.REJECT_FRIEND_ACTION_CODE == action) {
				String actionType = "RejectFriend";
				int numPendingsForThisUserTillNow = 0;
//				if(pendingInfo.get(Integer.toString(invitee))!= null){
//					numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(invitee));
//				}
//				pendingInfo.put(Integer.toString(invitee), (numPendingsForThisUserTillNow-1));
				
				long endUpdatea = System.nanoTime();
				if(CoreWorkload.enableLogging){
					updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
					CoreWorkload.updatesExist = true;
				}
			}
		} else {
			// inviter is not local
			if (CoreWorkload.commandLineMode) {
				if (ACCEPT_FRIEND_ACTION_CODE == action) {
					System.out.println("Delegating 2 Accept Friend Action to machine " + inviterMachineId);
				} else if (REJECT_FRIEND_ACTION_CODE == action) {
					System.out.println("Delegating 2 Reject Friend Action to machine " + inviterMachineId);
				}
			}
			try {
				SocketIO inviterSocket = BGServer.SockPoolMapWorkload.get(inviterMachineId).getConnection();
				ByteBuffer bb = ByteBuffer.allocate(4 + 4 + 4 + 4);
				bb.putInt(TokenWorker.DELEGATE_2_COMMAND);
				bb.putInt(action);
				bb.putInt(inviter);
				bb.putInt(invitee);
				inviterSocket.writeBytes(bb.array());
				bb.clear();
				long startUpdatea = System.nanoTime();
				ret = inviterSocket.readInt();
				if (ret < 0) {
					System.exit(0);
				}
				TokenWorker.releaseAcceptRejectFriend_invitee(action, ret, inviter, invitee);				
				BGServer.SockPoolMapWorkload.get(inviterMachineId).checkIn(inviterSocket);
				if (TokenWorker.ACCEPT_FRIEND_ACTION_CODE == action) {
					int numFriendsForThisUserTillNow = 0;
//					if(friendshipInfo.get(Integer.toString(invitee))!= null){
//						numFriendsForThisUserTillNow = friendshipInfo.get(Integer.toString(invitee));
//					}
//					friendshipInfo.put(Integer.toString(invitee), (numFriendsForThisUserTillNow+1));

					int numPendingsForThisUserTillNow = 0;
//					if(pendingInfo.get(Integer.toString(invitee))!= null){
//						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(invitee));
//					}
//					pendingInfo.put(Integer.toString(invitee), (numPendingsForThisUserTillNow-1));
					
					long endUpdatea = System.nanoTime();
					if(CoreWorkload.enableLogging){
						String actionType = "AcceptFriend";
						updateLog.append("UPDATE,ACCEPTFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numFriendsForThisUserTillNow+1)+",I"+ "," + actionType +"\n");
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
						CoreWorkload.updatesExist = true;
					}
				} else if (TokenWorker.REJECT_FRIEND_ACTION_CODE == action) {
					String actionType = "RejectFriend";
					int numPendingsForThisUserTillNow = 0;
//					if(pendingInfo.get(Integer.toString(invitee))!= null){
//						numPendingsForThisUserTillNow = pendingInfo.get(Integer.toString(invitee));
//					}
//					pendingInfo.put(Integer.toString(invitee), (numPendingsForThisUserTillNow-1));
					
					long endUpdatea = System.nanoTime();
					if(CoreWorkload.enableLogging){
						updateLog.append("UPDATE,PENDFRND,"+seqID+","+threadId+","+invitee+","+startUpdatea+","+endUpdatea+","+(numPendingsForThisUserTillNow-1)+",D"+ "," + actionType +"\n");
						CoreWorkload.updatesExist = true;
					}
				}
			} catch (ConnectException e) {
				System.out.println("Error: sending delegate reject friend " + e.getMessage());
			} catch (IOException e) {
				System.out.println("Error: sending delegate reject friend " + e.getMessage());
			}
		}
		return ret;
	}
	/*
	public void saveRequests (RequestParameters r1)
	{
		// save request


		BGServer.RecievedRequests.put(BGServer.ReqId, r1);

		System.out.println("Request no " + BGServer.ReqId + "is saved");
		BGServer.ReqId++;
		r1.printRequest();
	}
	 */

	public int RespondtoClient(SocketIO socket, int response)
	{


		// Yaz: Write to socket

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ByteBuffer bb = ByteBuffer.allocate(4);
			bb.putInt(response);
			baos.write(bb.array());
			//baos.write(command.getBytes());
			bb.clear();
			//System.out.println("Server Send 0000");

			//Determine what client ip's to send to the receiving client.

			baos.flush();
			socket.writeBytes(baos.toByteArray());
			baos.reset();

			baos = null;
			bb = null;
			//command = null;
			return 1;
		} catch (IOException e) {
			System.out.println("Error: Response Socket Error; Could not connect to Client");
			e.printStackTrace();
			System.exit(-1);
		} catch(Exception e) {
			System.out.println("Response Unknown Exception.");
			e.printStackTrace();
			System.exit(-1);
		}
		return 0;

	}


	public static int indexOf(Object[] arrayOfByteBuffers, byte[] byteArray) {
		for(int i = 0; i < arrayOfByteBuffers.length; i++) {
			if(ByteBuffer.wrap(byteArray).equals((ByteBuffer)arrayOfByteBuffers[i]))
				return i;
		}
		return -1;
	}

	/**
	 * Returns -1 if the array is full.
	 * @param array
	 * @return
	 */
	public int getNextEmpty(Object[] array) {
		for(int i = 0; i < array.length; i++) {
			if(array[i] == null) {
				return i;
			}
		}
		return -1;
	}
	public static void main(String args[]) {
		//UnitTest1(1);
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("-----------------------");

	}

	public long getNumRequestsProcessed() {
		return numRequestsProcessed;
	}

	public void setNumRequestsProcessed(long numRequestsProcessed) {
		this.numRequestsProcessed = numRequestsProcessed;
	}

	public double getMaxNumReqInQ() {
		return maxNumReqInQ;
	}

	public void setMaxNumReqInQ(double maxNumReqInQ) {
		this.maxNumReqInQ = maxNumReqInQ;
	}

	public double getMaxDiffWorkers_Req() {
		return maxDiffWorkers_Req;
	}

	public void setMaxDiffWorkers_Req(double maxDiffWorkers_Req) {
		this.maxDiffWorkers_Req = maxDiffWorkers_Req;
	}
}