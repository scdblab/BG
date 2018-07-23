package edu.usc.bg.server;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;



public class WorkloadThread extends Thread {

	//	ConcurrentHashMap<Integer, SocketIO> soc= new ConcurrentHashMap<Integer, SocketIO>();
	long duration_minutes;
	public long localActionsCount=0;
	public long remoteActionsCount=0;
	private int threadID;
	public static WorkloadThread [] WorkloadThreads= new WorkloadThread[BGServer.NumOfThreads];
	public static int threadsCount=0;
	WorkloadThread(long d, int id)
	{
		duration_minutes=d;
		threadID=id;
		WorkloadThreads[threadsCount++]=this;
		

	}
	public void run()
	{
		generateWorkLoad2();
		
	}

	public void generateWorkLoad2()
	{
		int m=0;
		long EndTime;
		if (duration_minutes==0)
			EndTime=0;
		else		
			EndTime = (System.currentTimeMillis())+ duration_minutes*1000*60;
		boolean timeflag=true;
		int owner;
		SocketIO soc;
		while ( m < BGServer.NumOfMembers && timeflag)
		{
			if(BGServer.verbose)
			if (m%10000==0)
				System.out.println("Thread "+threadID+" processed 10000 requests");

			owner= m%BGServer.NumOfClients ; //This generates a value from 0 to NumClients-1

			if (owner!= BGServer.CLIENT_ID)
			{
			
				try
				{
					
						
						soc= BGServer.SockPoolMapWorkload.get(owner).getConnection();
						if (soc==null)
						{
							System.out.println("Error: Not able to get Socket from the pool");
						
						}
						
						
						
					
					sendAcquireRequest2( BGServer.CLIENT_ID,soc, m,owner);
					
					BGServer.SockPoolMapWorkload.get(owner).checkIn(soc);
					
				}
				catch (Exception ex){System.out.println("Error Send Acquire " + ex.getMessage());}
			}
			else
			{   // local call delegate
				int result=0;// TokenWorker.delegateInviteFriend(2, m);
				if (result==1)
				{
				localActionsCount++;
				//if (BGServer.verbose);
				if (BGServer.verbose)
					System.out.println("Local Action completes " + "Client " + BGServer.CLIENT_ID + " Member " + m);
				}
				else
				{
					System.out.println("Local Action failed");
				}
			}
			m++;

			if (System.currentTimeMillis()>=EndTime && EndTime!=0)
			{
				timeflag=false;
				if (BGServer.verbose)
					System.out.println("Duration Ended!!!");
			}


		}// end while
		if (m>= BGServer.NumOfMembers)
			if (BGServer.verbose)
				System.out.println("Finish All Members!!!");
		
		//System.out.println("Thread " + this.threadID +" Finish");
	}	
	
	public void sendAcquireRequest2( int Cid,SocketIO socket, int m,int owner)
	{

		try
		{
			//Delegate Action 2,action code, member id


			ByteBuffer bb = ByteBuffer.allocate(12);
			bb.putInt(2);
			bb.putInt(2);
			bb.putInt(m);
			socket.writeBytes(bb.array());
		

			// wait for response
			
			
			    bb.clear();

			byte[] request = socket.readBytes();
		
			int a=ByteBuffer.wrap(request).getInt();

			if (a==1)
			{
				//socket.closeAll();
				//if (BGServer.verbose);
				//System.out.println("Remote Action completes");
				if (BGServer.verbose)
				System.out.println("Remote Action completes " + "Client " + BGServer.CLIENT_ID + " Member " + m);

				remoteActionsCount++;

			}
			else
			{
				System.out.println("Remote Action Failed");
			}
		} catch (Exception ex){System.out.println(ex.getMessage());}


	}
	
	
	
	//////////////////////


	public void generateWorkLoad()
	{
		int m=0;
		long EndTime;
		if (duration_minutes==0)
			EndTime=0;
		else		
			EndTime = (System.currentTimeMillis())+ duration_minutes*1000*60;
		boolean timeflag=true;
		int owner;
		SocketIO soc=null;
		while ( m < BGServer.NumOfMembers && timeflag)
		{
			if(BGServer.verbose)
			if (m%10000==0)
				System.out.println("Thread "+threadID+" processed 10000 requests");

			owner= m%BGServer.NumOfClients ; //This generates a value from 0 to NumClients-1

			if (owner!= BGServer.CLIENT_ID)
			{
			
//				try
//				{
//					
						
						try {
							soc= BGServer.SockPoolMapWorkload.get(owner).getConnection();
						} catch (Exception e) {
							System.out.println("Not able to get a socket from the pool " +e.getMessage());
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						if (soc==null)
						{
							System.out.println("Error: Not able to get Socket from the pool");
						
						}
						
						if (BGServer.logFile)
						{
						PrintWriter out;
						try {
			
							out = new PrintWriter(new BufferedWriter(new FileWriter( BGServer.LogFileName, true)));
							out.println("Start " +threadID + " , " + m);
						    out.close();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						}

						   
					
					sendAcquireRequest( BGServer.CLIENT_ID,soc, m,owner);
//					PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter( BGServer.LogFileName, true)));
//
//					   out1.println("end " +threadID + " , " + m);
//					    out1.close();
					BGServer.SockPoolMapWorkload.get(owner).checkIn(soc);
//				}
//				catch (Exception ex){
//					
//					
//					System.out.println("Error Send Acquire " + ex.getMessage());}
		}
			else
			{
				localActionsCount++;
				if (BGServer.verbose);
				//	System.out.println("Local Action completes ");
			}
			m++;

			if (System.currentTimeMillis()>=EndTime && EndTime!=0)
			{
				timeflag=false;
				if (BGServer.verbose)
					System.out.println("Duration Ended!!!");
			}


		}// end while
		if (m>= BGServer.NumOfMembers)
			if (BGServer.verbose)
				System.out.println("Finish All Members!!!");
		
		//System.out.println("Thread " + this.threadID +" Finish");
	}




	public void sendAcquireRequest( int Cid,SocketIO socket, int m,int owner)
	{

		try
		{
			//Socket socket1 = new Socket(ip,port);
			//SocketIO socket=new SocketIO(socket1);
			//ByteArrayOutputStream baos = new ByteArrayOutputStream();
			// ByteArrayOutputStream baos1 = new ByteArrayOutputStream();


			ByteBuffer bb = ByteBuffer.allocate(12);
			bb.putInt(1);
			bb.putInt(Cid);
			bb.putInt(m);
			socket.writeBytes(bb.array());
		

			// wait for response
			
			String msg="";
			int command = ByteBuffer.wrap(Arrays.copyOfRange(bb.array(), 0, 4)).getInt();
			int clientId= ByteBuffer.wrap(Arrays.copyOfRange(bb.array(),4 , 8)).getInt();
			int m1=ByteBuffer.wrap(Arrays.copyOfRange(bb.array(), 8, 12)).getInt();
//			PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter( BGServer.LogFileName, true)));
//			   out1.println("Waiting for response " +threadID + " , " + m+ "Message= " + " cmd= "+command +"," + " currentC "+clientId + ", m= " + m1 +"target C= "+ owner);
//			    out1.close();
			    bb.clear();

			byte[] request = socket.readBytes();
			
			int a=ByteBuffer.wrap(request).getInt();

			if (a==1)
			{
				//socket.closeAll();
				if (BGServer.verbose);
				//System.out.println("Remote Action completes");
				remoteActionsCount++;

			}
		} catch (Exception ex){System.out.println(ex.getMessage());}


	}}
