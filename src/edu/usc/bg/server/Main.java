package edu.usc.bg.server;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketPermission;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class Main {

	/**
	 * @param args
	 */




	/*
		 -p numclients=3

			-p clients=IP1:Port1,IP2:Port2,IP3:Port3

			-p clientid=1

			-p nummembers=1001

			-p numrequests = 1330  (It generates 1330 requests for memberid 0 to 1329)

			Error checking:

			- If numrequests > nummembers then report error and exit

			- If IP:Port are duplicates then report error and exit

			- The number of comma seperated IP:Port in the cleints must equal numclients.  Otherwise, report error and exit!
			- If clientid >= numclients then report error and exit
	 */


	public static void shutdown()
	{
		for (int i=0; i< BGServer.NumOfClients;i++)
		{
			if (i!= BGServer.CLIENT_ID)
			{
				SocketIO soc;
				SockIOPool p= BGServer.SockPoolMapWorkload.get(i);
				if (p==null)
				{
					System.out.println("Pool Null");
				}
				else
				{
					//System.out.println("111 no of sockets: " +p.availPool.size());

				//
					
					//System.out.println("Number of available : " +p.availPool.size() +" Number of dead: " +p.deadPool.size() + " Number of busy: " + p.busyPool.size());
					System.out.println("Sending stop handling");
					//p.sendStopHandling(p.availPool,99);
					System.out.println("Finish stop handling");

					System.out.println("Start shutdown workload pool");
					//p.shutDown();
					System.out.println("FInish shutdown workload pool");
				}
				//soc = BGServer.SockPoolMap.get(i).getConnection();
				//soc.sendValue(99);
				//BGServer.SockPoolMap.get(i).checkIn(soc);
			}

		}
		if (BGServer.NumOfClients==1)
		{
			if (BGServer.verbose)
				System.out.println("Shutting down BG one client");
			BGServer.shutdown();
		}

	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		InputParsing a=null;
		try
		{
			a= new InputParsing(args);
		}
		catch(Exception ex)
		{
			System.out.println("Error: Arguments are not passed correctly " + ex.getMessage());
			System.exit(1);

		}
		//int clientId, , int noclients, int nomembers , HashMap<Integer,ClientInfo> hm
		//int clientId, int noclients, int nomembers , HashMap<Integer,ClientInfo> hm

		
		BGServer b = new BGServer(a.ClientID,a.NumClients,a.NumMembers,a.NumThreads,a.ClientInfoMap, a.Duration,a.NumSockets);

		



		///

		//WorkloadThread TArray[] = new WorkloadThread[a.NumThreads];
		
		for (int i=0; i< a.NumThreads;i++)
			//TArray[i]=
			new WorkloadThread(BGServer.duration,i);
		
		for  (int i=0; i< a.NumThreads;i++)
			WorkloadThread.WorkloadThreads[i].start();
	

		//if (BGServer.verbose)
			System.out.println("Done creating "+a.NumThreads+" worker threads and waiting for them to finish...");
		
		try{
			for(int i=0; i< a.NumThreads;i++) {

				WorkloadThread.WorkloadThreads[i].join();
			//	if (BGServer.verbose)
//				System.out.println("Thread " + i + " finished.");
			}
		}

		catch (Exception ex){ System.out.println("Error Join: " +ex.getMessage())	;
		}




		// total number of actions
		long localActions=0;
		long remoteActions=0;
		for (int i=0; i<a.NumThreads;i++)
		{
			localActions=localActions+WorkloadThread.WorkloadThreads[i].localActionsCount;

			remoteActions=remoteActions+WorkloadThread.WorkloadThreads[i].remoteActionsCount;

		}
		System.out.println("Stats: " + "Number of threads: " +
				BGServer.NumOfThreads +" No. of local actions= " + localActions + " Number of remote actions= " + remoteActions+ " Total= " + (localActions+remoteActions));
		System.out.println("Done All");
		
		// shutdown if one BGClient
		if (BGServer.NumOfClients==1)
		{
			if (BGServer.verbose)
				System.out.println("Shutting down BG one client");
			BGServer.shutdown();
		}
		
		// Wait until recieve shutdown request from Coordinator
		
//		shutdown();



	}

}
