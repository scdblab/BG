package edu.usc.bg.server;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import edu.usc.bg.base.Client;


public class BGCoord {
	static boolean verbose=false;
	static Vector<BGInitThread> bgInitThreads = new Vector<BGInitThread>();
	static Vector<BGThread> bgThreads = new Vector<BGThread>();
	public static int MsgPassingBGMode=1;
	public static int Partitioned=2;
	public static int benchmarkMode=Partitioned;
	static Socket processListenerSocket = null;
	static InputStream plInputStream = null;
	static OutputStream plOutputStream = null;
	static Scanner plScanner = null;
	static BufferedReader plBufferedReader = null;
	static PrintWriter plPrintWriter = null;
	static DataInputStream plDataInputStream = null;
	static DataOutputStream plDataOutputStream = null;
	static int expDuration;

	public static Vector<MyListener> clients = new Vector<MyListener>();

	public static String arguments[];
	//	public static InputParsing input;
	public static ConcurrentHashMap<Integer, ClientInfo> ClientInfoMap;
	public static boolean deleteLogFiles=false;
	public static boolean copyFolder=false;
	public static boolean copycore=false;
	public static boolean copycoreall=false;
	public static boolean dbstat=false;
	static String dbip="10.0.0.225";
	int dbStatPort=12345;
	public static int numMembers;
	public static int numClients;
	public static Properties props;

	public static void main(String args[]) {


		// read parameters
		arguments=args;
		//		input =null;

		props = new Properties();
		Properties fileprops = new Properties();
		//Enums & map of enum,obj
		boolean[] inputArguments = {true, false, false, false, false, false, false, false, false};

		// parse arguments
		String tempArgs[]= new String[args.length-1];
		System.arraycopy(args,1, tempArgs,0,args.length-1);
		Client.readCmdArgs(tempArgs,props, inputArguments, fileprops);
		String mode=props.getProperty("benchmarkingmode","Partitioned");
		expDuration=Integer.parseInt(props.getProperty( "maxexecutiontime"));
		if(mode.equalsIgnoreCase("retain")|| mode.equalsIgnoreCase("delegate"))
		{
			benchmarkMode=MsgPassingBGMode;
		}
		else
		{
			benchmarkMode=Partitioned;
		}

		String clientArg=props.getProperty("clients");

		numClients= Integer.parseInt(props.getProperty("numclients"));
		numMembers= Integer.parseInt(props.getProperty("nummembers"));
		
		try
		{
			ClientInfoMap=Client.PopulateClientInfo(clientArg,numClients);
		}
		catch(Exception ex)
		{
			System.out.println("Error: Arguments are not passed correctly");
			System.exit(1);
		}
		String bg_destfolder;
		String src;	
		// copying folder
		if (copyFolder)
		{
			String ips[]=new String[ClientInfoMap.size()];
			for (int i: ClientInfoMap.keySet()){
				ips[i]=ClientInfoMap.get(i).getIP();
			}
			///////////////////////

			//			bg_destfolder="\\BGTest_Communication_Infrastructure";
			//			src="\\\\10.0.1.75\\Users\\yaz\\Documents\\BGTest_Communication_InfrastructureC";
			//
			//			copyToClients(src,bg_destfolder);
			//			
			//			bg_destfolder="\\BGTest";
			//			src="\\\\10.0.1.75\\Users\\yaz\\Documents\\BGSumita";
			//
			//			copyToClients(src,bg_destfolder);
			//		System.exit(0);
			///////////////

			//			bg_destfolder="\\BG\\BG\\kosarOracle.cfg";
			//			 src="\\\\127.0.0.1\\BG\\BG\\kosarOracle.cfg";
			//			copyToClients(src,bg_destfolder,ips);
			//			bg_destfolder="\\BG\\BG\\src";
			//			 src="\\\\127.0.0.1\\BG\\BG\\src";
			//			copyToClients(src,bg_destfolder,ips);
			//			bg_destfolder="\\BG\\BG\\bin";
			//			src="\\\\127.0.0.1\\BG\\BG\\bin";
			//
			//			copyToClients(src,bg_destfolder,ips);
			//
			//			bg_destfolder="\\BG\\BG\\db\\jdbc\\src";
			//			src="\\\\127.0.0.1\\BG\\BG\\db\\jdbc\\src";
			//
			//			copyToClients(src,bg_destfolder,ips);
			//			
			//		
			//			bg_destfolder="\\BG\\BG\\lib\\kvs.jar";
			//			src="\\\\127.0.0.1\\BG\\BG\\lib\\kvs.jar";
			//
			//			copyToClients(src,bg_destfolder,ips);
			//			
			//			
			//			bg_destfolder="\\BG\\BG\\workloads";
			//			src="\\\\127.0.0.1\\BG\\BG\\workloads";
			//			
			//			copyToClients(src,bg_destfolder,ips);

			///// Partitioned BG
			//			bg_destfolder="\\BGPartitioned\\BG\\db\\jdbc\\src\\relational";
			//			src="\\\\127.0.0.1\\BGPartitioned\\BG\\db\\jdbc\\src\\relational";
			//
			//			copyToClients(src,bg_destfolder);

			//			bg_destfolder="\\BGPartitioned\\BG\\bin";
			//			src="\\\\127.0.0.1\\BGPartitioned\\BG\\bin";
			//
			//			copyToClients(src,bg_destfolder);
			//			
			//			bg_destfolder="\\BGPartitioned\\BG\\src";
			//			src="\\\\127.0.0.1\\BGPartitioned\\BG\\src";
			//			copyToClients(src,bg_destfolder);
			//			
			//			
			//			bg_destfolder="\\BGPartitioned\\BG\\db\\jdbc\\src";
			//			src="\\\\127.0.0.1\\BGPartitioned\\BG\\db\\jdbc\\src";
			//			copyToClients(src,bg_destfolder);
			//			
			//			bg_destfolder="\\BG\\RyanListener\\src";
			//			src="\\\\127.0.0.1\\BG\\RyanListener\\src";
			//			copyToClients(src,bg_destfolder,ips);
			//			
			//			bg_destfolder="\\BG\\RyanListener\\bin";
			//			src="\\\\127.0.0.1\\BG\\RyanListener\\bin";
			//			copyToClients(src,bg_destfolder,ips);




		}
		if (deleteLogFiles)
		{
			String ips[]={"10.0.1.75","10.0.1.70","10.0.1.65","10.0.1.60","10.0.1.55","10.0.1.50","10.0.1.45","10.0.1.40","10.0.1.35","10.0.1.25","10.0.1.20","10.0.1.15","10.0.1.10"};
			bg_destfolder="\\BG\\BGServerListenerLogs";
			src="\\\\127.0.0.1\\BG\\e";

			copyToClients(src,bg_destfolder,ips);
			bg_destfolder="\\BG\\logs";


			copyToClients(src,bg_destfolder,ips);
			//cores
			bg_destfolder="\\Core\\Core\\KOSAR-Core-dev\\Stat";
			copyToClients(src,bg_destfolder,ips);
			//db
			String db_destfolder="\\OS\\stats";
			copyToClients(src, db_destfolder,new String[] {dbip});

			//			bg_destfolder="\\BG\\logs";
			//			src="\\\\127.0.0.1\\BG\\logs";
			//			copyToClients(src,bg_destfolder);
			//System.exit(0);

		}
		if (copycore){
			String ips[]=new String[ClientInfoMap.size()];
			for (int i: ClientInfoMap.keySet()){
				ips[i]=ClientInfoMap.get(i).getIP();
			}
			//			bg_destfolder="\\Core\\KVSJDBC\\bin";
			//			 src="\\\\127.0.0.1\\Core\\KVSJDBC\\bin";
			//			copyToClients(src,bg_destfolder,ips);
			//			 bg_destfolder="\\Core\\KVSJDBC\\src";
			//			 src="\\\\127.0.0.1\\Core\\KVSJDBC\\src";
			//			copyToClients(src,bg_destfolder,ips);

			bg_destfolder="\\Core\\Core\\KOSAR-Core-dev\\bin";
			src="\\\\127.0.0.1\\Core\\Core\\KOSAR-Core-dev\\bin";
			copyToClients(src,bg_destfolder,ips);

			bg_destfolder="\\Core\\Core\\KOSAR-Core-dev\\src";
			src="\\\\127.0.0.1\\Core\\Core\\KOSAR-Core-dev\\src";
			copyToClients(src,bg_destfolder,ips);

			bg_destfolder="\\Core\\Core\\KOSAR-Core-dev\\config";
			src="\\\\127.0.0.1\\Core\\Core\\KOSAR-Core-dev\\config";
			copyToClients(src,bg_destfolder,ips);


			//			System.exit(0);
		}
		if (copycoreall){
			String ips[]=new String[ClientInfoMap.size()];
			for (int i: ClientInfoMap.keySet()){
				ips[i]=ClientInfoMap.get(i).getIP();
			}

			bg_destfolder="\\Core";
			src="\\\\127.0.0.1\\Core";
			copyToClients(src,bg_destfolder,ips);
		}
		if (copycore||copyFolder||copycoreall)
			System.exit(0);


		System.out.println("The coordinator is running...");
		for (int i=0; i<ClientInfoMap.size(); i++)
		{
			MyListener l= new MyListener();
			l.setIp(ClientInfoMap.get(i).getIP());
			l.setPort(String.valueOf((ClientInfoMap.get(i).getPort()+1)));
			clients.add(l);

		}
		connectToListeners(ClientInfoMap.size());


		/** Constructor **/
		BGCoord driver = new BGCoord();

	}

	public static void copyToClients(String src, String bg_destfolder,String [] ips) {


		// TODO Auto-generated method stub
		String localIP="";
		try {
			localIP=InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		for (int i = 0; i < ips.length; i++) 
		{
			try {

				if (!ips[i].equalsIgnoreCase(localIP)|| src.equals("\\\\127.0.0.1\\BG\\e") )
				{

					long st;
					String destfolder = "\\\\" + ips[i]
							// + "\\c$\\BG";
							+ bg_destfolder;
					System.out.println("Deleting " + destfolder + "...");
					st = System.currentTimeMillis();
					try {
						File dest = new File(destfolder);

						if (!deleteDir(dest))
							System.out.println("Failed to delete " + destfolder);
						System.out.println(" Done! ("
								+ (System.currentTimeMillis() - st) + " msec)");

						st = System.currentTimeMillis();

						File srcFolder = new File(src);

						File destFolder = new File(destfolder);

						System.out.println("Copying " + destfolder + "...");

						copyFolder(srcFolder, destFolder, true,false);

					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(" Done! ("
							+ (System.currentTimeMillis() - st) + " msec)");
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}}
	}


	public static void copyFromClients(String src, String bg_destfolder, String[] IP) {


		// TODO Auto-generated method stub

		for (int i = 0; i < IP.length; i++) 
		{

			//Inet4Address.getLocalHost().getHostAddress()


			long st;
			String src1 = "\\\\" + IP[i]
					// + "\\c$\\BG";
					+ src;

			File srcFolder = new File(src1);

			File destFolder = new File( bg_destfolder);

			//						System.out.println("Copying From " + src1 + "...");

			try {
				copyFolder(srcFolder, destFolder, true,true);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}



			//					System.out.println(" Done");

		} 
	}






	BGCoord()

	{
		runClients(0,ClientInfoMap.size()," ");
	}

	public static void copyFolder(File src, File dest, boolean silent, boolean delSrc)
			throws IOException {
		if (src.isDirectory()) {
			// if directory not exists, create it
			if (!dest.exists()) {
				dest.mkdir();
				if (!silent)
					System.out.println("Directory copied from " + src + "  to "
							+ dest);
			}
			// list all the directory contents
			String files[] = src.list();
			for (String file : files) {
				// construct the src and dest file structure
				File srcFile = new File(src, file);
				File destFile = new File(dest, file);
				// recursive copy
				if (!delSrc||(delSrc&&!srcFile.isDirectory()))
				{
					copyFolder(srcFile, destFile, silent,delSrc);
					if (delSrc)
						deleteDir(srcFile);
				}

			}
		} else {
			// if file, then copy it
			// Use bytes stream to support all file types
			InputStream in = new FileInputStream(src);
			OutputStream out = new FileOutputStream(dest);
			byte[] buffer = new byte[1024];
			int length;
			// copy the file content in bytes
			while ((length = in.read(buffer)) > 0) {
				out.write(buffer, 0, length);
			}

			in.close();
			out.close();
			if (!silent)
				System.out.println("File copied from " + src + " to " + dest);
		}
	}

	public static boolean deleteDir(File dir) {
		boolean suc=true;
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					suc=false;
				}
			}
			if (suc==false)
				return false;
		}
		return dir.delete();
	}

	public static void connectToListeners(int numClients) {
		// connecting to clients. assumes no port clashes.
		// DOUBLE CHECK in config file.
		int counter = 0;
		for (MyListener listener : clients) {
			counter++;
			try {
				listener.setSocket(new Socket(listener.getIp(), listener
						.getPort()));
				listener.setInputStream(listener.getSocket().getInputStream());
				listener.setOutputStream(listener.getSocket().getOutputStream());
				listener.setScanner(new Scanner(listener.getInputStream()));
				listener.setPrintWriter(new PrintWriter(
						new BufferedOutputStream(listener.getOutputStream())));
				if (verbose)
					System.out.println("Connection to " + listener.getIp()
							+ " successful");
			} catch (UnknownHostException e) {
				System.out.println("UnknownHostException trying to connect to "
						+ listener.getIp());
			} catch (IOException e) {
				System.out.println("IOException trying to connect to "
						+ listener.getIp());
			}
			if (counter == numClients)
				break;
		}

	}


	public void runClients (int numThreads, int numClients, String workload)
	{
		// reset ALL bgClients
		bgInitThreads = new Vector<BGInitThread>();
		bgThreads = new Vector<BGThread>();
		if (bgThreads != null) {
			for (BGThread bgThread : bgThreads)
				if (bgThread != null)
					bgThread.resetClient();
		}
		// reset actions/sec stats
		for(MyListener listener : clients) {
			listener.actionsPerSecond = 0.0;
			listener.staleReads = 0.0;
		}

		for (int i = 0; i < numClients; i++) {
			BGInitThread bg = new BGInitThread(i, numThreads, clients.get(i),
					numClients, workload);
			bgInitThreads.add(bg);
			bg.start();
			try {
				bg.join();
			} catch (InterruptedException e) {
				System.out.println("Error in joining thread");
				e.printStackTrace();
			}
		}
		Socket dbsocket=null;
		if (dbstat)
		{
			try {
				dbsocket=new Socket(dbip,dbStatPort);
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		for (int i = 0; i < numClients; i++) {
			bgThreads.get(i).start();
			try {
				//				
				if (BGCoord.benchmarkMode==BGCoord.MsgPassingBGMode)
				{
					//					System.out.println("Sleeping");
					//					Thread.sleep(0);

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		for (BGThread bgThread : bgThreads) {
			try {
				bgThread.join();
				bgThread.finishedJoining = true;
				if(verbose)
					System.out.println("BGThread " + bgThread.threadID + " ended.");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (dbstat)
		{
			try {
				dbsocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		StringBuilder coord=new StringBuilder();
		String newline=System.getProperty("line.separator") ;
		coord.append("Arguments="+BGThread.createBGRunMessage(0)+newline);
		System.out.println("Summary Results:");
		coord.append("Summary Results:"+newline);
		System.out.println("ClientID,OVERALLTHROUGHPUT(ACTIONS/SECS),OVERALLTHROUGHPUT(SESSIONS/SECS),RAMPEDOVERALLTHROUGHPUT(ACTIONS/SECS),RAMPEDOVERALLTHROUGHPUT(SESSIONS/SECS), Number of Remote msgs/sec,LocalActsThrou(act/sec),PartialActsThrou(act/sec),LocOrPartialActsThrou(act/sec),max Requests in Q,Max Diff req_workers wating, max wating DB,Num Sockets");
		coord.append("ClientID,OVERALLTHROUGHPUT(ACTIONS/SECS),OVERALLTHROUGHPUT(SESSIONS/SECS),RAMPEDOVERALLTHROUGHPUT(ACTIONS/SECS),RAMPEDOVERALLTHROUGHPUT(SESSIONS/SECS), Number of Remote msgs/sec,LocalActsThrou(act/sec),PartialActsThrou(act/sec),LocOrPartialActsThrou(act/sec),max Requests in Q,Max Diff req_workers wating, max wating DB,Num Sockets"+newline);
		double avgSessionThroughput=0,avgActionThroughput=0, avgActionRampedThroughput=0,avgSessionRampedThroughput=0;
		double numReqProcessed=0,maxReqQ=0,maxDiffReqWorkersWaiting=0,maxWaitingDB=0;
		long numLocActs=0,numLocorPartialActs=0,numPartialActs=0;
		for (BGThread bgThread : bgThreads) {
			System.out.println(bgThread.threadID+","+bgThread.actionThrouput+","+bgThread.sessionThroughput+","+bgThread.actionRampedThroughput+","+bgThread.sessionRampedThroughput+","+bgThread.numRequestsProcessed/expDuration+","+bgThread.numLocalActions/expDuration+","+bgThread.numPartialActions/expDuration+","+bgThread.numLocalorPartial/expDuration+","+bgThread.maxNumReqInQ+","+bgThread.maxDiffWorkers_Req+","+bgThread.maxWaitingDB+","+ bgThread.numSockets+","+bgThread.staleInfo);
			coord.append(bgThread.threadID+","+bgThread.actionThrouput+","+bgThread.sessionThroughput+","+bgThread.actionRampedThroughput+","+bgThread.sessionRampedThroughput+","+bgThread.numRequestsProcessed/expDuration+","+bgThread.numLocalActions/expDuration+","+bgThread.numPartialActions/expDuration+","+bgThread.numLocalorPartial/expDuration+","+bgThread.maxNumReqInQ+","+bgThread.maxDiffWorkers_Req+","+bgThread.maxWaitingDB+","+ bgThread.numSockets+","+bgThread.staleInfo+newline);
			avgActionThroughput+=bgThread.actionThrouput;
			avgSessionThroughput+=bgThread.sessionThroughput;
			avgActionRampedThroughput+=bgThread.actionRampedThroughput;
			avgSessionRampedThroughput+=bgThread.sessionRampedThroughput;
			numReqProcessed+=bgThread.numRequestsProcessed;
			maxReqQ=Math.max(maxReqQ, bgThread.maxNumReqInQ);
			maxDiffReqWorkersWaiting=Math.max(maxDiffReqWorkersWaiting, bgThread.maxDiffWorkers_Req);
			maxWaitingDB=Math.max(maxWaitingDB, bgThread.maxWaitingDB);

			numLocActs+=bgThread.numLocalActions;
			numPartialActs+=bgThread.numPartialActions;
			numLocorPartialActs+=bgThread.numLocalorPartial;



		}
		System.out.println("summary:");
		coord.append("Summary:"+newline);
		System.out.println("THROUGHPUT(ACTIONS/SECS),THROUGHPUT(SESSIONS/SECS),RAMPEDTHROUGHPUT(ACTIONS/SECS),RAMPEDTHROUGHPUT(SESSIONS/SECS), NumMsgs/sec,LocalActsThrou(act/sec),PartialActsThrou(act/sec),LocOrPartialActsThrou(act/sec),max Requests in Q,Max Diff req_workers wating, max wating DB");
		coord.append("THROUGHPUT(ACTIONS/SECS),THROUGHPUT(SESSIONS/SECS),RAMPEDTHROUGHPUT(ACTIONS/SECS),RAMPEDTHROUGHPUT(SESSIONS/SECS), NumMsgs/sec,LocalActsThrou(act/sec),PartialActsThrou(act/sec),LocOrPartialActsThrou(act/sec),max Requests in Q,Max Diff req_workers wating, max wating DB"+newline);
		System.out.println(avgActionThroughput+","+avgSessionThroughput+","+avgActionRampedThroughput+","+avgSessionRampedThroughput+","+numReqProcessed/expDuration+","+numLocActs/expDuration+","+numPartialActs/expDuration+","+numLocorPartialActs/expDuration+","+maxReqQ+","+maxDiffReqWorkersWaiting+","+maxWaitingDB); 
		coord.append(avgActionThroughput+","+avgSessionThroughput+","+avgActionRampedThroughput+","+avgSessionRampedThroughput+","+numReqProcessed/expDuration+","+numLocActs/expDuration+","+numPartialActs/expDuration+","+numLocorPartialActs/expDuration+","+maxReqQ+","+maxDiffReqWorkersWaiting+","+maxWaitingDB+newline);
		try {
			Thread.sleep(10000);
		} catch (InterruptedException iex){
			System.out.println("Error in thread buffer sleep");
		}

		//copy log files
		String threadcount=props.getProperty("threadcount");
		String bgworkload=props.getProperty("workloadfile");
		String bmode=props.getProperty("benchmarkingmode");
		String log=props.getProperty("enablelogging");

		//		String path="C:\\Users\\yaz\\Documents\\Experiments\\";
		//		String dir="Exp"+System.currentTimeMillis()+"-nummembers"+BGCoord.numMembers+"-numclients"+BGCoord.numClients+"-bmode"+bmode+"-threadcount"+threadcount+"-log"+log+"-workload"+bgworkload;
		//		new File(path+dir).mkdir();
		//		File file = new File(path+dir+"\\coord.txt");
		//		FileWriter fstream;
		//		try {
		//			fstream = new FileWriter(file);
		//
		//			BufferedWriter l = new BufferedWriter(fstream);
		//			l.write(coord.toString());
		//			l.close();
		//		} catch (IOException e) {
		//			// TODO Auto-generated catch block
		//			e.printStackTrace();
		//		}
		String ips[]=new String[ClientInfoMap.size()];
		for (int i: ClientInfoMap.keySet()){
			ips[i]=ClientInfoMap.get(i).getIP();
		}
		Vector<StringBuilder> sv;
		//		//clients
		//		try{
		//		copyFromClients("\\BG\\BGServerListenerLogs", path+dir,ips);
		//		 sv=CreateResourcesFiles.createFiles(path+dir,"listener");
		//		CreateResourcesFiles.createResourcesFiles(sv,path+dir,"client");
		//		CreateResourcesFiles.createCharts(path+dir,"client");
		//		}
		//		catch (Exception ex){
		//			ex.printStackTrace();
		//		}
		//		ips = new String[CoreServerAddr.shards.length];
		//		for (int i = 0; i < CoreServerAddr.shards.length; i++) {
		//			ips[i] = CoreServerAddr.shards[i].split(":")[0];
		//		}
		//		
		//		// for cores
		//		try{
		//		copyFromClients("\\Core\\Core\\KOSAR-Core-dev\\Stat", path+dir, ips);
		//		 sv=CreateResourcesFiles.createFiles(path+dir,"os+");
		//		CreateResourcesFiles.createResourcesFiles(sv,path+dir,"core");
		//		CreateResourcesFiles.createCharts(path+dir,"core");
		//		}
		//		catch (Exception ex){
		//			ex.printStackTrace();
		//		}
		//		if (dbstat)
		//		{
		//			ips=new String[1];
		//			ips[0]=dbip;
		//			try{
		//				copyFromClients("\\OS\\stats", path+dir, ips);
		//				sv=CreateResourcesFiles.createFiles(path+dir,"osdb");
		//				CreateResourcesFiles.createResourcesFiles(sv,path+dir,"db");
		//				CreateResourcesFiles.createCharts(path+dir,"db");
		//
		//			}
		//			catch (Exception ex){
		//				ex.printStackTrace();
		//			}
		//		}

	}

}



class BGInitThread extends Thread {
	int threadID;
	int threadCount;
	int numClients;
	MyListener client;
	boolean done = false;
	String workload = "";

	BGInitThread(int i, int threadCount, MyListener client,
			int numClients, String workload) {
		this.threadID = i;
		this.threadCount = threadCount;
		this.client = client;
		this.numClients = numClients;
		this.workload = workload;
	}

	public void run() {
		BGThread bg = new BGThread(threadID, threadCount, client, numClients, workload);
		BGCoord.bgThreads.add(bg);
		bg.initThread();
		done = true;
	}

	public boolean isDone() {
		return done;
	}
}


///////////////


class BGThread extends Thread {
	double sessionThroughput=0,actionThrouput=0, actionRampedThroughput=0,sessionRampedThroughput=0;
	long numLocalActions=0,numPartialActions=0, numLocalorPartial=0;
	int threadID;
	long numRequestsProcessed;
	double maxNumReqInQ=0;
	double maxDiffWorkers_Req=0;
	double maxWaitingDB=0;
	String numSockets="";
	String staleInfo="";
	int threadCount;
	static AtomicInteger loadCount=new AtomicInteger(0);
	int numClients = 0;
	MyListener listenerClient;
	String runMessage = null;
	boolean stillRunning = true;
	boolean initialStats = true;
	private double throughput;
	private String[] stats;
	boolean finishedJoining = false;
	String workload = "";
	static AtomicInteger count=new AtomicInteger(0);

	public BGThread(int threadID, int threadCount, MyListener client,
			int numClients, String workload) {
		this.threadID = threadID;
		this.threadCount = threadCount;
		this.listenerClient = client;

		// num clients for this round;
		this.numClients = numClients;
		this.workload = workload;
	}

	public void initThread() {
		if(BGCoord.verbose)
			System.out.println(listenerClient.getIp() + ":" + listenerClient.getPort()
					+ " successfully initialized");
	}

	public static String createBGRunMessage(int threadID) {
		String message="";
		String arg;
		int usercount=0;
		if (BGCoord.benchmarkMode==BGCoord.MsgPassingBGMode)
		{
			usercount=BGCoord.numMembers/BGCoord.numClients;
			int remaining= BGCoord.numMembers-(usercount*BGCoord.numClients);
			if (remaining>threadID)
				usercount++;
		}
		for (int i=0; i< BGCoord.arguments.length; i++)
		{
			arg=BGCoord.arguments[i];

			if (BGCoord.arguments[i].contains("machineid"))
				arg= "machineid="+ threadID;

			if (BGCoord.benchmarkMode==BGCoord.MsgPassingBGMode)
			{
				if (BGCoord.arguments[i].contains("usercount"))
					arg= "usercount="+ usercount;
			}
			message=message+ arg+ ' ';
		}

		return message;
	}
	
	public boolean stillRunning() {
		return stillRunning;
	}

	public void resetClient() {
		System.out.println("Resetting Client");
		listenerClient.actionsPerSecond = 0.0;
		listenerClient.staleReads = 0.0;
		listenerClient.closeAllStreams();
		stillRunning = false;
	}

	public void run() {
		// tell the Client to Begin the Benchmark Simulation
		//listenerClient.sendMessage(createBGRunMessage());
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

		if(BGCoord.verbose)
			System.out.println("To "+ listenerClient.getIp() + ":" 
					+ listenerClient.getPort() + ": StartSimulation at: " + dateFormat.format(Calendar.getInstance().getTime())  );
		stillRunning = true;
		listenerClient.getPrintWriter().println(createBGRunMessage(this.threadID));
		listenerClient.getPrintWriter().flush();



		// read until the end of the process.
		boolean gotActThru = false;
		boolean gotThru = false;
		boolean gotLatency = false;

		/*
		 * Reads Input from the Listener. Listener sends stats as well as
		 * the end-test condition 'THEEND.'
		 */

		while (listenerClient.getScanner().hasNext() && stillRunning()) {




			String msgrec = listenerClient.getScanner().nextLine();


			// Shutdown BGClients 


			if (msgrec.contains("Done loading DB"))
			{
				if (loadCount.incrementAndGet()>=BGCoord.ClientInfoMap.size())
				{
					loadCount.set(0);
					for(MyListener listener : BGCoord.clients) {
						listener.getPrintWriter().println("Run Benchmark");
						listener.getPrintWriter().flush();
					}



				}

			}
			else if(msgrec.contains("SHUTDOWN!!!"))
			{
				if (count.incrementAndGet()>=BGCoord.ClientInfoMap.size() && BGCoord.numClients>1)
				{
					count.set(0);
					//					if(BGCoord.verbose)
					System.out.println("BGCoord is sending stop handling to all");
					// first, send stop handling request to each BGClient
					SocketIO[] BGClientSockets= new SocketIO[BGCoord.ClientInfoMap.size()];
					SocketIO socket;
					try {
						for (int i=0; i<BGCoord.ClientInfoMap.size();i++)
						{


							socket = new SocketIO(new Socket (BGCoord.ClientInfoMap.get(i).getIP(), BGCoord.ClientInfoMap.get(i).getPort()));
							BGClientSockets[i]=socket;
							BGClientSockets[i].sendValue(999);


						} 

						for (int j=0; j<BGCoord.ClientInfoMap.size();j++)
						{
							BGClientSockets[j].sendValue(9999);
							BGClientSockets[j].closeAll();
						}

					} //try
					catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println("Error: Coordinator is not able to send shutdown requests to BGClients " + e.getMessage());
						e.printStackTrace();
					} 

				}	

			} // end if shutdown
			else
			{
				if(BGCoord.verbose)
					System.out.println(listenerClient.getIp() + ":"
							+ listenerClient.getPort() + " ***Thread " + threadID
							+ ": " + msgrec);
				if 	(msgrec.contains("stale"))
					staleInfo=staleInfo+msgrec;
				else if 	(msgrec.contains("Partially"))
					numPartialActions=Long.parseLong(msgrec.substring(msgrec.indexOf(':')+1,msgrec.length()));
				else if 	(msgrec.contains("partial or local"))
					numLocalorPartial=Long.parseLong(msgrec.substring(msgrec.indexOf(':')+1,msgrec.length()));
				else if 	(msgrec.contains("Number of local actions"))
					numLocalActions=Long.parseLong(msgrec.substring(msgrec.indexOf(':')+1,msgrec.length()));
				else if 	(msgrec.contains("Number of Sockets"))
					numSockets=msgrec.substring(msgrec.indexOf(':')+1,msgrec.length());
				else if 	(msgrec.contains("Max waiting for DB"))
					maxWaitingDB=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());
				else if 	(msgrec.contains("Max Difference between requests and workers waiting"))
					maxDiffWorkers_Req=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());
				else if 	(msgrec.contains("Max Requests in Q"))
					maxNumReqInQ=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());

				else if (msgrec.contains("Number of requests processed by workers"))
					numRequestsProcessed=Long.parseLong((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());

				else if (msgrec.contains("OVERALLTHROUGHPUT(ACTIONS/SECS)")&& !msgrec.contains("RAMPED"))
					actionThrouput=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());

				else if (msgrec.contains("OVERALLTHROUGHPUT(SESSIONS/SECS)")&& !msgrec.contains("RAMPED"))
					sessionThroughput=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());
				else if (msgrec.contains("RAMPEDOVERALLTHROUGHPUT(ACTIONS/SECS)"))
					actionRampedThroughput=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());

				else if (msgrec.contains("RAMPEDOVERALLTHROUGHPUT(SESSIONS/SECS)"))
					sessionRampedThroughput=Double.parseDouble((msgrec.substring(msgrec.indexOf(':')+1,msgrec.length())).trim());



			}



			/*if (msgrec.contains("THEEND.")) {
				//listenerClient.sendPrintMessage("shutdown ");
				listenerClient.sendPrintMessage("KILL ");

				System.out.println("End of Benchmark for " + listenerClient.getIp()
						+ ":" + listenerClient.getPort());
				stillRunning = false;
			}*/
		} //while
		if(BGCoord.verbose)
			System.out.println("To "+ listenerClient.getIp() + ":" + listenerClient.getPort() + ": EndSimulation : "  );

		//	System.out.println("To "+ listenerClient.getIp() + ":" + listenerClient.getPort() + ": EndSimulation at: " + dateFormat.format(Calendar.getInstance().getTime()) );

	}

}








class MyListener {
	private Socket socket = null;
	private InputStream inputStream = null;
	private OutputStream outputStream = null;
	private Scanner scanner = null;
	private PrintWriter printWriter = null;
	private String ip = null;
	private int port = 0;
	private int userCount = 0;
	private int userOffset = 0;
	Double actionsPerSecond = 0.0;
	Double staleReads = 0.0;

	public void sendMessage(String command) throws IOException {
		DataOutputStream dos = new DataOutputStream(outputStream);
		dos.writeBytes(command);
		dos.flush();
	}

	public void sendPrintMessage(String command) throws IOException {
		printWriter.print(command);
		printWriter.flush();
	}

	public void closeAllStreams() {
		try {
			inputStream.close();
			outputStream.close();
			scanner.close();
			printWriter.close();
			socket.close();
		} catch (IOException e) {
			System.err.println("Error in shutting down streams");
			e.printStackTrace();
		}
	}

	/**** Getters & Setters ****/
	public Socket getSocket() {
		return socket;
	}

	public void setSocket(Socket socket) {
		this.socket = socket;
	}

	public InputStream getInputStream() {
		return inputStream;
	}

	public void setInputStream(InputStream inputStream) {
		this.inputStream = inputStream;
	}

	public OutputStream getOutputStream() {
		return outputStream;
	}

	public void setOutputStream(OutputStream outputStream) {
		this.outputStream = outputStream;
	}

	public Scanner getScanner() {
		return scanner;
	}

	public void setScanner(Scanner scanner) {
		this.scanner = scanner;
	}

	public PrintWriter getPrintWriter() {
		return printWriter;
	}

	public void setPrintWriter(PrintWriter printWriter) {
		this.printWriter = printWriter;
	}

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(String port) {
		this.port = Integer.parseInt(port);
	}

	public int getUserCount() {
		return this.userCount;
	}

	public void setUserCount(String count) {
		this.userCount = Integer.parseInt(count);
	}

	public int getUserOffset() {
		return this.userOffset;
	}

	public void setUserOffset(String offset) {
		this.userOffset = Integer.parseInt(offset);
	}
}