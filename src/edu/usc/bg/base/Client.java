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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import com.mitrallc.sql.KosarSoloDriver;

import kosar.AsyncSocketServer;
import kosar.CoreClient;
import edu.usc.bg.BGMainClass;
import edu.usc.bg.Distribution;
import edu.usc.bg.KillThread;
import edu.usc.bg.MonitoringThread;
import edu.usc.bg.Worker;
import edu.usc.bg.generator.Fragmentation;
import edu.usc.bg.measurements.MyMeasurement;
import edu.usc.bg.measurements.StatsPrinter;
import edu.usc.bg.server.BGServer;
import edu.usc.bg.server.ClientInfo;
import edu.usc.bg.server.RequestHandler;
import edu.usc.bg.validator.ValidationMainClass;
import edu.usc.bg.workloads.CoreWorkload;
import edu.usc.bg.workloads.loadActiveThread;

class CustomWarmupThread extends Thread{
	DB db;
	int start,end;
	boolean insertImage = false;
	
	public CustomWarmupThread(DB _db, int i, int j) {
		// TODO Auto-generated constructor stub
		db=_db;
		start=i;
		end=j;
		insertImage = Boolean.parseBoolean(db.getProperties().getProperty("insertimage"));
	}
	public void run(){
		try{

			HashMap<String, ByteIterator> result;//= new HashMap<String, ByteIterator>();
			Vector<HashMap<String,ByteIterator>> r;//=new Vector<HashMap<String,ByteIterator>>();

			for (int ii=start; ii>=end;ii--){
				//System.out.println("i="+ii);
//				int x=CoreWorkload.myMemberObjs[ii].get_uid();
				int x = ii;
				//	System.out.println("id="+x);
				result= new HashMap<String, ByteIterator>();


				db.viewProfile(0, x, result, insertImage, false);
				r=new Vector<HashMap<String,ByteIterator>>();
				db.listFriends(0, x, null, r, insertImage, false);
				r=new Vector<HashMap<String,ByteIterator>>();
				db.viewFriendReq(x, r, insertImage, false);
				if (ii%1000==0){
					System.out.println("Still working"+ii);
				}
			}
		}
		catch (Exception ex){
			ex.printStackTrace(System.out);
			System.exit(0);
		}

	}
}

/**
 * This class keeps a track of the frequency of access for each member during
 * the workload benchmark execution.
 * 
 * @author sumita barahmand
 */
class FreqElm {
	int uid = 0;
	int frequency = 0;

	public FreqElm(int userid, int userfrequency) {
		uid = userid;
		frequency = userfrequency;
	}

	public int getFrequency() {
		return frequency;
	}

	public int getUserId() {
		return uid;
	}

}

/**
 * This class is needed for sorting the frequency of access to members for
 * computing interested percentage frequencies.
 * 
 * @author sumita barahmand
 */
class FreqComparator implements Comparator<FreqElm> {
	public int compare(FreqElm o1, FreqElm o2) {
		if (o1.frequency > o2.frequency)
			return 1;
		return 0;
	}
}

/////////////////////////
class VisualizationThread extends Thread {
	int          serverPort  ;
	static ServerSocket serverSocket = null;
	static boolean      isStopped    = false;
	ExecutorService threadPool = Executors.newFixedThreadPool(100);
	StatusThread status;

	VisualizationThread( int port,StatusThread t)
	{
		status=t;
		serverPort=port;
		try {
			serverSocket = new ServerSocket(serverPort);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public static void stopServer()
	{
		if(!isStopped)
		{
			isStopped = true;
			try {
				serverSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
	}
	public boolean isStopped() {
		return isStopped;
	}

	public void run()
	{
		while(! status.alldone){
			//SocketIO 
			Socket clientSocket = null;
			try {
				clientSocket =serverSocket.accept();

				//				clientSocket =new SocketIO(serverSocket.accept());

				this.threadPool.execute( new WorkerRunnable(clientSocket,status));
			} catch (IOException e) {

				System.out.println("Closing Visualization thread socket");
			}
		}
		//stopServer();
		this.threadPool.shutdown();
		System.out.println("Visualization thread has Stopped...") ;

	}
}

class WorkerRunnable implements Runnable{

	Socket clientSocket = null;
	StatusThread status   = null;
	String separator="|";
	static AtomicInteger numSocilites= new AtomicInteger(Client.threadCount);


	public WorkerRunnable(Socket clientSocket, StatusThread t) {
		this.clientSocket = clientSocket;
		status=t;
	}

	public void run() {
		String msg="";
		try {
			InputStream is = clientSocket.getInputStream();
			OutputStream os = clientSocket.getOutputStream();

			// Receiving
			byte[] lenBytes = new byte[4];
			is.read(lenBytes, 0, 4);
			int len = (((lenBytes[3] & 0xff) << 24) | ((lenBytes[2] & 0xff) << 16) |
					((lenBytes[1] & 0xff) << 8) | (lenBytes[0] & 0xff));
			byte[] receivedBytes = new byte[len];
			is.read(receivedBytes, 0, len);
			msg = new String(receivedBytes, 0, len);
			//InputStream input  = clientSocket.getInputStream();
			//OutputStream output = clientSocket.getOutputStream();
			//			
			//		PrintWriter out=null;

			//			out = new PrintWriter(clientSocket.getOutputStream(), true);

			//			BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
			//			byte buf[]= new byte[300];
			//			int i=in.read(buf);
			//			int l=clientSocket.getDataInputStream().read(buf, 0, 4);
			//			int ll=ByteBuffer.wrap(buf).getInt();
			//			System.out.println(ll);
			//			msg=new String(buf,0,i);
			//			byte[] b=clientSocket.readBytes();
			//			msg=new String(b);
			//ByteBuffer.wrap(b).toString();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//String arg[] = msg.split(separator);



		if (msg.contains( "GetData")) 
		{// MSG_GETDATA 
			//// Socialites | actions | response time | percentage
			String summ=status.summary;
			int threadcount;
			double total=0;
			int index=summ.indexOf('=');
			int i=0;
			while (index!=-1)
			{
				i++;
				summ=summ.substring(index+1);
				double responsetime=0;
				if (Double.parseDouble(summ.substring(0,summ.indexOf(']')))>0)
					responsetime=Double.parseDouble(summ.substring(0,summ.indexOf(']')));
				total=total+responsetime;
				index=summ.indexOf('=');
			}
			total=total/1000;
			//if (flag==false)
			//{
			//flag=true;
			//threadcount=1;
			//}
			//else
			//threadcount=Client.threadCount;

			if (i==0)
				i=1;
			String data="GetData"+separator+ numSocilites.get()+ separator + (int)status.curactthroughput + separator + (int)total/i + separator + (int)MyMeasurement.getSatisfyingPerc();
			sendResponse(data);
		}

		else if (msg.contains("SetConfidence"))
		{// MSG_SETCONFIDENCE | arg
			//OnSetConfidenceMsg(arg);
			sendResponse("SetConfidence");
		}
		else if (msg.contains("SetResponseTime"))
		{// MSG_SETCONFIDENCE | arg
			//OnSetConfidenceMsg(arg);
			sendResponse("SetResponseTime");
		}
		else if (msg.contains("SetSociaty"))
		{
			// MSG_SETCONFIDENCE | arg
			//OnSetConfidenceMsg(arg);
			int count=Integer.parseInt((msg.substring(msg.indexOf('|')+1)).trim());
			if (count>numSocilites.get())
			{
				incermentSocilites(Math.abs(count-numSocilites.get()));
			}
			else if (count<numSocilites.get())
			{
				decrementSocilites(Math.abs(count-numSocilites.get()));
			}

			sendResponse("SetSociaty");
		}



		else
			System.out.println("msg error");




	}

	private void sendResponse(String data) {
		// TODO Auto-generated method stub
		byte[] toSendBytes = data.getBytes();
		int toSendLen = toSendBytes.length;
		OutputStream os;
		try {
			os = clientSocket.getOutputStream();

			byte[] toSendLenBytes = new byte[4];
			toSendLenBytes[0] = (byte)((toSendLen >> 0)& 0xff);
			toSendLenBytes[1] = (byte)((toSendLen >> 8) & 0xff);
			toSendLenBytes[2] = (byte)((toSendLen >> 16) & 0xff);
			toSendLenBytes[3] = (byte)((toSendLen >> 24) & 0xff);
			os.write(toSendLenBytes);

			//		os.write(ByteBuffer.allocate(4).putInt(yourInt).array());
			os.write(toSendBytes);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	synchronized private void decrementSocilites(int count)  {
		// TODO Auto-generated method stub

		for (int i=numSocilites.get()-1;i>=numSocilites.get()-count;i--) {

			Thread t=status._threads.get(i);
			((ClientThread) t).set_terminated(true);
			//		 status._threads.remove(i);

		}
		numSocilites.set(numSocilites.get()-count);
		//	Client.threadCount=status._threads.size();
	}

	synchronized private void incermentSocilites(int count) {
		// TODO Auto-generated method stub
		ClientThread threadInfo=(ClientThread)status._threads.get(0);
		String dbname=threadInfo._props.getProperty(Client.DB_CLIENT_PROPERTY, Client.DB_CLIENT_PROPERTY_DEFAULT);

		for (int threadid = Client.threadCount; threadid < count+Client.threadCount; threadid++) {
			DB db = null;
			try {
				db = DBFactory.newDB(dbname,threadInfo._props);
			} catch (UnknownDBException e) {
				System.out.println("Unknown DB " + dbname);
				//System.exit(0);


			}

			Thread t = new ClientThread(db, threadInfo._dotransactions, status._workload,
					threadid, Client.threadCount+count, threadInfo._props, 0,
					threadInfo._target, false);

			status._threads.add(t);
			boolean started = false;
			started = ((ClientThread) t).initThread();
			while (!started) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}

		}




		for (int threadid = Client.threadCount; threadid <count+Client.threadCount; threadid++)
		{
			status._threads.get(threadid).start();
		}

		Client.threadCount=status._threads.size();
		numSocilites.set(numSocilites.get()+count);
	}

}



//////////////////////////////////

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */


class StatusThread extends Thread {
	Vector<Thread> _threads;
	Workload _workload;
	double curactthroughput;
	boolean alldone;
	String summary="";


	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime = 10000;

	public StatusThread(Vector<Thread> threads, Workload workload) {
		_threads = threads;
		_workload = workload;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run() {
		long st = System.currentTimeMillis();

		long lasten = st;
		long lasttotalops = 0;
		long lasttotalacts = 0;


		do {
			alldone = true;

			int totalops = 0;
			int totalacts = 0;

			// terminate this thread when all the worker threads are done
			for (Thread t : _threads) {
				if (t.getState() != Thread.State.TERMINATED) {
					alldone = false;
				}

				ClientThread ct = (ClientThread) t;
				totalops += ct.getOpsDone();
				totalacts += ct.getActsDone();
			}

			long en = System.currentTimeMillis();

			long interval = 0;
			interval = en - st;

			double curthroughput = 1000.0 * (((double) (totalops - lasttotalops)) / ((double) (en - lasten)));
			curactthroughput = 1000.0 * (((double) (totalacts - lasttotalacts)) / ((double) (en - lasten)));
			lasttotalops = totalops;
			lasttotalacts = totalacts;
			lasten = en;

			DecimalFormat d = new DecimalFormat("#.##");

			if (totalacts == 0) {
				System.out.print(" " + (interval / 1000) + " sec: " + totalacts
						+ " actions; ");
			} else {
				System.out.print(" " + (interval / 1000) + " sec: " + totalacts
						+ " actions; " + d.format(curactthroughput)
						+ " current acts/sec; ");
			}

			if (totalops == 0) {
				summary=MyMeasurement.getSummary();
				System.out.println(" " + (interval / 1000) + " sec: "
						+ totalops + " operations; "
						+ summary);
			} else {
				summary=MyMeasurement.getSummary();
				System.out.println(" " + (interval / 1000) + " sec: "
						+ totalops + " operations; " + d.format(curthroughput)
						+ " current ops/sec; " + summary);
			}
			try {
				sleep(sleeptime);
			} catch (InterruptedException e) {
				// do nothing
			}

		} while (!alldone && !_workload.isStopRequested());
		alldone=true;
		VisualizationThread.stopServer();
	}
}


/**
 * Main class for executing BG.
 */
public class Client {
	public static CountDownLatch threadsStart;
	public static long experimentStartTime=0;
	//public static AtomicInteger threadcount= new AtomicInteger(0); 
	static int visualizerPort=6001;
	final public static int PARTITIONED=0; 
	final public static int RETAIN=1; 
	final public static int DELEGATE=2; 
	final public static int HYBRID_RETAIN=3; 
	final public static int HYBRID_DELEGATE=4;
	public static int BENCHMARKING_MODE=PARTITIONED;
	private static final String BENCHMARKENDMSG = " THEEND. ";
	private static final String EXECUTIONDONEMSG = "EXECUTIONDONE";
	public static final String SHUTDOWNMSG="SHUTDOWN!!!";
	public static final String OPERATION_COUNT_PROPERTY = "operationcount";
	public static final String OPERATION_COUNT_PROPERTY_DEFAULT = "0";
	public static final String NUM_LOAD_THREAD_PROPERTY = "numloadthreads";
	public static final String USER_COUNT_PROPERTY = "usercount";
	public static final String USER_COUNT_PROPERTY_DEFAULT = "0";
	public static final String USER_OFFSET_PROPERTY = "useroffset";
	public static final String USER_OFFSET_PROPERTY_DEFAULT = "0";
	public static final String RESOURCE_COUNT_PROPERTY = "resourcecountperuser";
	public static final String RESOURCE_COUNT_PROPERTY_DEFAULT = "0";
	public static final String FRIENDSHIP_COUNT_PROPERTY = "friendcountperuser";
	public static final String FRIENDSHIP_COUNT_PROPERTY_DEFAULT = "0";
	public static final String CONFPERC_COUNT_PROPERTY = "confperc";
	public static final String RATING_MODE_PROPERTY = "ratingmode";
	public static final String RATING_MODE_PROPERTY_DEFAULT = "false";
	// needed when rating is happening
	public static final String EXPECTED_LATENCY_PROPERTY = "expectedlatency";
	public static final String EXPECTED_LATENCY_PROPERTY_DEFAULT = "1.3";
	public static final String EXPECTED_AVAILABILITY_PROPERTY = "expectedavailability"; // for
	// freshness
	public static final String EXPECTED_AVAILABILITY_PROPERTY_DEFAULT = "1.3";
	public static final String EXPORT_FILE_PROPERTY = "exportfile";
	public static final String FEED_LOAD_PROPERTY = "feedload";
	public static final String FEED_LOAD_DEFAULT_PROPERTY = "false";

	public static final String WORKLOAD_PROPERTY = "workload";
	public static final String USER_WORKLOAD_PROPERTY = "userworkload";
	public static final String FRIENDSHIP_WORKLOAD_PROPERTY = "friendshipworkload";
	public static final String RESOURCE_WORKLOAD_PROPERTY = "resourceworkload";

	// in the benchmark phase, these params will be queried from the data store
	// initially and will be set
	public static final String INIT_STATS_REQ_APPROACH_PROPERTY = "initapproach";
	// can be QUERYDATA, CATALOGUQ, QUERYBITMAP
	public static final String INIT_USER_COUNT_PROPERTY = "initialmembercount";
	public static final String INIT_USER_COUNT_PROPERTY_DEFAULT = "0";
	public static final String INIT_FRND_COUNT_PROPERTY = "initialfriendsperuser";
	public static final String INIT_PEND_COUNT_PROPERTY = "initialpendingsperuser";
	public static final String INIT_FRND_COUNT_PROPERTY_DEFAULT = "0";
	public static final String INIT_RES_COUNT_PROPERTY = "initialresourcesperuser";
	public static final String INIT_RES_COUNT_PROPERTY_DEFAULT = "0";

	public static final String INSERT_IMAGE_PROPERTY = "insertimage";
	public static final String INSERT_IMAGE_PROPERTY_DEFAULT = "false";
	public static final String IMAGE_SIZE_PROPERTY = "imagesize";
	public static final String IMAGE_SIZE_PROPERTY_DEFAULT = "2";
	public static final String THREAD_CNT_PROPERTY = "threadcount";
	public static final String THREAD_CNT_PROPERTY_DEFAULT = "1";
	public static final String THINK_TIME_PROPERTY = "thinktime";
	public static final String THINK_TIME_PROPERTY_DEFAULT = "0";
	public static final String INTERARRIVAL_TIME_PROPERTY = "interarrivaltime";
	public static final String INTERARRIVAL_TIME_PROPERTY_DEFAULT = "0";
	public static final String WARMUP_OP_PROPERTY = "warmup";
	public static final String WARMUP_OP_PROPERTY_DEFAULT = "0";
	public static final String WARMUP_THREADS_PROPERTY = "warmupthreads";
	public static final String WARMUP_THREADS_PROPERTY_DEFAULT = "10";
	public static final String PORT_PROPERTY = "port";
	public static final String PORT_PROPERTY_DEFAULT = "10655";
	public static final String DB_CLIENT_PROPERTY = "db";
	public static final String DB_CLIENT_PROPERTY_DEFAULT = "fake.TestClient";
	public static final String MACHINE_ID_PROPERTY = "machineid";
	public static final String CLIENTS_INFO_PROPERTY = "clients";
	public static final String MACHINE_ID_PROPERTY_DEFAULT = "0";
	public static final String NUM_BG_PROPERTY = "numclients";
	public static final String 	BENCHMARKING_MODE_PROPERTY = "benchmarkingmode";
	public static final String 	BENCHMARKING_MODE_PROPERTY_DEFAULT = "partitioned";
	public static final String NUM_MEMBERS_PROPERTY = "nummembers";
	public static final String NUM_SOCKETS_PROPERTY = "numsockets";
	public static final String NUM_BG_PROPERTY_DEFAULT = "1";
	public static final String MONITOR_DURATION_PROPERTY = "monitor";
	public static final String MONITOR_DURATION_PROPERTY_DEFAULT = "0";
	public static final String TARGET__PROPERTY = "target";
	public static final String TARGET_PROPERTY_DEFAULT = "0";
	public static final String LOG_DIR_PROPERTY = "logdir";
	public static final String LOG_DIR_PROPERTY_DEFAULT ="." ;//"C:/Users/yaz/Documents/BGTest_Communication_Infrastructure/logs"
	public static final String PROBS_PROPERTY = "probs";
	public static final String PROBS_PROPERTY_DEFAULT = "";
	public static final String ENFORCE_FRIENDSHIP_PROPERTY = "enforcefriendship";
	public static final String ENFORCE_FRIENDSHIP_PROPERTY_DEFAULT = "false";
	public static Workload workload = null;
	public static Semaphore releaseWorkers= new Semaphore(0);

	//properties for open simulation
	public static final String SIMTYPE_PROPERTY  = "simulationtype";
	public static final String DISTRITYPE_PROPERTY  = "distributiontype";
	public static final String LAMBDA_PROPERTY  = "lambda"; //req/sec
	public static final String SIM_WARMUP_TIME_PROPERTY  = "simulationwarmuptime"; //s

	public static final String SIMTYPE_PROPERTY_DEFAULT  = "closed";
	public static final String DISTRITYPE_PROPERTY_DEFAULT  = "1";
	public static final String LAMBDA_PROPERTY_DEFAULT  = "10"; //req/sec
	public static final String SIM_WARMUP_TIME_PROPERTY_DEFAULT  = "0"; //ms

	//DB.get constants
	private final String AVG_PENDING_PER_USER = "avgpendingperuser";
	private final String AVG_FRNDS_PER_USER = "avgfriendsperuser";
	private final String RESOURCES_PER_USER = "resourcesperuser";
	private final String USERCOUNT = "usercount";

	private static int CLIENT_THREADS_INIT_BATCH = 50;

	//Benchmark stats parameters
	public static final String TimeTillFirstDeath = "TimeTillFirstDeath";
	public static final String OpsTillFirstDeath = "OpsTillFirstDeath";
	public static final String ActsTillFirstDeath = "ActsTillFirstDeath";
	public static final String NumStaleOps = "NumStaleOps";
	public static final String NumReadOps = "NumReadOps";
	public static final String NumPrunedOps = "NumPruned";
	public static final String ValidationTime = "ValidationTime";
	public static final String FreshnessConfidence = "FreshnessConfidence";	
	public static final String NumWriteOps = "NumWriteOps";
	public static final String NumProcessedOps = "NumProcessed";



	/**
	 * The maximum amount of time (in seconds) for which the benchmark will be
	 * run.
	 */
	public static final String MAX_EXECUTION_TIME = "maxexecutiontime";
	public static final String MAX_EXECUTION_TIME_DEFAULT = "0";

	public static int machineid = 0;
	public static int numBGClients = 1;
	public static int numMembers = 0;
	public static String logDir;
	public static int threadCount = 1;

	public static void usageMessage() {
		System.out.println("Usage: java BG [options]");
		System.out.println("Options:");
		System.out
		.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n"
				+ "             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out
		.println("  -loadindex:  run the loading phase of the workload and create index structures once loadig completed");
		System.out
		.println("  -schema:  Create the schema used for the load phase of the workload");
		System.out
		.println("  -dropupdates:  Drops friendships if available else create the schema used for the load phase of the workload");
		System.out
		.println("  -loadfriends:  Create friendships if users available else runs the load phase");
		System.out
		.println("  -testdb:  Tests the database by trying to connect to it");
		System.out
		.println("  -stats:  Queries the database stats such as usercount, avgfriendcountpermember and etc.");
		System.out
		.println("  -t:  run the benchmark phase of the workload (default)");
		System.out
		.println("  -db dbname: specify the name of the DB to use  - \n"
				+ "              can also be specified as the \"db\" property using -p");
		System.out
		.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out
		.println("                   be specified, and will be processed in the order specified");
		System.out
		.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out
		.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out
		.println("  -s:  show status during run (default: no status)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out
		.println("  "
				+ USER_WORKLOAD_PROPERTY
				+ ", "
				+ FRIENDSHIP_WORKLOAD_PROPERTY
				+ " ,"
				+ RESOURCE_WORKLOAD_PROPERTY
				+ ": the name of the workload class to use for -load or -loadindex (e.g. edu.usc.bg.workloads.CoreWorkload)");
		System.out
		.println("  "
				+ WORKLOAD_PROPERTY
				+ ": the name of the workload class to use for -t (e.g. edu.usc.bg.workloads.CoreWorkload)");
		System.out.println("");
		System.out
		.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out
		.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out
		.println("use the \"usercount\" and \"useroffset\" properties to divide up the records to be inserted");
		System.out
		.println("You can also load data store using the dzipfian fragments by using \"requestdistribution=dzipfian\" ");
		System.out
		.println("and \"zipfianmean=0.27\", for this approach you need to specify the number of bgclients, the current bgclient machineid and the rates of all the involved machienids");
	}

	/**
	 * check if the workload parameters exist
	 */
	public static boolean checkRequiredProperties(Properties props,
			boolean dotransactions, boolean doSchema, boolean doTestDB,
			boolean doStats) {
		if (dotransactions) {
			// benchmark phase
			if (props.getProperty(WORKLOAD_PROPERTY) == null) {
				System.out.println("Missing property: " + WORKLOAD_PROPERTY);
				return false;
			}
		} else {
			if (!doSchema && !doTestDB && !doStats) {
				// if schema is already created and are in load phase
				if (props.getProperty(USER_WORKLOAD_PROPERTY) == null
						|| props.getProperty(FRIENDSHIP_WORKLOAD_PROPERTY) == null
						|| props.getProperty(RESOURCE_WORKLOAD_PROPERTY) == null) {
					System.out.println("Missing property: "
							+ USER_WORKLOAD_PROPERTY + ", "
							+ FRIENDSHIP_WORKLOAD_PROPERTY + " ,"
							+ RESOURCE_WORKLOAD_PROPERTY);
					return false;
				}
			}
		}

		return true;
	}

	/**
	 * Exports the measurements to either console or a file using the printer.
	 * It also loads the statistics to a file and sends them to the coordinator
	 * if BG is in rating mode.
	 * 
	 * @param opcount
	 *            The total session count.
	 * @param actcount
	 *            The total action count.
	 * @param runtime
	 *            The duration of the benchmark.
	 * @param outpS
	 *            Used when the statistics are written on a socket for the
	 *            coordinator.
	 * @param props
	 *            The properties of the benchmark.
	 * @param benchmarkStats
	 *            Contains the statistics of the benchmark.
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */

	/**
	 * @param props
	 * @param opcount
	 * @param actcount
	 * @param runtime
	 * @param benchmarkStats
	 * @param outpS
	 * @throws IOException
	 */
	private static void printFinalStats(Properties props, int opcount, int actcount, long runtime, ClientDataStats benchmarkStats, PrintWriter outpS) throws IOException {
		OutputStream out=null;
		if(benchmarkStats == null) {
			System.out.println("The benchmark statistics were not populated.");
			return;
		}
		StatsPrinter printer = null;
		try {
			// compute stats
			double sessionthroughput = 1000.0 * ((double) opcount)
					/ ((double) runtime);
			double actthroughput = 1000.0 * ((double) actcount)
					/ ((double) runtime);

			// not considering the ramp down
			double sessionthroughputTillFirstDeath = 0;
			if (benchmarkStats.getTimeTillFirstDeath() != null
					&& benchmarkStats.getOpsTillFirstDeath() != null) {
				sessionthroughputTillFirstDeath = 1000.0
						* ((double) benchmarkStats.getOpsTillFirstDeath())
						/ ((double) benchmarkStats.getTimeTillFirstDeath());
			}
			double actthroughputTillFirstDeath = 0;
			if (benchmarkStats.getTimeTillFirstDeath() != null
					&& benchmarkStats.getActsTillFirstDeath() != null) {
				actthroughputTillFirstDeath = 1000.0
						* ((double) benchmarkStats.getActsTillFirstDeath())
						/ ((double) benchmarkStats.getTimeTillFirstDeath());
			}

			// write to socket
			// only when BG is in rating mode is true and it writes to the
			// socket communicating with the coordinator
			if (props.getProperty(Client.RATING_MODE_PROPERTY,
					Client.RATING_MODE_PROPERTY_DEFAULT).equals("true")) {
				outpS.print(" OVERALLRUNTIME(ms):" + runtime + " ");
				outpS.print(" OVERALLOPCOUNT(SESSIONS):" + opcount + " ");
				outpS.print(" OVERALLTHROUGHPUT(SESSIONS/SECS):"
						+ sessionthroughput + " ");
				outpS.print(" OVERALLOPCOUNT(ACTIONS):" + actcount);
				outpS.print(" OVERALLTHROUGHPUT(ACTIONS/SECS):" + actthroughput
						+ " ");
				outpS.flush();

				String messageBenchmarkStats="";
				Map<String,String> statsAndMessage = new HashMap<String,String>(); 

				// the run time and throughput right when the first thread
				// dies

				statsAndMessage.put("TimeTillFirstDeath", "RAMPEDRUNTIME(ms)");
				statsAndMessage.put("OpsTillFirstDeath", "RAMPEDRUNTIME(ms)");
				statsAndMessage.put("ActsTillFirstDeath", "RAMPEDOPCOUNT(ACTIONS)");
				statsAndMessage.put("NumReadOps", "READ(OPS)");
				statsAndMessage.put("NumPruned", "PRUNED(OPS)");
				statsAndMessage.put("NumProcessed", "PROCESSED(OPS)");
				statsAndMessage.put("NumWriteOps", "WRITE(OPS)");
				statsAndMessage.put("ValidationTime", "VALIDATIONTIME(MS)");
				statsAndMessage.put("NumStaleOps", "NUMSTALE(OPS)");
				statsAndMessage.put("FreshnessConfidence", "FRESHNESSCONF(%)");

				printStatsViaMap(benchmarkStats,statsAndMessage, outpS);

				if (benchmarkStats.getTimeTillFirstDeath() != null) {
					if(benchmarkStats.getOpsTillFirstDeath() != null) {
						messageBenchmarkStats += " RAMPEDTHROUGHPUT(SESSIONS/SECS):" + sessionthroughputTillFirstDeath + " ";
					}

					if(benchmarkStats.getActsTillFirstDeath() != null) {
						messageBenchmarkStats += " RAMPEDTHROUGHPUT(ACTIONS/SECS):"
								+ actthroughputTillFirstDeath + " ";
					}
				}


				if (benchmarkStats.getNumReadOps() != null) {
					messageBenchmarkStats += " READ(OPS):"
							+ benchmarkStats.getNumReadOps() + " ";
				}

				else {
					messageBenchmarkStats += " READ(OPS):0 ";
				}
				if (benchmarkStats.getNumStaleOps()!= null) {
					messageBenchmarkStats += " NUMSTALE(OPS):"
							+ ((double) benchmarkStats.getNumStaleOps())
							+ " ";
				} 
				else {
					messageBenchmarkStats += " NUMSTALE(OPS):0 ";
				}


				if (benchmarkStats.getNumStaleOps() != null && benchmarkStats.getNumReadOps() > 0) {
					messageBenchmarkStats += " STALENESS(OPS):" + (((double) benchmarkStats.getNumStaleOps()) / benchmarkStats.getNumReadOps()) + " ";

				} else {
					messageBenchmarkStats += " STALENESS(OPS):0 ";
				}

				outpS.print(messageBenchmarkStats);
				outpS.flush();

				double satisfyingPerc = MyMeasurement.getSatisfyingPerc();
				outpS.print(" SATISFYINGOPS(%):" + satisfyingPerc + " ");
				outpS.flush();
				outpS.print(BENCHMARKENDMSG);
				outpS.flush();
			}

			/* write to file if no destination file is provided the results will be written to stdout.*/

			String exportFile = props.getProperty(Client.EXPORT_FILE_PROPERTY);
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}
			try {
				printer = new StatsPrinter(out);
			} catch (Exception e) {
				e.printStackTrace(System.out);

			}

			// write the date to the beginning of the output file
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			Date date = new Date();
			printer.write("DATE: " + dateFormat.format(date).toString());
			// write the workload details to the beginning of the file
			String params = "Runtime configuration parameters (those missing from the list have default values):\n";
			@SuppressWarnings("unchecked")
			Enumeration<Object> em = (Enumeration<Object>) props
			.propertyNames();
			while (em.hasMoreElements()) {
				String str = (String) em.nextElement();
				params += ("\n" + str + ": " + props.get(str));
			}

			params += "\nSanity Init parameters:\n";
			params += "sanityMemberCount="
					+ props.getProperty(Client.INIT_USER_COUNT_PROPERTY,
							Client.INIT_USER_COUNT_PROPERTY_DEFAULT)
							+ " ,sanityAvgFriendsPerUserCount="
							+ props.getProperty(Client.INIT_FRND_COUNT_PROPERTY,
									Client.INIT_FRND_COUNT_PROPERTY_DEFAULT)
									+ " ,sanityAvgPendingsPerUserCount="
									+ props.getProperty(Client.INIT_PEND_COUNT_PROPERTY,
											Client.INIT_PEND_COUNT_PROPERTY)
											+ " ,sanityResourcePerUserCount="
											+ props.getProperty(Client.INIT_RES_COUNT_PROPERTY,
													Client.INIT_RES_COUNT_PROPERTY_DEFAULT);

			printer.write(params);
			printer.write("OVERALL", "RunTime(ms)", runtime);
			printer.write("OVERALL", "opcount(sessions)", opcount);
			printer.write("OVERALL", "Throughput(sessions/sec)",
					sessionthroughput);
			printer.write("OVERALL", "actcount(actions)", actcount);

			printer.write("OVERALL", "Throughput(actions/sec)", actthroughput);


			System.out.println("OVERALLOPCOUNT(SESSIONS):" + opcount);
			System.out.println("OVERALLTHROUGHPUT(SESSIONS/SECS):"
					+ sessionthroughput);
			System.out.println("OVERALLOPCOUNT(ACTIONS):" + actcount);
			System.out.println("OVERALLTHROUGHPUT(ACTIONS/SECS):"
					+ actthroughput);

			//if (benchmarkStats != null) {
			// the run time and throughput right when the first thread dies
			if (benchmarkStats.getTimeTillFirstDeath() != null) {
				printer.write("UntilFirstThreadsDeath", "RunTime(ms)",
						benchmarkStats.getTimeTillFirstDeath());
			}
			if (benchmarkStats.getOpsTillFirstDeath() != null) {
				printer.write("UntilFirstThreadsDeath",
						"opcount(sessions)",
						benchmarkStats.getOpsTillFirstDeath());
				System.out.println("RAMPEDOVERALLOPCOUNT(SESSIONS):"
						+ benchmarkStats.getOpsTillFirstDeath());
			}
			if (benchmarkStats.getTimeTillFirstDeath() != null
					&& benchmarkStats.getOpsTillFirstDeath() != null) {
				printer.write("UntilFirstThreadsDeath",
						"Throughput(sessions/sec)",
						sessionthroughputTillFirstDeath);
				System.out
				.println("RAMPEDOVERALLTHROUGHPUT(SESSIONS/SECS):"
						+ sessionthroughputTillFirstDeath);
			}
			if (benchmarkStats.getActsTillFirstDeath() != null) {
				printer.write("UntilFirstThreadsDeath", "opcount(actions)",
						benchmarkStats.getActsTillFirstDeath());
				System.out.println("RAMPEDOVERALLOPCOUNT(ACTIONS):"
						+ benchmarkStats.getActsTillFirstDeath());
			}
			if (benchmarkStats.getTimeTillFirstDeath() != null
					&& benchmarkStats.getActsTillFirstDeath() != null) {
				printer.write("UntilFirstThreadsDeath",
						"Throughput(actions/sec)",
						actthroughputTillFirstDeath);
				System.out.println("RAMPEDOVERALLTHROUGHPUT(ACTIONS/SECS):"
						+ actthroughputTillFirstDeath);
			}

			if (benchmarkStats.getNumReadOps() != null)
				printer.write("OVERALL", "Read(ops)",
						benchmarkStats.getNumReadOps());
			if (benchmarkStats.getNumStaleOps() != null)
				printer.write("OVERALL", "StaleRead(ops)",
						benchmarkStats.getNumStaleOps());
			if (benchmarkStats.getNumReadOps() != null
					&& benchmarkStats.getNumStaleOps() != null) {
				printer.write("OVERALL",
						"Staleness(staleReads/totalReads)",
						((double) benchmarkStats.getNumStaleOps())
						/ benchmarkStats.getNumReadOps());
			}
			if (benchmarkStats.getNumReadSessions() != null)
				printer.write("OVERALL", "Read(sessions)",
						benchmarkStats.getNumReadSessions());
			if (benchmarkStats.getNumStaleSessions() != null)
				printer.write("OVERALL", "StaleRead(sessions)",
						benchmarkStats.getNumStaleSessions());
			if (benchmarkStats.getNumReadSessions() != null
					&& benchmarkStats.getNumReadSessions() != null)
				printer.write("OVERALL",
						"Staleness(staleSessions/totalSessions)",
						((double) benchmarkStats.getNumStaleSessions())
						/ benchmarkStats.getNumStaleSessions());
			if (benchmarkStats.getNumPrunedOps() != null)
				printer.write("OVERALL", "Pruned(ops)",
						benchmarkStats.getNumPrunedOps());
			if (benchmarkStats.getValidationTime() != null)
				printer.write("OVERALL", "Validationtime(ms)",
						benchmarkStats.getValidationTime());
			if (benchmarkStats.getDumpAndValidateTime() != null)
				printer.write("OVERALL", "DumpAndValidationtime(ms)",
						benchmarkStats.getDumpAndValidateTime());
			if (benchmarkStats.getDumpTime() != null)
				printer.write("OVERALL", "dumpFilesToDB (ms)",
						benchmarkStats.getDumpTime());

			if (benchmarkStats.getFreshnessConfidence() != null)
				printer.write("OVERALL", " FRESHNESSCONF(%):",
						benchmarkStats.getFreshnessConfidence());

			//}// if benchmarkStats not null

			printer.write(MyMeasurement.getFinalResults());
			// Needed in case you want to print out frequency related stats
			printer.write(CoreWorkload.getFrequecyStats().toString());
		} finally {
			if (printer != null && out!=System.out) {
				printer.close();
			}
		}

	}

	/**
	 * @param benchmarkStats
	 * @param statsAndMessage
	 * @param outpS
	 */
	private static void printStatsViaMap(ClientDataStats benchmarkStats, Map<String, String> statsAndMessage, PrintWriter outpS) {
		Map<String,Double> benchmarkStatsMap = benchmarkStats.getStats();
		for(Entry<String, String> entry:statsAndMessage.entrySet()) {
			String paramName = (String) entry.getKey();
			String message = (String)entry.getValue();
			Double stat = benchmarkStatsMap.get(paramName);
			if(stat == null) {
				continue;
			}
			outpS.print(" " + message + ":" + stat + " ");
		}
		outpS.flush();
	}
	//	public void runBGCommandLineTest(String[] args, Socket listenerConnection) {
	//		String dbname;
	//		Properties props = new Properties();
	//		Properties fileprops = new Properties();
	//		//Enums & map of enum,obj
	//		boolean[] inputArguments = {true, false, false, false, false, false, false, false, false};
	//
	//		final int dotransactions = 0;
	//		final int doSchema = 1;
	//		final int doTestDB = 2;
	//		final int doStats = 3;
	//		final int doIndex = 4;
	//		final int doLoadFriends = 5;
	//		final int doDropUpdates = 6;
	//		final int doReset = 7;
	//		final int status = 8;
	//
	//		int target = 0;
	//
	//		// parse arguments
	//		if (args.length == 0) {
	//			usageMessage();
	//			System.out.println(EXECUTIONDONEMSG);
	//			return;
	//		}
	//
	//		readCmdArgs(args,props, inputArguments, fileprops);
	//
	//		//establish socket connection
	//		//		PrintWriter outpS = null;
	//		//		Scanner inSS = null;
	//		//		if (Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT)) == true && inputArguments[dotransactions]) {
	//		//			Object[] printerScanner = establishSocketConnection(listenerConnection,props);
	//		//			outpS = (PrintWriter) printerScanner[0];
	//		//			inSS = (Scanner) printerScanner[1];
	//		//		}
	//
	//		// overwrite file properties with properties from the command line
	//		for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
	//			String prop = (String) e.nextElement();
	//
	//			fileprops.setProperty(prop, props.getProperty(prop));
	//		}
	//
	//		props = fileprops;
	//
	//		if (!checkRequiredProperties(props, inputArguments[dotransactions], inputArguments[doSchema], inputArguments[doTestDB],
	//				inputArguments[doStats])) {
	//			System.out.println(EXECUTIONDONEMSG);
	//			return;
	//		}
	//
	//		machineid = Integer.parseInt(props.getProperty(MACHINE_ID_PROPERTY,
	//				MACHINE_ID_PROPERTY_DEFAULT));
	//		numBGClients = Integer.parseInt(props.getProperty(NUM_BG_PROPERTY,
	//				NUM_BG_PROPERTY_DEFAULT));
	//		logDir = props.getProperty(Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT);
	//		threadCount = Integer.parseInt(props.getProperty(THREAD_CNT_PROPERTY,THREAD_CNT_PROPERTY_DEFAULT));
	//		if (inputArguments[dotransactions]) {
	//			threadCount = threadCountForTransactions(props, threadCount);
	//		}
	//		else {
	//			threadCount = threadCountForNoTransactions(props, inputArguments, doSchema, doTestDB, doStats, doReset, threadCount);
	//		}
	//		
	//		
	//		
	//		// Start YAZ Server Code
	//		BGServer bb=null;
	//		if (numBGClients>1)  
	//		{
	//			if (props.getProperty(Client.CLIENTS_INFO_PROPERTY)==null ||props.getProperty(Client.NUM_MEMBERS_PROPERTY)==null  )
	//			{
	//				System.out.println("Argument is missing. Number of members and Client Info are required");
	//				System.exit(0);
	//			}
	//			numMembers=Integer.parseInt(props.getProperty(NUM_MEMBERS_PROPERTY));
	//			int numSockets=threadCount;
	//			if (props.getProperty(NUM_SOCKETS_PROPERTY)!=null)
	//				numSockets=Integer.parseInt(props.getProperty(NUM_SOCKETS_PROPERTY));
	//
	//			ConcurrentHashMap<Integer, ClientInfo> ClientInfoMap=PopulateClientInfo(props.getProperty(Client.CLIENTS_INFO_PROPERTY),Client.numBGClients);
	//
	//			//		
	//			bb = new BGServer(machineid,numBGClients,numMembers,0,ClientInfoMap, 0,numSockets);
	//
	//
	//		}
	//		else
	//		{ // one BGClient
	//			numMembers=Integer.parseInt(props.getProperty(Client.USER_COUNT_PROPERTY,USER_COUNT_PROPERTY_DEFAULT));
	//		}
	//		// End Yaz Server code
	//		// verify threadcount
	//		// get number of threads, target and db
	//		//		CoreWorkload.commandLineMode=true;
	//		CoreWorkload.enableLogging=false;
	//		threadCount = Integer.parseInt(props.getProperty(THREAD_CNT_PROPERTY,THREAD_CNT_PROPERTY_DEFAULT));
	//		if (inputArguments[dotransactions]) {
	//			threadCount = threadCountForTransactions(props, threadCount);
	//		}
	//		else {
	//			threadCount = threadCountForNoTransactions(props, inputArguments, doSchema, doTestDB, doStats, doReset, threadCount);
	//		}
	//
	//		//		int monitoringTime = Integer.parseInt(props.getProperty(MONITOR_DURATION_PROPERTY, MONITOR_DURATION_PROPERTY_DEFAULT));
	//		//		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, MAX_EXECUTION_TIME_DEFAULT));
	//		//		System.out.println("*****max execution time specified : " + maxExecutionTime);
	//		dbname = props.getProperty(DB_CLIENT_PROPERTY, DB_CLIENT_PROPERTY_DEFAULT);
	//		target = Integer.parseInt(props.getProperty(TARGET__PROPERTY, TARGET_PROPERTY_DEFAULT));
	//
	//		// compute the target throughput
	//		double targetperthreadperms = -1;
	//		if (target > 0) {
	//			double targetperthread = ((double) target) / ((double) threadCount);
	//			targetperthreadperms = targetperthread / 1000.0;
	//		}
	//		//		System.out.println("BG Client: ThreadCount =" + threadCount);
	//
	//		// show a warning message that creating the workload is taking a while
	//		// but only do so if it is taking longer than 2 seconds
	//		// (showing the message right away if the setup wasn't taking very long
	//		// was confusing people).	
	//
	//		Thread warningthread = new Thread() {
	//			public void run() {
	//				try {
	//					sleep(2000);
	//				} catch (InterruptedException e) {
	//					System.out.println(EXECUTIONDONEMSG);
	//					return;
	//				}
	//				System.out.println(" (might take a few minutes for large data sets)");
	//			}
	//		};
	//
	//		warningthread.start();
	//
	//		System.out.println();
	//		System.out.println("Loading workload...");
	//
	//		// load the workload
	//		ClassLoader classLoader = Client.class.getClassLoader();
	//
	//		Workload userWorkload = null;
	//		Workload friendshipWorkload = null;
	//		Workload resourceWorkload = null;
	//
	//		try {
	//			if (inputArguments[dotransactions]) {
	//				// check to see if the sum of all activity and action
	//				// proportions are 1
	//				//				if(!isTotalProbabilityOne(props)){
	//				//				return;	
	//				//				}
	//
	//				initializeDB(dbname, props, classLoader);
	//				if (numBGClients > 1 && bb != null) {
	//					BGServer.initializeWorkers(dbname, props);
	//				}
	//
	//				if (Client.workload!=null)
	//				{ // release workers, so they can start servicing requests 
	//					releaseWorkers.release(BGServer.NumWorkerThreads);
	//
	//				}
	//				else
	//				{
	//					System.out.println("Error Client workload is null can't start workers");
	//					System.exit(0);
	//				}
	//
	//				MyMeasurement.resetMeasurement();
	//				System.out.println("\nAfter init: " + new Date());
	//			}
	//		} catch (Exception e) {
	//			e.printStackTrace(System.out);
	//			System.out.println(EXECUTIONDONEMSG);
	//			return;
	//		}
	//
	//
	//
	//
	//
	//		warningthread.interrupt();
	//
	//		int numWarmpup = 0;
	//		if ((numWarmpup = Integer.parseInt(props.getProperty(WARMUP_OP_PROPERTY, WARMUP_OP_PROPERTY_DEFAULT))) != 0
	//				&& inputArguments[dotransactions]) {
	//			warmupPhase(dbname, props, inputArguments[dotransactions], threadCount, targetperthreadperms, numWarmpup);
	//			// we do not want the measurement for warmup to be counted in the
	//			// overall measurements
	//			MyMeasurement.resetMeasurement();
	//			System.out.println("\nAfter warmup: " + new Date());
	//		}// end warmup
	//
	//
	//
	//
	//		System.out.println("Connected");
	//
	//		// keep a thread of the worker client threads
	//		//		Vector<Thread> threads = new Vector<Thread>();
	//		//		if (inputArguments[dotransactions]) {
	//		//			String line = "";
	//		//			// wait till you get the start message from the coordinator
	//		//			if (Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT))) 
	//		//			{
	//		//				if(!housekeepingBeforeRunningBG(threadCount, outpS, inSS)) {
	//		//					return;
	//		//				}
	//		//			} 
	//		//			else if (threadCount == 0) {
	//		//				System.out.println("Invalid thread count: 0, system exiting");
	//		//				System.out.println(EXECUTIONDONEMSG);
	//		//				return;
	//		//			}
	//		//
	//		//			if(!performTransactions(dbname, props, inputArguments[dotransactions], inputArguments[status], threadCount, outpS, inSS, monitoringTime, maxExecutionTime, targetperthreadperms, threads)) {
	//		//				return;
	//		//			}
	//		//		} 
	//		//		else {
	//		//			DB db = null;
	//		//			if (inputArguments[doStats]) {
	//		//				executeDoStats(dbname, props);
	//		//			} else if (inputArguments[doTestDB]) {
	//		//				executeDoTestDB(dbname, props);
	//		//			}else if (inputArguments[doReset]){
	//		//				executeDoReset(props, classLoader);
	//		//			}else if (inputArguments[doSchema]) {
	//		//				executeDoSchema(dbname, props, inputArguments, doDropUpdates);
	//		//			} else {
	//		//				if(!executeDoLoad(dbname, props, inputArguments[dotransactions], inputArguments[doIndex], inputArguments[doLoadFriends], targetperthreadperms, classLoader)){
	//		//					return;
	//		//				}
	//		//			}
	//		//		}
	//		//System.exit(0);
	//
	//
	//		//		System.out.println(EXECUTIONDONEMSG);
	//		//		System.out.println("SHUTDOWN!!!");
	//	}

	class InitThread implements Runnable {
		ClientThread clientThread;
		InitThread(Thread t){
			clientThread=(ClientThread)t;

		}
		public void run(){
			boolean started=clientThread.initThread();
			while(!started){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(System.out);
				}
				started=clientThread.initThread();

			}
		}
	}

	// public static void main(String[] args) {
	public void runBG(String[] args, Socket listenerConnection) {
		String dbname;
		Properties props = new Properties();
		Properties fileprops = new Properties();
		//Enums & map of enum,obj
		boolean[] inputArguments = {true, false, false, false, false, false, false, false, false};

		final int dotransactions = 0;
		final int doSchema = 1;
		final int doTestDB = 2;
		final int doStats = 3;
		final int doIndex = 4;
		final int doLoadFriends = 5;
		final int doDropUpdates = 6;
		final int doReset = 7;
		final int status = 8;

		int target = 0;

		// parse arguments
		if (args.length == 0) {
			usageMessage();
			System.out.println(EXECUTIONDONEMSG);
			return;
		}

		readCmdArgs(args,props, inputArguments, fileprops);

		//establish socket connection
		PrintWriter outpS = null;
		Scanner inSS = null;
		if (Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT)) == true && inputArguments[dotransactions]) {
			Object[] printerScanner = establishSocketConnection(listenerConnection,props);
			outpS = (PrintWriter) printerScanner[0];
			inSS = (Scanner) printerScanner[1];
		}

		// overwrite file properties with properties from the command line
		for (Enumeration e = props.propertyNames(); e.hasMoreElements();) {
			String prop = (String) e.nextElement();

			fileprops.setProperty(prop, props.getProperty(prop));
		}

		props = fileprops;

		if (!checkRequiredProperties(props, inputArguments[dotransactions], inputArguments[doSchema], inputArguments[doTestDB],
				inputArguments[doStats])) {
			System.out.println(EXECUTIONDONEMSG);
			return;
		}
		Vector<Thread> threads = new Vector<Thread>();
		machineid = Integer.parseInt(props.getProperty(MACHINE_ID_PROPERTY,
				MACHINE_ID_PROPERTY_DEFAULT));
		numBGClients = Integer.parseInt(props.getProperty(NUM_BG_PROPERTY,
				NUM_BG_PROPERTY_DEFAULT));
		logDir = props.getProperty(Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT);
		threadCount = Integer.parseInt(props.getProperty(THREAD_CNT_PROPERTY,THREAD_CNT_PROPERTY_DEFAULT));
		BGServer bb=null;
		threadsStart= new CountDownLatch(threadCount);
		if (inputArguments[dotransactions]) {
			threadCount = threadCountForTransactions(props, threadCount);
			String benchmarkMode=props.getProperty(BENCHMARKING_MODE_PROPERTY,BENCHMARKING_MODE_PROPERTY_DEFAULT);
			boolean hybrid=false;
			boolean hybridWithCommunication=false;
			if (benchmarkMode.equalsIgnoreCase("retain")){
				BENCHMARKING_MODE=RETAIN;
				System.out.println("Running BG in Non-Partitioned (Retain) mode");
			}
			else if (benchmarkMode.equalsIgnoreCase("delegate")){
				System.out.println("Running BG in Non-Partitioned (Delegate) mode");
				BENCHMARKING_MODE=DELEGATE;
			}
			else if (benchmarkMode.contains("hybrid")){
				hybrid=true;
				boolean uniqueSocilites =Boolean.parseBoolean(props.getProperty(CoreWorkload.LOCK_READS_PROPERTY,Boolean.toString(CoreWorkload.lockReads) ));
				boolean enablelogging=Boolean.parseBoolean(props.getProperty(CoreWorkload.ENABLE_LOGGING_PROPERTY,Boolean.toString(CoreWorkload.enableLogging) ));

				if (numBGClients >1 && (uniqueSocilites || enablelogging)  ){
					hybridWithCommunication=true;
				}
				if (benchmarkMode.contains("delegate")){
					System.out.println("Running BG in Hybrid (Delegate) mode");
					BENCHMARKING_MODE=HYBRID_DELEGATE;
				}
				else
				{
					BENCHMARKING_MODE=HYBRID_RETAIN;
					System.out.println("Running BG in Hybrid (Retain) mode");
				}
			}
			else{
				BENCHMARKING_MODE=PARTITIONED;
				System.out.println("Running BG in Partitioned mode");
			}


			if(numBGClients==1 || BENCHMARKING_MODE==PARTITIONED || (hybrid && !hybridWithCommunication) ){
				numMembers=Integer.parseInt(props.getProperty(Client.USER_COUNT_PROPERTY,USER_COUNT_PROPERTY_DEFAULT));
				BGServer.NumWorkerThreads=0;

			}
			if (BENCHMARKING_MODE== RETAIN || BENCHMARKING_MODE== DELEGATE || (hybrid&&hybridWithCommunication)  )
			{
				// Start YAZ Server Code


				if (numBGClients>1)  
				{
					if (props.getProperty(Client.CLIENTS_INFO_PROPERTY)==null ||(props.getProperty(Client.NUM_MEMBERS_PROPERTY)==null && !hybrid)  )
					{
						System.out.println("Argument is missing. Number of members and Client Info are required");
						System.exit(0);
					}
					if (props.getProperty(NUM_MEMBERS_PROPERTY)!=null){
						numMembers=Integer.parseInt(props.getProperty(NUM_MEMBERS_PROPERTY));
					}
					int numSockets=threadCount*2;
					if (props.getProperty(NUM_SOCKETS_PROPERTY)!=null)
						numSockets=Integer.parseInt(props.getProperty(NUM_SOCKETS_PROPERTY));

					ConcurrentHashMap<Integer, ClientInfo> ClientInfoMap=PopulateClientInfo(props.getProperty(Client.CLIENTS_INFO_PROPERTY),Client.numBGClients);

					//		
					bb = new BGServer(machineid,numBGClients,numMembers,0,ClientInfoMap, 0,numSockets);


				}
				//			else
				//			{ // one BGClient
				//				
				//			}
				// End Yaz Server code
				if (BENCHMARKING_MODE== RETAIN || BENCHMARKING_MODE== DELEGATE || (hybrid&&hybridWithCommunication)  )
				{
					if (numBGClients>1 ){
						// pass props and outps to handler which are needed for validation and determining the db client to shutdown the cache
						RequestHandler.setValidationInfo(props,outpS,threads);
					}
				}
			}


		}
		else {
			threadCount = threadCountForNoTransactions(props, inputArguments, doSchema, doTestDB, doStats, doReset, threadCount);
		}

		// target and db
		int monitoringTime = Integer.parseInt(props.getProperty(MONITOR_DURATION_PROPERTY, MONITOR_DURATION_PROPERTY_DEFAULT));
		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, MAX_EXECUTION_TIME_DEFAULT));
		System.out.println("*****max execution time specified : " + maxExecutionTime);
		dbname = props.getProperty(DB_CLIENT_PROPERTY, DB_CLIENT_PROPERTY_DEFAULT);
		target = Integer.parseInt(props.getProperty(TARGET__PROPERTY, TARGET_PROPERTY_DEFAULT));

		// compute the target throughput
		double targetperthreadperms = -1;
		if (target > 0) {
			double targetperthread = ((double) target) / ((double) threadCount);
			targetperthreadperms = targetperthread / 1000.0;
		}
		System.out.println("BG Client: ThreadCount =" + threadCount);

		// show a warning message that creating the workload is taking a while
		// but only do so if it is taking longer than 2 seconds
		// (showing the message right away if the setup wasn't taking very long
		// was confusing people).	

		Thread warningthread = new Thread() {
			public void run() {
				try {
					sleep(2000);
				} catch (InterruptedException e) {
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				System.out.println(" (might take a few minutes for large data sets)");
			}
		};

		warningthread.start();

		System.out.println();
		System.out.println("Loading workload...");

		// load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload userWorkload = null;
		Workload friendshipWorkload = null;
		Workload resourceWorkload = null;

		try {
			if (inputArguments[dotransactions]) {
				// check to see if the sum of all activity and action
				// proportions are 1
				if(!isTotalProbabilityOne(props)){
					return;	
				}

				initializeDB(dbname, props, classLoader);
				if (inputArguments[dotransactions]&& numBGClients > 1 && bb != null) {
					BGServer.initializeWorkers(dbname, props);
				}

				if (Client.workload!=null)
				{ // release workers, so they can start servicing requests 
					releaseWorkers.release(BGServer.NumWorkerThreads);
					//					for (int i=0;i<BGServer.NumSemaphores;i++)
					//					{
					//						BGServer.semaphoreMonitorThreads[i].start();
					//					}
				}
				else
				{
					System.out.println("Error Client workload is null can't start workers");
					System.exit(0);
				}

				MyMeasurement.resetMeasurement();
				System.out.println("\nAfter init: " + new Date());
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
			System.out.println(EXECUTIONDONEMSG);
			return;
		}
		
		warningthread.interrupt();

		int numWarmpup = 0;
		if ((numWarmpup = Integer.parseInt(props.getProperty(WARMUP_OP_PROPERTY, WARMUP_OP_PROPERTY_DEFAULT))) != 0
				&& inputArguments[dotransactions]) {
			warmupPhase(dbname, props, inputArguments[dotransactions], threadCount, targetperthreadperms, numWarmpup);
			// we do not want the measurement for warmup to be counted in the
			// overall measurements
			MyMeasurement.resetMeasurement();
			System.out.println("\nAfter warmup: " + new Date());
		}// end warmup

		System.out.println("Connected");
		if (CoreWorkload.commandLineMode==false || (CoreWorkload.commandLineMode==true && CoreWorkload.enableLogging==true))
		{ // if enableLogging and commandline mode must create threads to generate log records
			// keep a thread of the worker client threads

			if (inputArguments[dotransactions]) {
				String line = "";
				// wait till you get the start message from the coordinator
				if (Boolean.parseBoolean(props.getProperty(RATING_MODE_PROPERTY, RATING_MODE_PROPERTY_DEFAULT))) 
				{
					if(!housekeepingBeforeRunningBG(threadCount, outpS, inSS)) {
						return;
					}
				} 
				else if (threadCount == 0) {
					System.out.println("Invalid thread count: 0, system exiting");
					System.out.println(EXECUTIONDONEMSG);
					return;
				}

				if(!performTransactions(dbname, props, inputArguments[dotransactions], inputArguments[status], threadCount, outpS, inSS, monitoringTime, maxExecutionTime, targetperthreadperms, threads)) {
					return;
				}
			} 
			else {
				DB db = null;
				if (inputArguments[doStats]) {
					executeDoStats(dbname, props);
				} else if (inputArguments[doTestDB]) {
					executeDoTestDB(dbname, props);
				}else if (inputArguments[doReset]){
					executeDoReset(props, classLoader);
				}else if (inputArguments[doSchema]) {
					executeDoSchema(dbname, props, inputArguments, doDropUpdates);
				} else {
					if(!executeDoLoad(dbname, props, inputArguments[dotransactions], inputArguments[doIndex], inputArguments[doLoadFriends], targetperthreadperms, classLoader)){
						return;
					}
				}
			}
			//System.exit(0);


			System.out.println(EXECUTIONDONEMSG);
			System.out.println(Client.SHUTDOWNMSG);
			if (dbname.contains("JdbcDBClient_KOSAR")&&(numBGClients == 1 || BENCHMARKING_MODE == PARTITIONED || BENCHMARKING_MODE==HYBRID_DELEGATE || BENCHMARKING_MODE==HYBRID_RETAIN)) {

				KosarSoloDriver.closeSockets();
			}

			if ((dbname.toLowerCase().contains("kosar"))
					&& CoreClient.enableCache
					&& (numBGClients == 1 || BENCHMARKING_MODE == PARTITIONED) 
					|| BENCHMARKING_MODE == HYBRID_DELEGATE 
					|| BENCHMARKING_MODE == HYBRID_RETAIN) {
				try {
					AsyncSocketServer.shutdown();
				} catch (Exception e) {
					e.printStackTrace(System.out);
				}
			}

		}
	}

	/**
	 * @param dbname
	 * @param props
	 * @param inputArguments
	 * @param dotransactions
	 * @param doIndex
	 * @param doLoadFriends
	 * @param targetperthreadperms
	 * @param classLoader
	 * @return false if exceptions => forced returns else true
	 */
	private boolean executeDoLoad(String dbname, Properties props, boolean dotransactions,boolean doIndex, boolean doLoadFriends, double targetperthreadperms, ClassLoader classLoader) {
		Workload userWorkload;
		Workload friendshipWorkload;
		Workload resourceWorkload;
		DB db;
		long loadStart, loadEnd;
		loadStart = System.currentTimeMillis();

		try {
			/*
			 * //if you want create schema and load to happen together
			 * uncomment this // create schema for RDBMS db =
			 * DBFactory.newDB(dbname, props); db.init();
			 * db.createSchema(props); db.cleanup(true);
			 */

			int useropcount = 0;
			int useroffset = 0;
			Vector<Integer> allMembers = new Vector<Integer>();
			// creating the memberids for this BGClient
			long loadst = System.currentTimeMillis();
			if (props.getProperty(
					CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY,
					CoreWorkload.REQUEST_DISTRIBUTION_PROPERTY_DEFAULT)
					.equals("dzipfian")) {
				Fragmentation createFrags = new Fragmentation(
						Integer.parseInt(props.getProperty(
								USER_COUNT_PROPERTY,
								USER_COUNT_PROPERTY_DEFAULT)),
								Integer.parseInt(props.getProperty(
										NUM_BG_PROPERTY,
										NUM_BG_PROPERTY_DEFAULT)),
										Integer.parseInt(props.getProperty(
												MACHINE_ID_PROPERTY,
												MACHINE_ID_PROPERTY_DEFAULT)),
												props.getProperty(PROBS_PROPERTY,
														PROBS_PROPERTY_DEFAULT),
														Double.parseDouble(props
																.getProperty(
																		CoreWorkload.ZIPF_MEAN_PROPERTY,
																		CoreWorkload.ZIPF_MEAN_PROPERTY_DEFAULT)),true);
				int[] myMembers = createFrags.getMyMembers();
				useropcount = myMembers.length;
				useroffset = Integer.parseInt(props.getProperty(
						USER_OFFSET_PROPERTY,
						USER_OFFSET_PROPERTY_DEFAULT));
				for (int j = 0; j < useropcount; j++)
					allMembers.add(myMembers[j] + useroffset);
			} else {
				useropcount = Integer.parseInt(props.getProperty(
						USER_COUNT_PROPERTY,
						USER_COUNT_PROPERTY_DEFAULT));
				useroffset = Integer.parseInt(props.getProperty(
						USER_OFFSET_PROPERTY,
						USER_OFFSET_PROPERTY_DEFAULT));
				for (int j = 0; j < useropcount; j++)
					allMembers.add(j + useroffset);
			}
			System.out.println("Time to create fragments :"
					+ (System.currentTimeMillis() - loadst) + " msecs");

			System.out.println("Done dividing users");
			loadActiveThread stateThread = new loadActiveThread();
			stateThread.start();

			int friendshipopcount = Integer.parseInt(props.getProperty(
					FRIENDSHIP_COUNT_PROPERTY,
					FRIENDSHIP_COUNT_PROPERTY_DEFAULT));
			int resourceopcount = Integer.parseInt(props.getProperty(
					RESOURCE_COUNT_PROPERTY,
					RESOURCE_COUNT_PROPERTY_DEFAULT));

			// verify thread count
			int numLoadThreads = Integer.parseInt(props.getProperty(
					THREAD_CNT_PROPERTY, THREAD_CNT_PROPERTY_DEFAULT));
			Vector<ClientThread> loadThreads = new Vector<ClientThread>();

			// verify fragment size related to friendships
			if (useropcount < friendshipopcount + 1) {
				System.out
				.println("Fragment size is too small, can't create appropriate friendships. exiting.");
				//System.exit(0);
				System.out.println(EXECUTIONDONEMSG);
				return false;
			}
			// verify load thread count and fragment size
			if (friendshipopcount >= useropcount / numLoadThreads) {
				System.out.println("Can't load with " + numLoadThreads
						+ " so loading with 1 thread");
				numLoadThreads = 1;
			}
			workload = null; //every time load happens the workload left from prev executions should reset

			int numUserThreadOps = 0;
			numUserThreadOps = useropcount / numLoadThreads;
			int remainingUsers = useropcount
					- (numLoadThreads * numUserThreadOps);
			int addUserCnt = 0;
			// TODO: if wrong data is there and we want to redo only
			// friendship on this,
			// probably we will see some sql exceptions
			DB dbglob = DBFactory.newDB(dbname, props);
			dbglob.init();
			if (!doLoadFriends || !dbglob.dataAvailable()) {
				for (int j = 0; j < numLoadThreads; j++) {
					Properties tprop = new Properties();
					db = DBFactory.newDB(dbname, props);
					tprop = (Properties) props.clone();
					if (j == numLoadThreads - 1)
						addUserCnt = remainingUsers;
					tprop.setProperty(
							USER_COUNT_PROPERTY,
							(Integer.toString(numUserThreadOps
									+ addUserCnt)));
					Class<?> userWorkloadclass = classLoader
							.loadClass(tprop
									.getProperty(USER_WORKLOAD_PROPERTY));
					userWorkload = (Workload) userWorkloadclass
							.newInstance();
					Vector<Integer> threadMembers = new Vector<Integer>();
					for (int u = j * numUserThreadOps; u < numUserThreadOps
							* j + numUserThreadOps + addUserCnt; u++) {
						threadMembers.add(allMembers.get(u));
					}
					userWorkload.init(tprop, threadMembers);
					Thread t = new ClientThread(db, dotransactions, userWorkload, j, 1, tprop, numUserThreadOps + addUserCnt, targetperthreadperms, true);
					loadThreads.add((ClientThread) t);
					((ClientThread) t).initThread();
					t.start();
				}
				for (int j = 0; j < numLoadThreads; j++)
					loadThreads.get(j).join();
				System.out.println("Done loading users");
			}
			loadThreads = new Vector<ClientThread>();
			addUserCnt = 0;

			if (friendshipopcount != 0) {
				for (int j = 0; j < numLoadThreads; j++) {
					Properties tprop = new Properties();
					db = DBFactory.newDB(dbname, props);
					tprop = (Properties) props.clone();
					if (j == numLoadThreads - 1) {
						addUserCnt = remainingUsers;
					}
					tprop.setProperty(
							USER_COUNT_PROPERTY,
							Integer.toString(numUserThreadOps
									+ addUserCnt));
					Class friendshipWorkloadclass = classLoader
							.loadClass(tprop
									.getProperty(FRIENDSHIP_WORKLOAD_PROPERTY));
					friendshipWorkload = (Workload) friendshipWorkloadclass
							.newInstance();
					Vector<Integer> threadMembers = new Vector<Integer>();
					for (int u = j * numUserThreadOps; u < numUserThreadOps
							* j + numUserThreadOps + addUserCnt; u++) {
						threadMembers.add(allMembers.get(u));
					}
					friendshipWorkload.init(tprop, threadMembers);
					Thread t = new ClientThread(
							db, dotransactions, friendshipWorkload, j, 1, tprop, 
							((numUserThreadOps + addUserCnt) * friendshipopcount),
							targetperthreadperms, true);

					loadThreads.add((ClientThread) t);
					((ClientThread) t).initThread();
					t.start();
				}
				for (int j = 0; j < numLoadThreads; j++)
					loadThreads.get(j).join();
			}
			System.out.println("Done loading friends");

			if (!doLoadFriends || !dbglob.dataAvailable()) {
				loadThreads = new Vector<ClientThread>();
				addUserCnt = 0;
				if (resourceopcount != 0) {
					for (int j = 0; j < numLoadThreads; j++) {
						Properties tprop = new Properties();
						db = DBFactory.newDB(dbname, props);
						tprop = (Properties) props.clone();
						if (j == numLoadThreads - 1)
							addUserCnt = remainingUsers;
						tprop.setProperty(
								USER_COUNT_PROPERTY,
								Integer.toString(numUserThreadOps
										+ addUserCnt));
						Class resourceWorkloadclass = classLoader
								.loadClass(tprop
										.getProperty(RESOURCE_WORKLOAD_PROPERTY));
						resourceWorkload = (Workload) resourceWorkloadclass
								.newInstance();
						Vector<Integer> threadMembers = new Vector<Integer>();
						for (int u = j * numUserThreadOps; u < numUserThreadOps
								* j + numUserThreadOps + addUserCnt; u++) {
							threadMembers.add(allMembers.get(u));
						}
						resourceWorkload.init(tprop, threadMembers);
						Thread t = new ClientThread(db, dotransactions,
								resourceWorkload, j, 1, tprop,
								(numUserThreadOps + addUserCnt)
								* resourceopcount,
								targetperthreadperms, true);
						loadThreads.add((ClientThread) t);
						((ClientThread) t).initThread();
						t.start();
					}
					for (int j = 0; j < numLoadThreads; j++)
						loadThreads.get(j).join();
				}
				System.out.println("Done loading resources");

				System.out.println("Done loading manipulation");
				if (doIndex) {
					db = DBFactory.newDB(dbname, props);
					db.init();
					db.buildIndexes(props);
					db.cleanup(true);
				}
				System.out
				.println("Done creating indexes and closing db connection");
			}
			dbglob.cleanup(false);
			loadEnd = System.currentTimeMillis();

			// printing out load data
			OutputStream out;
			String exportFile = props.getProperty(EXPORT_FILE_PROPERTY);
			String text = "LoadTime (msec)=" + (loadEnd - loadStart)
					+ "\n";
			text += "Load configuration parameters (those missing from the list have default values):\n";
			Enumeration<Object> em = (Enumeration<Object>) props
					.propertyNames();
			while (em.hasMoreElements()) {
				String str = (String) em.nextElement();
				text += ("\n" + str + ": " + props.get(str));
			}

			// sanity check using statistics from the data store
			text += "\n \n Stats queried from the data store: \n";
			db = DBFactory.newDB(dbname, props);
			db.init();
			Class<?> userWorkloadclass = classLoader.loadClass(props
					.getProperty(USER_WORKLOAD_PROPERTY));
			userWorkload = (Workload) userWorkloadclass.newInstance();
			userWorkload.init(props, null);
			text += "\t MemberCount="
					+ userWorkload.getDBInitialStats(db).get(
							USERCOUNT) + "\n";
			text += "\t ResourceCountPerUser="
					+ userWorkload.getDBInitialStats(db).get(
							RESOURCES_PER_USER) + "\n";
			text += "\t FriendCountPerUser="
					+ userWorkload.getDBInitialStats(db).get(
							AVG_FRNDS_PER_USER) + "\n";
			text += "\t PendingCountPerUser="
					+ userWorkload.getDBInitialStats(db).get(
							AVG_PENDING_PER_USER) + "\n";
			db.cleanup(false);
			System.out.println("Done doing load sanity check");

			byte[] b = text.getBytes();
			if (exportFile == null) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}
			out.write(b);
			if (stateThread != null)
				stateThread.setExit();
			stateThread.join();
			System.out.println("load state thread exited");
		} catch (Exception e) {
			e.printStackTrace(System.out);
		}
		System.out.println("Loading datastore completed.");
		return true;
	}

	/**
	 * @param dbname
	 * @param props
	 * @param inputArguments
	 * @param doDropUpdates
	 */
	private void executeDoSchema(String dbname, Properties props,
			boolean[] inputArguments, final int doDropUpdates) {
		DB db;
		// create schema for RDBMS
		try {
			db = DBFactory.newDB(dbname, props);
			db.init();
			if (inputArguments[doDropUpdates] && db.schemaCreated()) {
				System.out.println("Dropping friendships and manipulations...");
				db.reconstructSchema();
				System.out
				.println("Dropping friendhsips and manipulations was successful...");
			} else {
				System.out.println("Creating data store schema...");
				db.createSchema(props);
				System.out.println("Schema creation was successful");
			}
			db.cleanup(false);
		} catch (UnknownDBException e) {
			e.printStackTrace(System.out);
		} catch (DBException e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * @param props
	 * @param classLoader
	 */
	private void executeDoReset(Properties props, ClassLoader classLoader) {
		Class<?> workloadclass;
		try {
			workloadclass = classLoader.loadClass(props
					.getProperty(WORKLOAD_PROPERTY));
			workload.cleanup();
			workload = (Workload) workloadclass.newInstance();
			workload.init(props, null);
			workload.resetDBInternalStructures(props, BGMainClass.exeMode );
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (WorkloadException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param dbname
	 * @param props
	 */
	private void executeDoTestDB(String dbname, Properties props) {
		DB db;
		try {
			System.out
			.println("Creating connection to the data store...");
			db = DBFactory.newDB(dbname, props);
			boolean connState = db.init();
			db.cleanup(true);
			if (connState)
				System.out.println("connection was successful");
			else
				System.out
				.println("There was an error creating connection to the data store server.");
		} catch (UnknownDBException e) {
			e.printStackTrace(System.out);
		} catch (DBException e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * @param dbname
	 * @param props
	 */
	private void executeDoStats(String dbname, Properties props) {
		DB db;
		try {
			System.out
			.println("Querying statistics for the data store...");
			db = DBFactory.newDB(dbname, props);
			db.init();
			HashMap<String, String> initStats = db.getInitialStats();
			Set<String> keys = initStats.keySet();
			Iterator<String> it = keys.iterator();
			String str = "Stats:{";
			while (it.hasNext()) {
				String tmpKey = it.next();
				str += "[" + tmpKey + ", " + initStats.get(tmpKey)
						+ "]";
			}
			str += "}\n";
			System.out.println(str);
			db.cleanup(true);
			System.out.println("Query stats completed");
		} catch (UnknownDBException e) {
			e.printStackTrace(System.out);
		} catch (DBException e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * @param dbname
	 * @param props
	 * @param inputArguments
	 * @param dotransactions
	 * @param status
	 * @param threadCount
	 * @param outpS
	 * @param inSS
	 * @param monitoringTime
	 * @param maxExecutionTime
	 * @param targetperthreadperms
	 * @param threads
	 * @return false if exceptions => forced returns else true
	 */
	private boolean performTransactions(String dbname, Properties props,
			boolean dotransactions,
			boolean status, int threadCount, PrintWriter outpS, Scanner inSS,
			int monitoringTime, long maxExecutionTime,
			double targetperthreadperms, Vector<Thread> threads) {
		// run the workload
		System.out.println("Starting benchmark.");

		int opcount = Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,
				OPERATION_COUNT_PROPERTY_DEFAULT));
		String simType = props.getProperty(
				SIMTYPE_PROPERTY, SIMTYPE_PROPERTY_DEFAULT);

		if(simType.equalsIgnoreCase("open"))
		{
			DB db = null;

			double lambda = Double.parseDouble(props.getProperty(
					LAMBDA_PROPERTY, LAMBDA_PROPERTY_DEFAULT));

			long simWarmupTime = Integer.parseInt(props.getProperty(
					SIM_WARMUP_TIME_PROPERTY, SIM_WARMUP_TIME_PROPERTY_DEFAULT));
			int distriType = Integer.parseInt(props.getProperty(
					DISTRITYPE_PROPERTY, DISTRITYPE_PROPERTY_DEFAULT));

			try {
				db = DBFactory.newDB(dbname, props);
			} catch (UnknownDBException e) {
				System.out.println("Unknown DB " + dbname);
				System.exit(0);
			}

			Distribution distriThread = new Distribution(db, dotransactions, workload, 1, 1, props, opcount, targetperthreadperms, 
					false, lambda, maxExecutionTime, simWarmupTime, distriType);

			//distriThread added
			threads.add(distriThread);

			//start time noted
			//st = System.currentTimeMillis();

			//distri initThread just initializes workload_state; db.init called on each Worker only
			boolean started = false;
			started = distriThread.initThread();
			while (!started) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace(System.out);
				}
			}

		}
		else{

			for (int threadid = 0; threadid < threadCount; threadid++) {
				DB db = null;
				try {
					db = DBFactory.newDB(dbname, props);
				} catch (UnknownDBException e) {
					System.out.println("Unknown DB " + dbname);
					//System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return false;
				}

				Thread t = new ClientThread(db, dotransactions, workload,
						threadid, threadCount, props, opcount / threadCount,
						targetperthreadperms, false);

				threads.add(t);
			}

			//st = System.currentTimeMillis();

			// initialize all threads before they start issuing requests - ramp
			// up
			for (Thread t : threads) {
				boolean started = false;
				started = ((ClientThread) t).initThread();
				while (!started) {
					try {
						Thread.sleep(1);
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
			}
		}

		StatusThread statusthread = null;


		if (status) {
			statusthread = new StatusThread(threads, workload);
		}
		// start all threads
		for (Thread t : threads) {
			t.start();
		}
		if (status) {

			statusthread.start();
		}

		// visual
		VisualizationThread visual= new VisualizationThread(Client.visualizerPort+Client.machineid,statusthread);
		visual.start();
		//if(simType.equalsIgnoreCase("closed")){
		Thread terminator = null;

		if (maxExecutionTime > 0) {
			terminator = new TerminatorThread(maxExecutionTime, threads,
					workload);
			terminator.start();
		}
		//}

		Thread killThread = null;
		Thread monitorThread = null;
		if (monitoringTime > 0) {
			System.out
			.println("creating the kill thread which waits for kill msg and once received one it kills the BG client");
			killThread = new KillThread(inSS, threads, workload);
			//	killThread.start();
			System.out
			.println("creating monitoring thread to monitor the performance of the BGClient");
			monitorThread = new MonitoringThread(monitoringTime, threads,
					props, outpS, workload);
			monitorThread.start();
		}

		int opsDone = 0; // keeps a track of total number of operations till
		// all threads complete
		int actsDone = 0;
		// needed to stop capturing throughput once the first thread is dead
		int allOpsDone = 0; // keeps a track of total number of actions till
		// the first thread completes
		int allActsDone = 0;
		boolean anyDied = false;
		long firstEn = 0;

		if(simType.equalsIgnoreCase("closed")){
			//Yaz
			System.out.println("Waiting for threads to finish...");
			while (!workload.isStopRequested())
			{
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			System.out.println("Waiting for threads to join...");
			for (Thread t : threads) {
				try {
					t.join();
					System.out.println("BG Thread "+((ClientThread)t)._threadid+" join");
					opsDone += ((ClientThread) t).getOpsDone();
					actsDone += ((ClientThread) t).getActsDone();
					// now that one thread has died for fair throughput
					// computation we stop counting the rest of the completed
					// operations
					if (!anyDied) {
						// this is the first thread that is completing work
						firstEn = System.currentTimeMillis();
						for (Thread t1 : threads) {
							allOpsDone += ((ClientThread) t1).getOpsDone();
							allActsDone += ((ClientThread) t1).getActsDone();
						}
						anyDied = true;
					}

				} catch (InterruptedException e) {
				}
			}
		}else{
			for (Thread t : threads) {
				try {
					t.join();
					//System.out.println(((Distribution)t)._threadid+" died");
					opsDone += ((Distribution) t).getOpsDone();
					actsDone += ((Distribution) t).getActsDone();
					//only one Distribution thread
					if(!anyDied){
						//this is the first and last Distribution thread that is completing work
						firstEn = System.currentTimeMillis();
						for(Thread t1: threads){
							allOpsDone +=((Distribution) t1).getOpsDone();
							allActsDone += ((Distribution) t1).getActsDone();
						}
						anyDied = true;
					}

				} catch (InterruptedException e) {
				}
			}
			//Need to set the following two for proper calculation of Staleness in Open Simulation
			props.setProperty(THREAD_CNT_PROPERTY, new Integer(Worker.maxWorker).toString());
			threadCount=Worker.maxWorker;
		}

		long en = System.currentTimeMillis();
		// if (terminator != null && !terminator.isInterrupted()) {
		// try {
		// terminator.join(2000);
		// } catch (InterruptedException e) {
		// e.printStackTrace();
		// }
		// if(terminator.isAlive())
		// terminator.interrupt();
		// }
		// System.out.println("TerminatorThread died");

		if (killThread != null && !killThread.isInterrupted()) {
			killThread.interrupt();
		}
		System.out.println("killerThread died");

		if (monitorThread != null && !monitorThread.isInterrupted()) {
			monitorThread.interrupt();
		}
		System.out.println("monitoringThread died");

		if (statusthread != null) {
			statusthread.interrupt();
		}

		System.out.println("statusThread died");

		System.out.println("\nAfter workload done: " + new Date());


		try {
			workload.cleanup();
		} catch (WorkloadException e) {
			e.printStackTrace(System.out);
			//System.exit(0);
			System.out.println(EXECUTIONDONEMSG);
			return false;
		}
		if ((BENCHMARKING_MODE==RETAIN||BENCHMARKING_MODE==DELEGATE)&& CoreWorkload.enableLogging && numBGClients>1)
		{
			System.out.println("Computing stats about local and remote actions...");
			printLocalRemoteActionsStats();


		}


		ClientDataStats expStat = new ClientDataStats();
		// if no updates or no reads have taken place , the validation phase
		// does not happen
		// if updates happen graph actions are also executed the validation does not happen

		// pass props and outps to handler which are needed for validation and determining the db client to shutdown the cache
		//RequestHandler.setValidationInfo(props,outpS);
		if (CoreWorkload.enableLogging==false)
		{
			System.out.println("Logging is disabled. Validation is not invoked");

		}
		else
		{
			if ( (CoreWorkload.updatesExist && CoreWorkload.readsExist) || ( !CoreWorkload.updatesExist && CoreWorkload.graphActionsExist )  ) {
				// threadid,listOfSeqs seen by that threadid
				if(numBGClients==1 || BENCHMARKING_MODE==PARTITIONED){
					HashMap<Integer, Integer>[] seqTracker = new HashMap[threadCount+BGServer.NumWorkerThreads];
					HashMap<Integer, Integer>[] staleSeqTracker = new HashMap[threadCount+BGServer.NumWorkerThreads];
					long dumpVTimeE = 0, dumpVTimeS = 0;

					System.out
					.println("--Discarding, dumping and validation starting.");
					dumpVTimeS = System.currentTimeMillis();

					ValidationMainClass.dumpFilesAndValidate(props, seqTracker,
							staleSeqTracker, expStat, outpS, props.getProperty(
									LOG_DIR_PROPERTY, LOG_DIR_PROPERTY_DEFAULT));
					dumpVTimeE = System.currentTimeMillis();
					System.out
					.println("******* Discrading, dumping and validation is done."
							+ (dumpVTimeE - dumpVTimeS));
					expStat.setDumpAndValidateTime((double) (dumpVTimeE - dumpVTimeS)); // the dumptime
				}
				else
				{
					// do validation after workers are done


				}
			}else if(!CoreWorkload.graphActionsExist && !CoreWorkload.updatesExist){
				System.out.println("Warning: Update and Graph actions not exist in the workload so the validation is not invoked.");
			}
		}
		System.out.println("DONE");
		expStat.setOpsTillFirstDeath((double) allOpsDone);
		expStat.setActsTillFirstDeath((double) allActsDone);
		expStat.setTimeTillFirstDeath( (double) (firstEn - experimentStartTime)); // time
		// it
		// took
		// till
		// first
		// thread
		// dies
		try {
			printFinalStats(props, opsDone, actsDone, en - experimentStartTime, expStat,
					outpS);
		} catch (IOException e) {
			System.out.println("Could not export measurements, error: "
					+ e.getMessage());
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		System.out.println("Executing benchmark is completed.");
		return true;
	}

	private void printLocalRemoteActionsStats() {
		// TODO Auto-generated method stub
		long numLocalActs=0;
		long numPartialActs=0;
		long numPartialOrLocalActs=0;
		Vector <ActionStatsThread> actionStatusThreads= new Vector<ActionStatsThread>();
		for (int i=0;i<threadCount;i++)
		{

			ActionStatsThread a=new ActionStatsThread(i, logDir, machineid);
			a.start();
			actionStatusThreads.add(a);
		}
		for(ActionStatsThread t:actionStatusThreads)
		{
			try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			numLocalActs+=t.numLocalActs;
			numPartialActs+=t.numPartialActs;
			numPartialOrLocalActs+=t.numPartialOrLocalActs;

		}


		System.out.println("Number of local actions:"+numLocalActs);
		System.out.println("Number of Partially local actions:"+numPartialActs);
		System.out.println("Number of partial or local actions:"+numPartialOrLocalActs);



	}

	/**
	 * @param threadCount
	 * @param outpS
	 * @param inSS
	 * @return false if threadCount = 0 else true
	 */
	private boolean housekeepingBeforeRunningBG(int threadCount,
			PrintWriter outpS, Scanner inSS) {
		String line;
		if (inSS != null) {
			while (inSS.hasNext()) {
				line = inSS.next();
				if (line.equals("StartSimulation")) {
					break;
				}
			}
			System.out
			.println("BGCLient: Received start simulation msg and will run benchmark");
		}

		if (threadCount == 0 && outpS != null) {
			// need to exit
			System.out.println("DONE");
			outpS.print(" OVERALLRUNTIME(ms):" + 0);
			outpS.print(" OVERALLOPCOUNT(SESSIONS):" + 0);
			outpS.print(" OVERALLTHROUGHPUT(SESSIONS/SECS):" + 0);
			outpS.print(" OVERALLOPCOUNT(ACTIONS):" + 0);
			outpS.print(" OVERALLTHROUGHPUT(ACTIONS/SECS):" + 0);
			outpS.print(" RAMPEDRUNTIME(ms):" + 0);
			outpS.print(" RAMPEDOPCOUNT(SESSIONS):" + 0);
			outpS.print(" RAMPEDTHROUGHPUT(SESSIONS/SECS):" + 0);
			outpS.print(" RAMPEDOPCOUNT(ACTIONS):" + 0);
			outpS.print(" RAMPEDTHROUGHPUT(ACTIONS/SECS):" + 0);
			outpS.print(" STALENESS(OPS):" + 0);
			outpS.print(" SATISFYINGOPS(%):" + 100);
			outpS.print(BENCHMARKENDMSG);
			outpS.flush();
			System.out.println(EXECUTIONDONEMSG);
			System.out.println(Client.SHUTDOWNMSG);
			return false;
		}
		return true;
	}

	/**
	 * @param dbname
	 * @param props
	 * @param inputArguments
	 * @param dotransactions
	 * @param threadcount
	 * @param targetperthreadperms
	 * @param numWarmpup
	 * @return false if UnknownDBEcception else true
	 */
	private boolean warmupPhase(String dbname, Properties props,
			boolean dotransactions,
			int threadcount, double targetperthreadperms, int numWarmpup) {
		// do the warmup phase
		boolean customWarmup=true;
		Vector<Thread> warmupThreads = new Vector<Thread>();
		int numWarmpThread = Integer.parseInt(props.getProperty(
				WARMUP_THREADS_PROPERTY, WARMUP_THREADS_PROPERTY_DEFAULT));
		System.out.println("Starting warmup with " + numWarmpThread
				+ " threads and " + numWarmpup + " operations.");
		long wst = System.currentTimeMillis();
		if (dotransactions) {
			for (int threadid = 0; threadid < numWarmpThread; threadid++) {
				DB db = null;
				try {
					db = DBFactory.newDB(dbname, props);
				} catch (UnknownDBException e) {
					System.out.println("Unknown DB " + dbname);
					System.out.println(EXECUTIONDONEMSG);
					return false;
				}

				Thread t = new ClientThread(db, dotransactions, workload, threadid, threadcount, props, numWarmpup/numWarmpThread, targetperthreadperms, true);
				warmupThreads.add(t);
			}
			// initialize all threads before they start issuing requests -
			// ramp up
			Vector<Thread> initThreads= new Vector<Thread>();


			int i=0;
			for (Thread t : warmupThreads) {
				Thread thread= new Thread(new InitThread(t));
				thread.start();
				if (i%CLIENT_THREADS_INIT_BATCH==0){
					for ( Thread it: initThreads){
						try {
							it.join();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace(System.out);
						}
					}
				}
				initThreads.add(thread);
				i++;
			}

			for ( Thread it: initThreads){
				try {
					it.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace(System.out);
				}
			}




			if (!customWarmup){

				// start all threads
				for (Thread t : warmupThreads) {
					t.start();
				}

				int warmupOpsDone = 0;
				int warmupActsDone = 0;
				// wait for all warmup threads to end
				for (Thread t : warmupThreads) {
					try {
						t.join();
						warmupOpsDone += ((ClientThread) t).getOpsDone();
						warmupActsDone += ((ClientThread) t).getActsDone();
					} catch (InterruptedException e) {
						e.printStackTrace(System.out);
					}
				}
				System.out.println("num warmup Ops done (sessions=: "
						+ warmupOpsDone + ", actions=" + warmupActsDone + ")");
			}
			else{
				System.out.println("Doing Custom Warmup...");
				ArrayList<CustomWarmupThread> customThreads= new ArrayList<CustomWarmupThread>();

				int start, end;
				if (numWarmpup > numMembers){
					numWarmpup = numMembers;
				}
				int portion=numWarmpup/numWarmpThread;
				for (int t=0; t<numWarmpThread;t++){
					start= ((t+1) *portion)-1;
					end= t*portion;
					ClientThread wthread = (ClientThread)warmupThreads.get(t);
					CustomWarmupThread customW = new CustomWarmupThread(wthread._db, start,end);
					customThreads.add(customW);
					System.out.println("Starting Warmup thread "+ t +" start="+start +" end="+end+ " portion="+portion);
					customW.start();

				}
				//				ClientThread wthread = (ClientThread)warmupThreads.get(0);
				//				int nuMembers=200000;
				//				System.out.println("Starting custom warmup");
				//				HashMap<String, ByteIterator> result;//= new HashMap<String, ByteIterator>();
				//				Vector<HashMap<String,ByteIterator>> r;//=new Vector<HashMap<String,ByteIterator>>();
				//				
				//				for (int ii=nuMembers; ii>=0;ii--){
				//					//System.out.println("i="+ii);
				//					int x=CoreWorkload.myMemberObjs[ii].get_uid();
				//				//	System.out.println("id="+x);
				//					result= new HashMap<String, ByteIterator>();
				//					wthread._db.viewProfile(0, x, result, false, false);
				//					r=new Vector<HashMap<String,ByteIterator>>();
				//					wthread._db.listFriends(0, x, null, r, false, false);
				//					r=new Vector<HashMap<String,ByteIterator>>();
				//					wthread._db.viewFriendReq(x, r, false, false);
				//					if (ii%100==0){
				//						System.out.println("Client "+ Client.machineid+" Done with "+ (nuMembers-ii) +" members");
				//					}
				//				}

				for ( i=0;i<customThreads.size();i++){
					try {
						customThreads.get(i).join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace(System.out);
					}
					System.out.println("Warmup thread "+i +"joined");

				}


			}
		}
		long wed = System.currentTimeMillis();
		System.out.println("End warmup. elapsedTime = " + (wed - wst));
		return true;
	}

	/**
	 * @param dbname
	 * @param props
	 * @param classLoader
	 * @throws UnknownDBException
	 * @throws DBException
	 * @throws ClassNotFoundException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws WorkloadException
	 */
	private void initializeDB(String dbname, Properties props,
			ClassLoader classLoader) throws UnknownDBException, DBException,
			ClassNotFoundException, InstantiationException,
			IllegalAccessException, WorkloadException {
		DB db = DBFactory.newDB(dbname, props);
		db.init();
		Class<?> workloadclass = classLoader.loadClass(props
				.getProperty(WORKLOAD_PROPERTY));
		if(workload == null)
			workload = (Workload) workloadclass.newInstance();
		else
			workload.setStopRequested(false);
		// before starting the benchmark get the database statistics :
		// member count, resource per member and avg friend per member
		workload.init(props, null);
		workload.resetDBInternalStructures(props, BGMainClass.exeMode );
		props.setProperty(INIT_USER_COUNT_PROPERTY, workload
				.getDBInitialStats(db).get(USERCOUNT));
		props.setProperty(INIT_RES_COUNT_PROPERTY, workload
				.getDBInitialStats(db).get(RESOURCES_PER_USER));
		props.setProperty(INIT_FRND_COUNT_PROPERTY, workload
				.getDBInitialStats(db).get(AVG_FRNDS_PER_USER));
		props.setProperty(INIT_PEND_COUNT_PROPERTY, workload
				.getDBInitialStats(db).get(AVG_PENDING_PER_USER));
		db.cleanup(true);
	}

	/**
	 * @param props
	 * @return true if the sum of all probabilities = 1 else false
	 */
	private boolean isTotalProbabilityOne(Properties props) {
		double totalProb = 0;
		System.out
		.println(props
				.getProperty(
						CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,
						CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT));
		totalProb = Double
				.parseDouble(props
						.getProperty(
								CoreWorkload.GETOWNPROFILE_PROPORTION_PROPERTY,
								CoreWorkload.GETOWNPROFILE_PROPORTION_PROPERTY_DEFAULT))
								+ Double.parseDouble(props
										.getProperty(
												CoreWorkload.GETFRIENDPROFILE_PROPORTION_PROPERTY,
												CoreWorkload.GETFRIENDPROFILE_PROPORTION_PROPERTY_DEFAULT))
												+ Double.parseDouble(props
														.getProperty(
																CoreWorkload.POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY,
																CoreWorkload.POSTCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT))
																+ Double.parseDouble(props
																		.getProperty(
																				CoreWorkload.DELCOMMENTONRESOURCE_PROPORTION_PROPERTY,
																				CoreWorkload.DELCOMMENTONRESOURCE_PROPORTION_PROPERTY_DEFAULT))
																				+ Double.parseDouble(props
																						.getProperty(
																								CoreWorkload.GENERATEFRIENDSHIP_PROPORTION_PROPERTY,
																								CoreWorkload.GENERATEFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))
																								+ Double.parseDouble(props
																										.getProperty(
																												CoreWorkload.ACCEPTFRIENDSHIP_PROPORTION_PROPERTY,
																												CoreWorkload.ACCEPTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))
																												+ Double.parseDouble(props
																														.getProperty(
																																CoreWorkload.REJECTFRIENDSHIP_PROPORTION_PROPERTY,
																																CoreWorkload.REJECTFRIENDSHIP_PROPORTION_PROPERTY_DEFAULT))
																																+ Double.parseDouble(props
																																		.getProperty(
																																				CoreWorkload.UNFRIEND_PROPORTION_PROPERTY,
																																				CoreWorkload.UNFRIEND_PROPORTION_PROPERTY_DEFAULT))
																																				+ Double.parseDouble(props
																																						.getProperty(
																																								CoreWorkload.GETRANDOMPROFILEACTION_PROPORTION_PROPERTY,
																																								CoreWorkload.GETRANDOMPROFILEACTION_PROPORTION_PROPERTY_DEFAULT))
																																								+ Double.parseDouble(props
																																										.getProperty(
																																												CoreWorkload.GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY,
																																												CoreWorkload.GETRANDOMLISTOFFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
																																												+ Double.parseDouble(props
																																														.getProperty(
																																																CoreWorkload.GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY,
																																																CoreWorkload.GETRANDOMLISTOFPENDINGREQUESTSACTION_PROPORTION_PROPERTY_DEFAULT))
																																																+ Double.parseDouble(props
																																																		.getProperty(
																																																				CoreWorkload.INVITEFRIENDSACTION_PROPORTION_PROPERTY,
																																																				CoreWorkload.INVITEFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
																																																				+ Double.parseDouble(props
																																																						.getProperty(
																																																								CoreWorkload.ACCEPTFRIENDSACTION_PROPORTION_PROPERTY,
																																																								CoreWorkload.ACCEPTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
																																																								+ Double.parseDouble(props
																																																										.getProperty(
																																																												CoreWorkload.REJECTFRIENDSACTION_PROPORTION_PROPERTY,
																																																												CoreWorkload.REJECTFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
																																																												+ Double.parseDouble(props
																																																														.getProperty(
																																																																CoreWorkload.UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY,
																																																																CoreWorkload.UNFRIENDFRIENDSACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																+ Double.parseDouble(props
																																																																		.getProperty(
																																																																				CoreWorkload.GETTOPRESOURCEACTION_PROPORTION_PROPERTY,
																																																																				CoreWorkload.GETTOPRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																				+ Double.parseDouble(props
																																																																						.getProperty(
																																																																								CoreWorkload.GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY,
																																																																								CoreWorkload.GETCOMMENTSONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																								+ Double.parseDouble(props
																																																																										.getProperty(
																																																																												CoreWorkload.POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,
																																																																												CoreWorkload.POSTCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																												+ Double.parseDouble(props
																																																																														.getProperty(
																																																																																CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY,
																																																																																CoreWorkload.DELCOMMENTONRESOURCEACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																																+ Double.parseDouble(props
																																																																																		.getProperty(
																																																																																				CoreWorkload.VIEWNEWSFEEDACTION_PROPORTION_PROPERTY,
																																																																																				CoreWorkload.VIEWNEWSFEEDACTION_PROPORTION_PROPERTY_DEFAULT))
																																																																																				+ Double.parseDouble(props
																																																																																						.getProperty(
																																																																																								CoreWorkload.GETSHORTESTPATHLENGTH_PROPORTION_PROPERTY,
																																																																																								CoreWorkload.GETSHORTESTPATHLENGTH_PROPORTION_PROPERTY_DEFAULT))
																																																																																								+ Double.parseDouble(props
																																																																																										.getProperty(
																																																																																												CoreWorkload.LISTCOMMONFRNDS_PROPORTION_PROPERTY,
																																																																																												CoreWorkload.LISTCOMMONFRNDS_PROPORTION_PROPERTY_DEFAULT))
																																																																																												+ Double.parseDouble(props
																																																																																														.getProperty(
																																																																																																CoreWorkload.LISTFRNDSOFFRNDS_PROPORTION_PROPERTY,
																																																																																																CoreWorkload.LISTFRNDSOFFRNDS_PROPORTION_PROPERTY_DEFAULT));

		if (((totalProb - 1) > 0.1) || ((1 - totalProb) > 0.1)) {
			System.out
			.println("The sum of the probabilities assigned to the actions and activities is not 1. Total Prob = "
					+ totalProb);
			//System.exit(0);
			System.out.println(EXECUTIONDONEMSG);
			return false;
		}
		return true;
	}

	/**
	 * @param props
	 * @param inputArguments
	 * @param doSchema
	 * @param doTestDB
	 * @param doStats
	 * @param doReset
	 * @param threadCount
	 * @return threadCount
	 */
	private int threadCountForNoTransactions(Properties props, boolean[] inputArguments,
			final int doSchema, final int doTestDB, final int doStats,
			final int doReset, int threadCount) {
		if (!inputArguments[doSchema] && !inputArguments[doTestDB] && !inputArguments[doStats] && !inputArguments[doReset]) { // when creating schema
			// the number of threads
			// does not matter
			// so no thread will get 0 users
			if (Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY,
					USER_COUNT_PROPERTY_DEFAULT)) < threadCount) {
				props.setProperty(THREAD_CNT_PROPERTY, "5");
				threadCount = 5;
			}
			if (Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY,
					USER_COUNT_PROPERTY_DEFAULT)) % threadCount != 0) {
				while (Integer.parseInt(props.getProperty(
						USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))
						% threadCount != 0)
					threadCount--;
				props.setProperty(THREAD_CNT_PROPERTY,
						Integer.toString(threadCount));
			}
			// ensure the friendship creation within clusters for each
			// thread makes sense
			if (Integer.parseInt(props.getProperty(
					FRIENDSHIP_COUNT_PROPERTY,
					FRIENDSHIP_COUNT_PROPERTY_DEFAULT)) != 0) {
				int tmp = Integer.parseInt(props.getProperty(
						USER_COUNT_PROPERTY, USER_COUNT_PROPERTY_DEFAULT))
						/ threadCount;
				while (tmp <= Integer.parseInt(props.getProperty(
						FRIENDSHIP_COUNT_PROPERTY,
						FRIENDSHIP_COUNT_PROPERTY_DEFAULT))) {
					threadCount--;
					while (Integer.parseInt(props.getProperty(
							USER_COUNT_PROPERTY,
							USER_COUNT_PROPERTY_DEFAULT))
							% threadCount != 0)
						threadCount--;
					tmp = Integer.parseInt(props.getProperty(
							USER_COUNT_PROPERTY,
							USER_COUNT_PROPERTY_DEFAULT))
							/ threadCount;
				}
				props.setProperty(THREAD_CNT_PROPERTY,
						Integer.toString(threadCount));
			}
		}
		return threadCount;
	}

	/**
	 * @param props
	 * @param threadCount
	 * @return threadCount
	 */
	private int threadCountForTransactions(Properties props, int threadCount) {
		// needed for the activate user array
		if (Integer.parseInt(props.getProperty(USER_COUNT_PROPERTY,
				USER_COUNT_PROPERTY_DEFAULT)) < threadCount) {
			props.setProperty(THREAD_CNT_PROPERTY, "5");
			threadCount = 5;
		}
		// so no thread will get 0 ops
		if (Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,
				OPERATION_COUNT_PROPERTY_DEFAULT)) != 0
				&& Integer.parseInt(props.getProperty(
						OPERATION_COUNT_PROPERTY,
						OPERATION_COUNT_PROPERTY_DEFAULT))
						% threadCount != 0) {
			threadCount--;
			while (Integer.parseInt(props.getProperty(
					OPERATION_COUNT_PROPERTY,
					OPERATION_COUNT_PROPERTY_DEFAULT))
					% threadCount != 0)
				threadCount--;
			props.setProperty(THREAD_CNT_PROPERTY,
					Integer.toString(threadCount));
		}
		return threadCount;
	}

	/**
	 * 
	 * @param listenerConnection
	 * @param props
	 * @return an object containing the PrintWriter and the Scanner references 
	 */
	private Object[] establishSocketConnection(Socket listenerConnection, Properties props) {
		PrintWriter printWriter;
		Scanner scanner;
		ServerSocket BGSocket;
		InputStream inputStream;
		OutputStream outputStream;
		Object printerScanner[] = new Object[2];
		try {
			if (listenerConnection == null) {
				int port = Integer.parseInt(props.getProperty(
						PORT_PROPERTY, PORT_PROPERTY_DEFAULT));
				System.out
				.println("Trying to do rating with the specified thread count , creating socket on "
						+ port);


				BGSocket = new ServerSocket(port, 10);
				System.out.println("Started");
				System.out
				.println("BGClient: started and Waiting for connection on "
						+ port);
				listenerConnection = BGSocket.accept();
				System.out.println("BGClient: Connection received from "
						+ listenerConnection.getInetAddress().getHostName());
				//BGSocket.close();
			} 

			inputStream = listenerConnection.getInputStream();
			outputStream = listenerConnection.getOutputStream();
			printWriter = new PrintWriter(outputStream);
			scanner = new Scanner(inputStream);
			// send connected message to the rater thread
			System.out.println("Initiated"); 
			printWriter.print("Initiated ");
			printWriter.flush();
			System.out.println("BGClient: SENT initiation message to "
					+ listenerConnection.getInetAddress().getHostName());
			printerScanner[0] = printWriter;
			printerScanner[1] = scanner;

		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		return printerScanner;
	}

	/**
	 * @param args
	 * @param props
	 * @param inputArguments Boolean array of the initial values of the input args
	 * @param fileprops
	 */

	public static ConcurrentHashMap<Integer, ClientInfo> PopulateClientInfo(String s,int numBGClients)
	{


		ConcurrentHashMap<Integer, ClientInfo> ClientInfoMap= new ConcurrentHashMap<Integer, ClientInfo>();

		//clients=IP1:Port1,IP2:Port2,IP3:Port3
		try {
			int count=0;
			for (int n=0;n<s.length();n++)
			{
				if (s.charAt(n)==':')
					count++;
			}
			if (count!=numBGClients)
			{
				System.out.println("Error Number of Clients not match client pairs");
				System.exit(1);
			}
			int j=s.indexOf('=');
			String s2= s.substring(j+1);

			String IP;
			int i2,i3, port;
			int i=0;
			for (i=0; i< numBGClients-1;i++)
			{
				i2=s2.indexOf(':');
				i3= s2.indexOf(',');

				IP= s2.substring(0, i2);
				//System.out.println("IP= " + IP);
				port= Integer.parseInt(s2.substring(i2+1, i3));
				//System.out.println("port= " + port);
				s2=s2.substring(i3+1, s2.length());

				ClientInfo a = new ClientInfo(IP,port,i);
				ClientInfoMap.put(i, a);

			}
			i2=s2.indexOf(':');
			IP= s2.substring(0,i2);
			port= Integer.parseInt(s2.substring(i2+1,s2.length()));
			ClientInfo a = new ClientInfo(IP,port,i);

			ClientInfoMap.put(i, a);


			// check Errors
			for ( i=0; i< ClientInfoMap.size();i++)
				for (j=i+1; j<ClientInfoMap.size();j++)
				{
					if (ClientInfoMap.get(i).getIP().equals(ClientInfoMap.get(j).getIP())&& ClientInfoMap.get(i).getPort()== ClientInfoMap.get(j).getPort())
					{
						System.out.println("IP:Port Duplicates");
						System.exit(1);
					}
				}


			//The number of comma seperated IP:Port in the cleints must equal numclients.  Otherwise, report error and exit!

			if (ClientInfoMap.size()!= numBGClients )
			{
				System.out.println("Error: Number of Clients not Equal Number of (IP,Port) pairs");
				System.exit(1);
			}


		}
		catch(Exception ex)
		{

			System.out.println("Error: Client IP, Port are not Passed correctly");
			System.exit(1);
		}
		return ClientInfoMap;

	}
	public static void readCmdArgs(String[] args, Properties props, boolean[] inputArguments, Properties fileprops) {
		int argIndex = 0;
		final int dotransactions = 0;
		final int doSchema = 1;
		final int doTestDB = 2;
		final int doStats = 3;
		final int doIndex = 4;
		final int doLoadFriends = 5;
		final int doDropUpdates = 6;
		final int doReset = 7;
		final int status = 8;
		while (args[argIndex].startsWith("-")) {
			if (args[argIndex].equals("-threads")) {
				argIndex++;
				if (argIndex >= args.length) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				int tcount = Integer.parseInt(args[argIndex]);
				props.setProperty(THREAD_CNT_PROPERTY, tcount + "");
			}
			else if (args[argIndex].equals("-target")) {
				argIndex++;
				if (argIndex >= args.length) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				int ttarget = Integer.parseInt(args[argIndex]);
				props.setProperty("target", ttarget + "");
			} else if (args[argIndex].compareTo("-load") == 0) {
				inputArguments[dotransactions] = false;
			} else if (args[argIndex].compareTo("-loadindex") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doIndex] = true;
			} else if (args[argIndex].compareTo("-loadfriends") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doIndex] = true;
				inputArguments[doLoadFriends] = true;
			} else if (args[argIndex].compareTo("-t") == 0) {
				inputArguments[dotransactions] = true;
			} else if (args[argIndex].compareTo("-schema") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doSchema] = true;
			} else if (args[argIndex].compareTo("-dropupdates") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doSchema] = true;
				inputArguments[doDropUpdates] = true;
			} else if (args[argIndex].compareTo("-testdb") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doTestDB] = true;
			} else if (args[argIndex].compareTo("-stats") == 0) {
				inputArguments[dotransactions] = false;
				inputArguments[doStats] = true;
			}else if(args[argIndex].compareTo("-reset") == 0){
				inputArguments[dotransactions] = false;
				inputArguments[doReset] = true;
			} else if (args[argIndex].compareTo("-s") == 0) {
				inputArguments[status] = true;
			} else if (args[argIndex].compareTo("-db") == 0) {
				argIndex++;
				if (argIndex >= args.length) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				props.setProperty("db", args[argIndex]);
			} 
			else if (args[argIndex].equals("-P")) {
				argIndex++;
				String filename=args[argIndex].toLowerCase();
				if (filename.contains("action") || filename.contains("symmetric"))
				{
					char delimeter='\\';
					if (filename.contains("/"))
						delimeter='/';
					String workload=filename.substring(filename.lastIndexOf(delimeter)+1);
					//					String tokens[]=filename.split(delimeter);
					props.put("workloadfile", workload);
				}
				if (argIndex >= args.length) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				String propfile = args[argIndex];

				Properties myfileprops = new Properties();
				try {
					myfileprops.load(new FileInputStream(propfile));
				} catch (IOException e) {
					System.out.println(e.getMessage());
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}

				for (Enumeration e = myfileprops.propertyNames(); e.hasMoreElements();) {
					String prop = (String) e.nextElement();
					fileprops.setProperty(prop, myfileprops.getProperty(prop));
				}
			}
			//			else if (args[argIndex].compareTo("-c") == 0)
			//			{
			//				argIndex++;
			//				if (argIndex >= args.length) {
			//					usageMessage();
			//					// System.exit(0);
			//					System.out.println(EXECUTIONDONEMSG);
			//					return;
			//				}
			//				PopulateClientInfo(args[argIndex]);
			//				
			//				
			//				
			//			}
			else if (args[argIndex].compareTo("-p") == 0) {
				argIndex++;
				if (argIndex >= args.length) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				int eq = args[argIndex].indexOf('=');
				if (eq < 0) {
					usageMessage();
					// System.exit(0);
					System.out.println(EXECUTIONDONEMSG);
					return;
				}
				String name = args[argIndex].substring(0, eq);
				String value = args[argIndex].substring(eq + 1);
				props.put(name, value);
			} else {
				System.out.println("Unknown option " + args[argIndex]);
				usageMessage();
				// System.exit(0);
				System.out.println(EXECUTIONDONEMSG);
				return;
			}
			argIndex++;
			if (argIndex >= args.length) {
				break;
			}
		}// done reading command args
	}
}
