/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;
import edu.usc.bg.base.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;


public class Distribution extends Thread{
	static DB _db; // each worker has its own DB; this member of Distribution is of no use, kept to maintain the members same as ClientThread
	//not useful for open simulation
	static boolean _warmup; //identifies if this thread is a warmup thread,m if it is it does not issue updates; 
	static int _opcount;
	static double _target;
	static int _opsdone=0; //sessions
	static int _actionsDone=0;

	static AtomicInteger opsDone=new AtomicInteger(0);
	static AtomicInteger actionsDone=new AtomicInteger(0);

	public static Workload _workload;
	static Object _workloadstate;

	static AtomicInteger seqID=new AtomicInteger(0);

	static int _threadid;//always 1; in open,workerId defines the number of threads 
	static int _threadcount;//1 for now; after completion of simulation set to the maxWorkers created
	static Properties _props;

	static HashMap<String, Integer> resUpdateOperations; // keep a track of the updates
	// done by this thread on
	// different resources
	public static HashMap<String, Integer> friendshipInfo; // keep a track of all friendids
	// for a user
	public static HashMap<String, Integer> pendingInfo; // keep a track of all friendids that
	// have generated pending request for
	// this user

	static boolean insertImages = false;
	static boolean _dotransactions;//always true in case of open


	BufferedWriter OpenSimulationStats;
	static double lambda; 			// req/sec
	static long simulationTime;  //milliseconds
	static long warmupTime;  //milliseconds
	static long ActualSimulationTime;//milliseconds
	static long numOfReq;
	static long warmupNumOfReq;
	static int type; //type of distribution

	static final long GRAN_LOOP_ITERATION = 10; // number of iterations for granularity
	static public  double granularity;		//milliseconds
	static int currentReqCount=0;

	public static  ArrayList<Request> queue = new ArrayList<Request>();  
	public static  Semaphore QS = new Semaphore(1);

	public static HashMap<Integer,Times> requestStats = new HashMap<Integer,Times>();

	public static AtomicBoolean flag=new AtomicBoolean(false);
	//	public static Server server1; Use this if need to test using our dummy server

	/**
	 * Constructor.
	 * 
	 * @param db
	 *            The DB implementation to use.
	 * @param dotransactions
	 *            True to do transactions, false to insert data or create schema.
	 * @param workload
	 *            The workload to use.
	 * @param threadid
	 *            The id of this thread.
	 * @param threadcount
	 *            The total number of threads.
	 * @param props
	 *            The properties defining the experiment.
	 * @param opcount
	 *            The number of operations (transactions or inserts) to do.
	 * @param targetperthreadperms
	 *            Target number of operations per thread per ms.
	 * @param warmup Identifies if its the warmup phase so update requests would not be issued.
	 */
	public Distribution(DB db, boolean dotransactions, Workload workload,
			int threadid, int threadcount, Properties props, int opcount,
			double targetperthreadperms, boolean warmup,double lambda,long maxexecutiontime,long warmupTime,int distributionType) {
		_warmup = warmup;
		_db = db;
		_dotransactions = dotransactions;
		_workload = workload;
		_opcount = opcount;
		_opsdone = 0;
		_actionsDone = 0;
		_target = targetperthreadperms;
		_threadid = threadid;
		_threadcount = threadcount;
		_props = props;
		resUpdateOperations = new HashMap<String, Integer>();
		friendshipInfo = new HashMap<String, Integer>();
		pendingInfo = new HashMap<String, Integer>();
		insertImages = Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
				Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		Distribution.lambda = lambda;
		Distribution.ActualSimulationTime = maxexecutiontime*1000;
		type =distributionType;
		Distribution.warmupTime = warmupTime*1000;
		simulationTime = warmupTime*1000 + ActualSimulationTime;
		String dir = Distribution._props.getProperty(Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT);
		String machineid = Distribution._props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT);
		opsDone=new AtomicInteger(0);
		actionsDone=new AtomicInteger(0);
		seqID=new AtomicInteger(0);
		queue = new ArrayList<Request>();  
		QS = new Semaphore(1);
		requestStats = new HashMap<Integer,Times>();
		flag=new AtomicBoolean(false);
		//
		try {
			//update file
			File ufile = new File(dir+"/Stats-"+lambda+ ".txt");
			FileWriter ufstream = new FileWriter(ufile);
			OpenSimulationStats = new BufferedWriter(ufstream);
			//read file
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}


	}
	//do nothing constructor required for inherited classes
	public Distribution()
	{

	}

	public int getOpsDone() {
		return opsDone.get();
	}
	public int getActsDone() {
		return actionsDone.get();
	}

	public boolean initThread() {
		try {
			_workloadstate = _workload.initThread(_props, _threadid,
					_threadcount);
		} catch (WorkloadException e) {
			e.printStackTrace(System.out);
			return false;
		}
		return true;
	}

	public void run () {
		granularity = calc_granularity();
		//server1 = new Server(1,100); //numofServers, serviceTime

		runSimulation();

		try {

			if(OpenSimulationStats != null)
				OpenSimulationStats.close();
		} catch (Exception e) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.out);
			return;
		}

		System.out.println("Number of Worker Threads :"+Worker.maxWorker);
		this.interrupt();		
	}



	protected  void runSimulation(){

		switch(type)
		{
		case 1:
			System.out.println("---------- Uniform Distribution .......");
			UniformDistribution ud=new UniformDistribution();
			ud.preProcessing();
			ud.runSimulation();
			break;
		case 2:
			System.out.println("---------- Random Distribution .......");
			RandomDistribution rd=new RandomDistribution();
			rd.preProcessing();
			rd.warmupProcessing();
			rd.runSimulation();
			break;
		case 3:
			System.out.println("---------- Poisson Distribution .......");
			PoissonDistribution pd=new PoissonDistribution();
			pd.preProcessing();
			pd.warmupProcessing();
			pd.runSimulation();
			break;
		default:
			System.out.println("Type argument() should be one of 1,2,3");
			break;

		}
		//double totalClientQueueingTime=0;
		//Statistics
		double totalResponseTime =0;
		int count =0;
		for(Map.Entry<Integer,Times> e: requestStats.entrySet()){
			if(count >= warmupNumOfReq){
				//totalClientQueueingTime += e.getValue().getClientQueueingTime()-e.getValue().getTimeAtCreation(); can be found if required
				totalResponseTime += e.getValue().getTimeAfterService()-e.getValue().getTimeBeforeService();
			}else
				count++;	
		}

		long actualNumOfReq = numOfReq - warmupNumOfReq;
		double W=totalResponseTime/actualNumOfReq;

		try {

			if(OpenSimulationStats != null)
			{
				OpenSimulationStats.write("Total Response Time: "+ totalResponseTime*1000.0 + " ms\n");
				OpenSimulationStats.write("No. of Requests:  " + actualNumOfReq+"\n");
				OpenSimulationStats.write("Average Response Time: "+  W*1000.0 + " ms\n");
				if(OpenSimulationStats != null)
					OpenSimulationStats.close();
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

	}



	protected  double calc_granularity()
	{

		double granularity = 100;
		double actualGran = 100;
		long miliSleepTime = 100;  // assuming sleeps at least for 100 millisecond
		int nanoSleepTime = 0;

		System.out.println("---------- Number of iterations : " +  GRAN_LOOP_ITERATION);   

		try {
			while(true)
			{	
				double timeBeforeSleep = (System.nanoTime())/1000000.0;

				int i=0;
				while(i < GRAN_LOOP_ITERATION )
				{
					Thread.sleep(miliSleepTime, nanoSleepTime);
					i++;
				}	

				double timeAfterSleep = (System.nanoTime())/1000000.0;
				//actual time slept
				double timeDiff = timeAfterSleep - timeBeforeSleep;
				//expected time slept

				double expectedTimeSlept = 1.0 * GRAN_LOOP_ITERATION * (miliSleepTime*1.0 + nanoSleepTime/1000000.0); //in milliseconds after GRAN_LOOP_ITERATIONs

				System.out.println("Current Sleep time : "+ (miliSleepTime*1.0 + nanoSleepTime/1000000.0)  + " ms");
				System.out.println("Expected time difference : "+ expectedTimeSlept  + " ms");
				System.out.println("Actual time difference : "+ timeDiff + " ms");
				/*If difference between expected and actual time difference is less than 3 ms, go check for smaller granularity
				 * else break with the actual granularity set to the value of granularity just before this break*/
				if(Math.abs(( timeDiff - expectedTimeSlept)) < 3 )
				{
					if(miliSleepTime  == 1 )
					{
						miliSleepTime =0;
						nanoSleepTime = 100000; 
					}else if(miliSleepTime  == 0 )
					{
						nanoSleepTime = nanoSleepTime/10 ; 

					}else 
						miliSleepTime = miliSleepTime/10;  

					granularity = granularity/10;

				}else 
					break;
				//Before going for smaller value of granularity assign actual granularity till this point 
				actualGran = granularity*10;			
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}   

		System.out.println("Granularity : "+ actualGran + " ms");
		return actualGran;		
	}
}