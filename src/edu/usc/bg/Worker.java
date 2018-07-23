/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;
import edu.usc.bg.base.*;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.util.concurrent.atomic.AtomicInteger;




public class Worker implements Runnable{
	DB _db;


	public static AtomicInteger NumOfWorker=new AtomicInteger(0);
	private Request req;
	static int thinkTime = 0;
	static boolean insertImage = false;
	BufferedWriter updateLog; // update log file
	BufferedWriter readLog; // read log file
	int workerId;
	public static int maxWorker=1;
	private static AtomicInteger processedReq=new AtomicInteger(0);
	private static AtomicInteger NumOfIdleWorkers=new AtomicInteger(0);
	private static Workload _workload;
	
	
	Worker(Request InputReq,int workerId, Workload workload){
		req= InputReq;
		this.workerId=workerId;
		_workload = workload;
		String dbname=Distribution._props.getProperty(Client.DB_CLIENT_PROPERTY,Client.DB_CLIENT_PROPERTY_DEFAULT);
		_db = null;
		try {
			_db = DBFactory.newDB(dbname,Distribution._props);
		} catch (UnknownDBException e) {
			System.out.println("Unknown DB "+dbname);
			System.exit(0);
		}	
		String machineid = Distribution._props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT);
		String dir = Distribution._props.getProperty(Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT);
		//no file is needed if the thread is in warmup or load step
		// create file and open it
		try {
			//update file
			File ufile = new File(dir+"/update"+machineid+"-"+workerId + ".txt");
			FileWriter ufstream = new FileWriter(ufile);
			updateLog = new BufferedWriter(ufstream);
			//read file
			File rfile = new File(dir+"/read"+machineid+"-"+workerId + ".txt");
			FileWriter rfstream = new FileWriter(rfile);
			readLog = new BufferedWriter(rfstream);
		} catch (IOException e) {
			e.printStackTrace(System.out);
		}
		boolean started = false;
		started = this.initThread();
		while (!started) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace(System.out);
			}
		}

	}
	
	public static void initInitialWorkerThred(){
		
		NumOfWorker=new AtomicInteger(0);
		maxWorker=1;
		processedReq=new AtomicInteger(0);
		NumOfIdleWorkers=new AtomicInteger(0);
		System.out.println("********************");
		
	}
	
	
	public boolean initThread() {
		try {
			_db.init();
		} catch (DBException e) {
			e.printStackTrace(System.out);
			return false;
		}


		return true;
	}
	public static void setParameters()
	{
		if (Distribution._props.getProperty(Client.THINK_TIME_PROPERTY) != null) {
			thinkTime = Integer.parseInt(Distribution._props
					.getProperty(Client.THINK_TIME_PROPERTY));
		}
		if (Distribution._props.getProperty(Client.INSERT_IMAGE_PROPERTY) != null) {
			insertImage = Boolean.parseBoolean(Distribution._props.getProperty(
					Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		}
	}

	public void run()
	{




		StringBuilder updateTestLog = new StringBuilder();
		StringBuilder readTestLog = new StringBuilder();
		int actsDone=0;
		NumOfWorker.getAndIncrement();

		try{

			while ( processedReq.get() < Distribution.numOfReq && !_workload.isStopRequested()) //not done
			{

				if ( req != null)
				{	
					updateTestLog.delete(0, updateTestLog.length());
					readTestLog.delete(0, readTestLog.length());
					Distribution.requestStats.get(req.ReqID).setTimeBeforeService(System.nanoTime()/1000000000.0);

					//submit request
					//Distribution.server1.service(workerId,req.ReqID.intValue()); Use this if need to test using our dummy server

					actsDone = 0;
					if ((actsDone = Distribution._workload.doTransaction(_db,Distribution._workloadstate,
							workerId, updateTestLog,readTestLog, Distribution.seqID.get(), Distribution.resUpdateOperations,
							Distribution.friendshipInfo, Distribution.pendingInfo, thinkTime,
							insertImage, Distribution._warmup)) < 0) { //=0 when only perfomring actions like accept friendship and no pending frnd are there
						System.out.println("Couldnt Service request");
					}
					Distribution.requestStats.get(req.ReqID).setTimeAfterService(System.nanoTime()/1000000000.0);

					Distribution.actionsDone.getAndAdd(actsDone);
					Distribution.opsDone.getAndIncrement();

					Distribution.seqID.getAndIncrement();
					try {

						if(updateLog != null)
							updateLog.write(updateTestLog.toString());
						if(readLog != null)
							readLog.write(readTestLog.toString());
					} catch (IOException e) {
						e.printStackTrace();
					}


					processedReq.getAndIncrement();
					NumOfIdleWorkers.getAndIncrement();
					//System.out.println("After processing request : "+ processedReq.get()+" "+_workload.isStopRequested()+" "+workerId);
					req= null;
				}

				//Wait for QS
				try {

					while(/*true &&*/ !_workload.isStopRequested())
					{
						Distribution.QS.acquire();
						//create new worker only if following conditions satisfy else break
						if(!((Distribution.queue.size() >1) && ((Distribution.queue.size()-(NumOfIdleWorkers.get()-1))>0)&& (maxWorker < 500) && !_workload.isStopRequested()))
						{	
							Distribution.QS.release();
							break;
						}	

						Worker workerRunnable = new Worker(Distribution.queue.remove(0),maxWorker, _workload);
						Thread workerThread = new Thread(workerRunnable);
						maxWorker++;
						workerThread.start();
						Distribution.QS.release();
					}

					Distribution.QS.acquire();
					if(!Distribution.queue.isEmpty() && !_workload.isStopRequested()){
						req =Distribution.queue.remove(0); //the remaining element in the queue
						Distribution.requestStats.get(req.ReqID).setClientQueueingTime(System.nanoTime()/1000000000.0);

						if(!(Thread.currentThread().getName().equalsIgnoreCase("First Thread")))
							NumOfIdleWorkers.getAndDecrement();
					}	
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				//Release QS
				Distribution.QS.release();	

			}   
		} catch (Exception e) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.out);
			System.exit(0);
		}
		NumOfWorker.getAndDecrement();
		try {
			cleanup();
			if(updateLog != null)
				updateLog.close();
			if(readLog != null)
				readLog.close();
		} catch (Exception e) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.out);
			return;
		}
		if(NumOfWorker.get()==0)
			Distribution.flag.set(true);

	}
	public void cleanup(){
		try {
			_db.cleanup(Distribution._warmup);
		} catch (DBException e) {
			e.printStackTrace();
		}
	}
}