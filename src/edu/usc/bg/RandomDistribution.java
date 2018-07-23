/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;




public class RandomDistribution extends Distribution {


	public RandomDistribution() {
	}

	//random
	double interArriveTime;  // milliseconds
	SortedSet<Double> interArriveTimeset = new TreeSet<Double>();
	int count[];
	int NUM_OF_COUNTS; // number of granularity window slots in the simulation time



	public void runSimulation(){

		int index=0;
		currentReqCount = 1;
		Worker workerRunnable = new Worker(null,0, _workload); 
		Thread workerThread = new Thread(workerRunnable);
		workerThread.setName("First Thread");
		Worker.setParameters();
		Worker.initInitialWorkerThred();

		workerThread.start();
		while(currentReqCount <= numOfReq && !_workload.isStopRequested())
		{	

			int i=0;
			while(i< count[index]) //count[index] gives the number of requests in this granularity window
			{
				//System.out.println("Arrival of New Request : "+(currentReqCount));
				Request newReq = new Request(currentReqCount);
				requestStats.put(newReq.ReqID, new Times(System.nanoTime()/1000000000.0));

				try {
					QS.acquire();
					queue.add(newReq);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}

				QS.release();
				currentReqCount++;

				i++;
			}
			try {
				Thread.sleep((long)granularity);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} 
			index++;			
		}

		//enable if you want all queued requests to be processed
		//while(!flag.get())
			try {
				Thread.sleep(10);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}


	}



	public void preProcessing(){
		interArriveTime = (1/lambda)*1000;
		numOfReq=(long) Math.floor(simulationTime/interArriveTime);
		double randNo;
		//Get the random time instances
		while( interArriveTimeset.size() < numOfReq){

			randNo = simulationTime *Math.random();
			interArriveTimeset.add(randNo);
		}

		NUM_OF_COUNTS=(int)Math.floor(simulationTime/granularity);
		count=new int[NUM_OF_COUNTS];

		Iterator<Double> it=interArriveTimeset.iterator();
		double value=0.0;
		int temp=0,index=0;
		double timePassed=granularity;
		/*count number of requests to be submitted in each granularity window*/
		while(it.hasNext())
		{
			value=it.next();
			//	System.out.println("Random Time Instance:"+value);

			if(value<=timePassed)
				temp++;
			else {
				count[index++]=temp;
				timePassed+=granularity;
				temp=0;
				//value is greater than granularity
				while(value >= timePassed){
					count[index++]=0;
					timePassed+=granularity;  
				}
				temp=1;
			}
		}
		if(index<NUM_OF_COUNTS)
		{
			count[index++]=temp;
			timePassed+=granularity;
		}
		while(index<NUM_OF_COUNTS)
		{
			count[index++]=0;
		}

	}

	public void warmupProcessing(){

		Iterator<Double> it=interArriveTimeset.iterator();
		double value=0.0;

		while(it.hasNext())
		{
			value=it.next();
			if(value <= warmupTime)
				warmupNumOfReq++;
			else 
				break;
		}
	}
}
