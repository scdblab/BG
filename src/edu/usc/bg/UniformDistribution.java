/**
 * Authors:  Aniruddh Munde and Snehal Deshmukh
 */
package edu.usc.bg;



public class UniformDistribution extends Distribution {



	//uniform
	int remainder;
	double interArriveTime;  // milliseconds


	public void runSimulation(){

		currentReqCount = 1;
		int count=0;

		//start the first worker
		Worker workerRunnable = new Worker(null,0, _workload); 
		Thread workerThread = new Thread(workerRunnable);
		workerThread.setName("First Thread");	
		Worker.setParameters();
		Worker.initInitialWorkerThred();
		workerThread.start();

		//run until all the numOfReq are not generated

		while(currentReqCount <= numOfReq && !_workload.isStopRequested())
		{	

			if(granularity < interArriveTime) //submit just 1 request
			{
				//System.out.println("**Arrival of New Request : "+(currentReqCount));

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
				try {
					Thread.sleep((long)interArriveTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 
			}
			else {
				int i=0;
				remainder = (int)(granularity % interArriveTime);
				count = (int) ((remainder+granularity)/interArriveTime);
				while(i< count) //submit count number of requests before sleeping
				{
					//System.out.println("Arrival of New Request : "+(currentReqCount));
					Request newReq = new Request(currentReqCount);

					requestStats.put(newReq.ReqID, new Times(System.nanoTime()/1000000000.0));

					try {
						QS.acquire();		
						queue.add(newReq);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					QS.release();
					currentReqCount++;

					i++;
				}
				//sleep after submitting requests
				try {
					Thread.sleep((long)granularity);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} 

			}
		}
		//wait for all workers
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
		Distribution.warmupNumOfReq=(long) Math.floor(warmupTime/interArriveTime);
	}
}
