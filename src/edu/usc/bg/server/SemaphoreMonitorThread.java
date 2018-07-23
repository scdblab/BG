package edu.usc.bg.server;

import java.util.concurrent.Semaphore;

public class SemaphoreMonitorThread extends Thread{
	int threadId;

	public SemaphoreMonitorThread( int id) {
		// TODO Auto-generated constructor stub
		threadId=id;
		
	}
	public void run()
	{
		while(BGServer.ServerWorking) {
			if (!BGServer.requestsSemaphores.get(threadId).hasQueuedThreads())
			{
				System.out.println("No worker waiting to acquire for request queue number: "+ threadId);
			
			try {
				sleep(100);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		}
	}
	

}
