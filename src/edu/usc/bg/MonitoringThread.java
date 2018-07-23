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


package edu.usc.bg;
import java.io.PrintWriter;

import java.util.Properties;
import java.util.Vector;


import edu.usc.bg.base.*;
import edu.usc.bg.measurements.MyMeasurement;

/**
 * Responsible for monitoring the behavior of a BG client and reporting its statistics to the
 * coordinator once every s seconds
 * the s seconds is decided by the coordinator in the rating mode
 * @author barahman
 *
 */
public class MonitoringThread extends Thread{
	long _sleepTime;
	Vector<Thread> _threads;
	Properties _props;
	PrintWriter _out = null;
	Workload _workload;

	public MonitoringThread(int monitor,Vector<Thread> threads, Properties props, PrintWriter out, Workload workload ){
		System.out.println("MONITORING THREAD CREATED");
		_sleepTime = monitor*1000;
		_threads = threads;
		_props = props;
		_out = out;
		_workload = workload;

	}

	public void run(){
		System.out.println("MONITORING THREAD STARTED");
		long st = System.currentTimeMillis();
		//long lasten = st;
		//long lasttotalops = 0;
		boolean alldone;
		do {
			try {
				sleep(_sleepTime);
			} catch (InterruptedException e) {
				// do nothing
			}
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
			long interval = en - st;
			double totalThroughputTillNow = 1000.0 * (((double) (totalops)) / ((double) (interval)));
			double totalActThroughputTillNow = 1000.0 * (((double) (totalacts)) / ((double) (interval)));
			//get latency
			/*Vector<Double> latencies = MyMeasurement.getAllLatencies();
			int numSatisfying = 0;
			double expectedLatency = (Double.parseDouble(_props.getProperty(Client.EXPECTED_LATENCY_PROPERTY, Client.EXPECTED_LATENCY_PROPERTY_DEFAULT)))*1000;
			for(int j=0; j<latencies.size(); j++){
				if(latencies.get(j) <= expectedLatency){
					numSatisfying++;
				}
			}*/
			double satisfyingPerc = MyMeasurement.getSatisfyingPerc();
			
			
			System.out.println("MONITOR-THROUGHPUT(SESSIONS/SEC):"+totalThroughputTillNow);
			System.out.println("MONITOR-THROUGHPUT(ACTIONS/SEC):"+totalActThroughputTillNow);
			//System.out.println("MONITOR-SATISFYINGOPS(%):"+(double)(numSatisfying)/latencies.size()*100);
			System.out.println("MONITOR-SATISFYINGOPS(%):"+satisfyingPerc);
		} while (!alldone && !_workload.isStopRequested());
	}//end run

}//end class
