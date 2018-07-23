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


package edu.usc.bg.measurements;

import java.io.File;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;

/**
 * Collects latency and rerurn code measurements, and reports them when requested.
 * 
 * @author barahman
 *
 */
public class MyMeasurement
{
	
	static Vector<MyMeasurement> allMeasurements=new Vector<MyMeasurement>();
	static String fileNameToken = "latency";
	static double expectedLatency;
		
	/**
	 * creates a measurement tracker for each thread 
	 * which is not synchronized between threads
	 * @return MyMeasurement
	 */
    public static MyMeasurement getMeasurements(double expLatency)
	{
    	expectedLatency = expLatency;
		MyMeasurement mm = new MyMeasurement(allMeasurements.size());
		allMeasurements.add(mm);
		return mm;
	}

    /**
     * keeps a track of all the latency measurements
     * the key is the type of the operation
     * the value is the object containing all latencies observed
     */
	HashMap<String,OpMeasurementTracker> data;
	int threadid = 0;
	String latencyFileName = fileNameToken+threadid+".txt";
	OutputStream out;
	public MyMeasurement(int tid)
	{
		data=new HashMap<String,OpMeasurementTracker>();	
		threadid = tid;
		latencyFileName = fileNameToken+threadid+".txt";
		/*//create or open file
		try {
			out = new FileOutputStream(latencyFileName);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}*/
	}
	
	OpMeasurementTracker constructMeasurementTrackerForOp(String name)
	{
		return new OpMeasurementTracker(name, out, expectedLatency);
	}

	/**
	 * Adds the latency observed for the operation 
	 */
    public void measure(String operation, int latency)
	{
		if (!data.containsKey(operation))
		{
			data.put(operation,constructMeasurementTrackerForOp(operation));	
		}
		try
		{
			data.get(operation).measure(latency);
		}
		catch (java.lang.ArrayIndexOutOfBoundsException e)
		{
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
			e.printStackTrace(System.out);
		}
	}

      /**
       * Report a return code for a single type of operation by a thread
       */
	public void reportReturnCode(String operation, int code)
	{
		
		if (!data.containsKey(operation))
		{
			data.put(operation,constructMeasurementTrackerForOp(operation));
		}
		data.get(operation).reportReturnCode(code);
	}
	
	
  /**
   * Return a one line summary of measurements of all the threads
   */
	public static String getSummary()
	{
		String ret="";
		Set<String> allOpTypes = null;
		//getting all the optypes different threads have performed
		//TODO: what if one thread does an extra operation
		for(int i=0; i<allMeasurements.size(); i++){
			allOpTypes = allMeasurements.get(i).data.keySet();
		}
		
		try
		{
			Iterator<String> it = allOpTypes.iterator();	
			while(it.hasNext()){
				String opType = it.next();
				int totalWindowLatency = 0;
				int totalWindowOps = 0;
				for(int i=0; i<allMeasurements.size(); i++){
						OpMeasurementTracker m = allMeasurements.get(i).data.get(opType);
						if(m!=null){
							totalWindowLatency += m.getWindowtotallatencysum();
							totalWindowOps += m.getWindownumoperations();
							m.resetWindow();
						}
				}
				DecimalFormat d = new DecimalFormat("#.##");
				double report=((double)totalWindowLatency)/((double)totalWindowOps);
				ret += "["+opType+" AverageResponseTime(us)="+d.format(report)+"]";
			}
		}
		catch(ConcurrentModificationException e)
		{
			ret = "Warning - Concurrent modification. stats output skipped";
		}
		return ret;
	}
	
	
	  /**
	   * Return a one line summary of measurements of all the threads
	   */
		public static String getFinalResults()
		{
			String ret="";
			Set<String> allOpTypes = null;
			//getting all the optypes different threads have performed
			//TODO: what if one thread does an extra operation
			for(int i=0; i<allMeasurements.size(); i++){
				allOpTypes = allMeasurements.get(i).data.keySet();
			}
			
			Iterator<String> it = allOpTypes.iterator();	
			while(it.hasNext()){
				String opType = it.next();
				long totalLatency = 0;
				long totalOps = 0;
				long max = 0;
				long min = 0;
				HashMap<Integer, Integer> returnCodes = new HashMap<Integer,Integer>();
				String rets = "";
				for(int i=0; i<allMeasurements.size(); i++){
						OpMeasurementTracker m = allMeasurements.get(i).data.get(opType);
						if(m!=null){
							totalLatency += m.getTotallatencysum();
							totalOps += m.getNumoperations();
							if(min == 0 || min > m.getMin())
								min = m.getMin();
							if(max == 0 || max < m.getMax())
								max = m.getMax();
							HashMap<Integer, int[]> tmpRetCode = m.getReturnCode();
							Set<Integer> keys = tmpRetCode.keySet();
							Iterator<Integer> tmpit = keys.iterator();
							while(tmpit.hasNext()){
								Integer tmpk = tmpit.next();
								if(returnCodes.containsKey(tmpk)){
									returnCodes.put(tmpk, (returnCodes.get(tmpk)+tmpRetCode.get(tmpk)[0]));
								}
								else{
									returnCodes.put(tmpk, tmpRetCode.get(tmpk)[0]);
								}
							}
						}
						//close latency file if open
						if(m != null) m.closePrinter();
				}
				Set<Integer> finalOpCodes = returnCodes.keySet();
				Iterator<Integer> fit = finalOpCodes.iterator();
				while(fit.hasNext()){
					int fkey = fit.next();
					rets+= "ReturnCode:"+fkey+" numObserved:"+returnCodes.get(fkey);
				}
				DecimalFormat d = new DecimalFormat("#.##");
				double report=((double)totalLatency)/((double)totalOps);
				ret += "["+opType+"]";
				ret +="NumOperations="+totalOps+", AverageResponseTime(us)="+d.format(report)+", MinResponseTime(us)="+min+", MaxResponseTime(us)="+max+"\n";
				ret += rets+"\n";
			}
			return ret;
		}
		

	
	
	/*public static Vector<Double> getAllLatencies()
	{
		Vector<Double> allLatencies = new Vector<Double>();
		for(int i=0; i<allMeasurements.size(); i++){
			//as its static all elements will return the same thing
			HashMap<String, OpMeasurementTracker> omData = allMeasurements.get(i).data;
			if(omData.size()> 0){
				Set<String> keys = omData.keySet();
				Iterator<String> it = keys.iterator();
				while(it.hasNext()){
					allLatencies.addAll(omData.get(it.next()).getLatencies());
				}
			}
		}
		//System.out.println("@@@"+allLatencies.size());
		return allLatencies;
	}*/
		
	public static double getSatisfyingPerc()
	{
		double ret=0;
		Set<String> allOpTypes = null;
		//getting all the optypes different threads have performed
		//TODO: what if one thread does an extra operation
		for(int i=0; i<allMeasurements.size(); i++){
			allOpTypes = allMeasurements.get(i).data.keySet();
		}
		int numsatisfying = 0;
		int total = 0;	
		Iterator<String> it = allOpTypes.iterator();
		while(it.hasNext()){
			String opType = it.next();
			for(int i=0; i<allMeasurements.size(); i++){
					OpMeasurementTracker m = allMeasurements.get(i).data.get(opType);
					if(m!=null){
						numsatisfying += m.getSatisfying();
						total += m.getNumoperations();
					}
			}
		}
		if(total == 0)
			ret = 0;
		else
			ret = ((double)(numsatisfying))/total*100;
		
		
		return ret;
	
	}

	public static void resetMeasurement(){
		allMeasurements = new Vector<MyMeasurement>();
		//delete the files created so far
		for(int i=0; i<allMeasurements.size(); i++){
			File f1 = new File(fileNameToken+i+".txt");
			boolean success = f1.delete();
			if (!success){
				System.out.println("Deletion failed.");
				System.exit(0);
			}
		}
	}
	
}
