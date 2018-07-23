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
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;



/**
 * Take measurements and maintains min, max, avg for each type of operation for each thread
 * 
 * @author barahman
 *
 */
public class OpMeasurementTracker
{
	//private static int LATENCY_ARRAY_SIZE =1000;
	//keep a track of the latencies seen by a thread for every op
	//its static so there is one per thread per op type
	//public Vector<Double> latencies = new Vector<Double>();
	//double[] tmpLatencies = new double[LATENCY_ARRAY_SIZE];
	long numoperations;
	long totallatencysum;
	//keep a windowed version of these stats for printing status
	long windownumoperations;
	long windowtotallatencysum;
	long min;
	long max;
	String opname;
	StatsPrinter printer;
	//return codes for each thread's this kind of operation
	HashMap<Integer,int[]> returncodes;
	long numsatisfyingops = 0;
	double expected = 0;

	public long getSatisfying() {
		return numsatisfyingops;
	}

	public long getNumoperations() {
		return numoperations;
	}

	public long getTotallatencysum() {
		return totallatencysum;
	}


	public long getWindownumoperations() {
		return windownumoperations;
	}

	public long getWindowtotallatencysum() {
		return windowtotallatencysum;
	}

	public long getMin() {
		return min;
	}

	public long getMax() {
		return max;
	}

	public String getOpname() {
		return opname;
	}

	public HashMap<Integer, int[]>getReturnCode(){
		return returncodes;
	}

	public OpMeasurementTracker(String name, OutputStream out, double expectedLatency)
	{
		numoperations=0;
		totallatencysum=0;
		windownumoperations=0;
		windowtotallatencysum=0;
		opname = name;
		min=-1;
		max=-1;
		expected = expectedLatency;
		//keeps a track of the return codes for this optype by one thread
		//the key is the return code
		//the value basically shows how many ops observed this return code
		returncodes=new HashMap<Integer,int[]>();
		/*try {
			printer = new StatsPrinter(out);
		} catch (Exception e) {
			e.printStackTrace(System.out);

		}*/
	}

	public void reportReturnCode(int code)
	{
		Integer Icode=code;
		if (!returncodes.containsKey(Icode))
		{
			int[] val=new int[1];
			val[0]=0;
			returncodes.put(Icode,val);
		}
		returncodes.get(Icode)[0]++;
	}


	public void measure(long latency)
	{
		numoperations++;
		totallatencysum+=latency;
		windownumoperations++;
		windowtotallatencysum+=latency;
		//do comparison in milliseconds
		if((((double)(latency))/1000) < (expected*1000))
			numsatisfyingops++;
		//write it to the latency file of this thread
		/*try {
			printer.write((((double)(latency))/1000)+"");
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		/*tmpLatencies[(numoperations-1)%LATENCY_ARRAY_SIZE] = ((double)(latency))/1000;
		if(numoperations%LATENCY_ARRAY_SIZE == 0){
			Vector<Double> test= arrayToVector(tmpLatencies);
			latencies.addAll(test);
		}*/

		if ( (min<0) || (latency<min) )
		{
			min=latency;
		}

		if ( (max<0) || (latency>max) )
		{
			max=latency;
		}
	}

	/**
	 * once the status thread reads the stats it resets this window
	 */
	public void resetWindow() {
		windowtotallatencysum=0;
		windownumoperations=0;
	}

	public void closePrinter(){
		try {
			if(printer != null)
				printer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/*@SuppressWarnings("unchecked")
	public Vector<Double> getLatencies(){
		Vector<Double> allLatencies = (Vector<Double>) latencies.clone();
		if(numoperations%LATENCY_ARRAY_SIZE != 0){
			int remaining = numoperations%LATENCY_ARRAY_SIZE;
			for(int i=0; i<remaining; i++){
				allLatencies.add(tmpLatencies[i]);
			}
		}
		return allLatencies;
	}


	public Vector<Double> arrayToVector(double[] latencies){
		Vector<Double> lat = new Vector<Double>();
		for(int i=0; i<latencies.length; i++)
			lat.add(latencies[i]);
		return lat;


	}*/

}
