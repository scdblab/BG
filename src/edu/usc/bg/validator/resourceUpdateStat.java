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


package edu.usc.bg.validator;

import java.util.List;
/**
 * keeps a track of the first update details for a resource and its related interval if applicable 
 * constructs the appropriate interval tree or hashmap
 * @author barahman
 *
 */
public class resourceUpdateStat {
	//the min time when an update started on this resource
	//the max time when an update ended on this resource
	//the final value expected for this resource
	//timeIntTree is used when the intervaltree approach is used for validation

	String minStartTime="0", maxEndTime="0", finalVal="0";
	IntervalTree<Long> timeIntTree = new IntervalTree<Long>();  //there should be only one instance of this for every resource
	
	resourceUpdateStat(){

	}

	public synchronized void addInterval(long start, long end, long updateType){
		timeIntTree.addInterval(start, end, updateType);
		//List<Interval<Long>> its = timeIntTree.getIntervals(start, end);
		//System.out.println(((Interval<Long>)(its.get(0))).getData()+" "+((Interval<Long>)(its.get(0))).getStart()+" "+((Interval<Long>)(its.get(0))).getEnd());
		/*List<Long> its2 = timeIntTree.get(start, end);
		System.out.println("**"+((Long)(its2.get(0))).longValue());
		*/
	}
	
	public List<Interval<Long>> queryIntervalTree(long start, long end){
		return timeIntTree.getIntervals(start, end);
	}
	
	
	public String getMinStartTime() {
		return minStartTime;
	}

	public void setMinStartTime(String minStartTime) {
		this.minStartTime = minStartTime;
	}

	public String getMaxEndTime() {
		return maxEndTime;
	}

	public void setMaxEndTime(String maxEndTime) {
		this.maxEndTime = maxEndTime;
	}

	public String getFinalVal() {
		return finalVal;
	}

	public void setFinalVal(String finalVal) {
		this.finalVal = finalVal;
	}


}
