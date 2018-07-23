/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
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

import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One experiment scenario. One object of this type will
 * be instantiated and shared among all client threads. This class
 * should be constructed using a no-argument constructor, so we can
 * load it dynamically. Any argument-based initialization should be
 * done by init().
 * 
 */
public abstract class Workload
{
	/**
	 * needed for stopping the client threads when issuing requests
	 */
	private volatile AtomicBoolean stopRequested = new AtomicBoolean(false);

	/**
	 * Initialize the scenario. Create any generators and other shared objects here.
	 * Called once, in the main client thread, before any operations are started.
	 * Also called when client threads load the database.
	 * @param p The properties.
	 * @param members Needed for the load phase, keeps a track of the set of memberids that the BG client is responsible for.
	 */
	public void init(Properties p, Vector<Integer> members) throws WorkloadException
	{
	}
	
	/**
	 * Initialize the internal data structures keeping a track of databases state. Create any generators and other shared objects here.
	 * Called with the continual mode of executing BG.
	 * @param p The properties.
	 * @param executionMode (0 stands for single and 1 stands for continual mode of operation)
	 * @return true if everything with settinguo the structures is fine or if the structures are already available
	 */
	public boolean resetDBInternalStructures(Properties p, int executionMode)
	{
		return true;
	}

	/**
	 * Initialize any state for a particular client thread. Since the scenario object
	 * will be shared among all threads, this is the place to create any state that is specific
	 * to one thread. To be clear, this means the returned object should be created anew on each
	 * call to initThread(); do not return the same object multiple times. 
	 * The returned object will be passed to invocations of doInsert() and doTransaction() 
	 * for this thread. There should be no side effects from this call; all state should be encapsulated
	 * in the returned object. If you have no state to retain for this thread, return null. (But if you have
	 * no state to retain for this thread, probably you don't need to override initThread().)
	 * 
	 * @return false if the workload knows it is done for this thread. Client will terminate the thread. Return true otherwise. Return true for workloads that rely on operationcount. 
	 * For workloads that read traces from a file, return true when there are more to do, false when you are done.
	 */
	public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException
	{
		return null;
	}

	/**
	 * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
	 */
	public void cleanup() throws WorkloadException
	{
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
	 * synchronized, since each thread has its own threadstate instance.
	 */
	public abstract boolean doInsert(DB db, Object threadstate);

	/**
	 * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
	 * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
	 * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
	 * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
	 * synchronized, since each thread has its own threadstate instance.
	 * @param updateLog The update log record created by that transaction which needs to be written to the update file.
	 * @param readLog The read log record created by that transaction which needs to be written to the update file.
	 * @param seqID  For workloads issuing sessions keeps a track of the current sessionid.
	 * @param resUpdateOperations  Keeps a track of all the postcomment updates issued - needed for validation based on exact values.
	 * @param friendshipInfo Keeps a track of all the confirmed friendships created throughout the benchmark execution - needed for validation based on exact values.
	 * @param pendingInfo Keeps a track of all the pending invites created during the benchmark execution - needed for validation based on exact values.
	 * @param thinkTime Think time between the actions in a session.
	 * @param insertImage  If set to true means images will be retrieved for relevant actions i.e. getUserProfile.
	 * @param warmup If set to true the workload would not issue any update actions.
	 * @return greater than 0 if everything is fine.
	 */

	public abstract int doTransaction(DB db, Object threadstate, int threadid, StringBuilder updateLog, StringBuilder readLog ,int seqID, HashMap<String, Integer> resUpdateOperations
			,HashMap<String, Integer> friendshipInfo, HashMap<String, Integer> pendingInfo, int thinkTime,boolean insertImage, boolean warmup);
	

	/**
	 * Gets the initial statistics from the data stores: member count, friend count per user, pending friend count per user and the resource count per user
	 * assuming all users initially have the same number of confirmed and pending friendships.
	 */

	public abstract HashMap<String, String> getDBInitialStats(DB db);
	
	/**
	 * Allows scheduling a request to stop the workload.
	 */
	public void requestStop() {
		stopRequested.set(true);
		System.out.println("Stop request is set to :"+stopRequested);
	}

	/**
	 * Check the status of the stop request flag.
	 * @return true if stop was requested, false otherwise.
	 */
	public boolean isStopRequested() {
		if (stopRequested.get() == true) return true;
		else return false;
	}
	
	/**
	 * Check the status of the stop request flag.
	 * return true if stop was requested, false otherwise.
	 */
	public void setStopRequested(boolean value) {
		stopRequested.set(value);
	}

}
