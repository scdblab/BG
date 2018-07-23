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


package edu.usc.bg.base;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;


/**
 * A thread for executing transactions or data inserts to the database.
 * 
 * @author cooperb
 * 
 */
public class ClientThread extends Thread {
	DB _db;
	boolean _dotransactions;
	Workload _workload;
	int _opcount;
	double _target;

	boolean _warmup; //identifies if this thread is a warmup thread,m if it is it does not issue updates.
	int _opsdone=0; //sessions
	int _actionsDone=0;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;
	BufferedWriter updateLog; // update log file
	BufferedWriter readLog; // read log file
	HashMap<String, Integer> resUpdateOperations; // keep a track of the updates
	// done by this thread on
	// different resources
	HashMap<String, Integer> friendshipInfo; // keep a track of all friendids
	// for a user
	HashMap<String, Integer> pendingInfo; // keep a track of all friendids that
	private boolean _terminated; 										// have generated pending request for
	// this user

	boolean insertImages = false;

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


	public ClientThread(DB db, boolean dotransactions, Workload workload,
			int threadid, int threadcount, Properties props, int opcount,
			double targetperthreadperms, boolean warmup) {
		this.setName("BGClient Thread");
		set_terminated(false);
		_warmup = warmup;
		_db = db;
		_dotransactions = dotransactions;
		_workload = workload;
		_opcount = opcount;
		_opsdone = 0;
		_target = targetperthreadperms;
		_threadid = threadid;
		_threadcount = threadcount;
		_props = props;
		resUpdateOperations = new HashMap<String, Integer>();
		friendshipInfo = new HashMap<String, Integer>();
		pendingInfo = new HashMap<String, Integer>();
		insertImages = Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
				Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		String machineid = props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT);
		String dir = props.getProperty(Client.LOG_DIR_PROPERTY, Client.LOG_DIR_PROPERTY_DEFAULT);
		if(!_warmup && dotransactions){ //no file is needed if the thread is in warmup or load step
			// create file and open it
			if (true) {
				try {
					//update file
					File ufile = new File(dir+"/update"+machineid+"-"+_threadid + ".txt");
					FileWriter ufstream = new FileWriter(ufile);
					updateLog = new BufferedWriter(ufstream);
					//read file
					File rfile = new File(dir+"/read"+machineid+"-"+_threadid + ".txt");
					FileWriter rfstream = new FileWriter(rfile);
					readLog = new BufferedWriter(rfstream);
				} catch (IOException e) {
					e.printStackTrace(System.out);
				}
			}
		}
	}

	public int getOpsDone() {
		return _opsdone;
	}
	public int getActsDone() {
		return _actionsDone;
	}

	public boolean initThread() {
		try {

			_db.init();
		} catch (DBException e ) {
			e.printStackTrace(System.out);
			return false;
		}

		try {
			_workloadstate = _workload.initThread(_props, _threadid,
					_threadcount);
		} catch (WorkloadException e) {
			e.printStackTrace(System.out);
			return false;
		}
		return true;
	}

	public void run() {
		// spread the thread operations out so they don't all hit the DB at the
		// same time
		try {
			// GH issue 4 - throws exception if _target>1 because random.nextInt
			// argument must be >0
			// and the sleep() doesn't make sense for granularities < 1 ms
			// anyway
			if ((_target > 0) && (_target <= 1.0)) {
				sleep(Utils.random().nextInt((int) (1.0 / _target)));
			}
		} catch (InterruptedException e) {
			// do nothing.
		}

		try {
			if (_dotransactions) {
				long st = System.currentTimeMillis();
				int seqID = 0; // needed for determining staleness in
				// granularity of users
				int thinkTime = 0;
				boolean insertImage = false;
				int interarrivalTime = 0;

				if (_props.getProperty(Client.THINK_TIME_PROPERTY) != null) {
					thinkTime = Integer.parseInt(_props
							.getProperty(Client.THINK_TIME_PROPERTY));
				}
				if (_props.getProperty(Client.INSERT_IMAGE_PROPERTY) != null) {
					insertImage = Boolean.parseBoolean(_props.getProperty(
							Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
				}
				if (_props.getProperty(Client.INTERARRIVAL_TIME_PROPERTY) != null) {
					sleep(Integer.parseInt(_props
							.getProperty(Client.INTERARRIVAL_TIME_PROPERTY)));
					interarrivalTime = Integer.parseInt(_props
							.getProperty(Client.INTERARRIVAL_TIME_PROPERTY));
				}

				StringBuilder updateTestLog = new StringBuilder();
				StringBuilder readTestLog = new StringBuilder();
				if (!this._warmup){
					Client.threadsStart.countDown();
					Client.threadsStart.await();
				}
				Client.experimentStartTime= System.currentTimeMillis();

				/*while (((_opcount == 0) || (_opsdone < _opcount))
						&& !_workload.isStopRequested()) {*/
				while (((_opcount == 0) || (_actionsDone < _opcount))
						&& (!_workload.isStopRequested() &&!is_terminated())) {

					sleep(interarrivalTime);
					updateTestLog.delete(0, updateTestLog.length());
					readTestLog.delete(0, readTestLog.length());
					int actsDone = 0;
					if ((actsDone = _workload.doTransaction(_db, _workloadstate,
							_threadid, updateTestLog,readTestLog, seqID, resUpdateOperations,
							friendshipInfo, pendingInfo, thinkTime,
							insertImage, _warmup)) < 0) { //=0 when only perfomring actions like accept friendship and no pending frnd are there
						break;
					}

					seqID++;
					if(updateLog != null) //in warmup phase
						updateLog.write(updateTestLog.toString()); 
					if(readLog != null) //in warmup phase
						readLog.write(readTestLog.toString());

					_opsdone++; //keeps a track of the number of sessions/sequences done
					_actionsDone+=actsDone;  //keeps a track of the number of actual successful actions done


					// throttle the operations
					if (_target > 0) {
						// this is more accurate than other throttling
						// approaches we have tried,
						// like sleeping for (1/target throughput)-operation
						// latency,
						// because it smooths timing inaccuracies (from sleep()
						// taking an int,
						// current time in millis) over many operations
						while (System.currentTimeMillis() - st < ((double) _opsdone)
								/ _target) {
							try {
								sleep(1);
							} catch (InterruptedException e) {
								// do nothing.
							}

						}
					}
				}  // end doTransaction while
				//YAZ
				//				_workload.setStopRequested(true);

			} else {
				long st = System.currentTimeMillis();

				while (((_opcount == 0) || (_opsdone < _opcount))
						&& !_workload.isStopRequested()) {

					if (!_workload.doInsert(_db, _workloadstate)) {
						//break;
						System.out.println("Insertion failed. Make sure the appropriate data store schema" +
								" was created.");
						System.exit(-1);
					}
					_opsdone++;

					// throttle the operations
					if (_target > 0) {
						// this is more accurate than other throttling
						// approaches we have tried,
						// like sleeping for (1/target throughput)-operation
						// latency,
						// because it smoothens timing inaccuracies (from sleep()
						// taking an int,
						// current time in millis) over many operations
						while (System.currentTimeMillis() - st < ((double) _opsdone)
								/ _target) {
							try {
								sleep(1);
							} catch (InterruptedException e) {
								// do nothing.
							}
						}
					}
				}

			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try {
			//System.out.println("Worker thread "+_threadid+" is cleaning up");
			cleanup();
			if(updateLog != null){
				updateLog.flush();
				updateLog.close();
			}
			if(readLog != null){
				readLog.flush();
				readLog.close();
			}
		} catch (Exception e) {
			e.printStackTrace(System.out);
			e.printStackTrace(System.out);
			return;
		}
		this.interrupt();		
	}

	public void cleanup(){
		try {
			_db.cleanup(_warmup);
			this.interrupt();
		} catch (DBException e) {
			e.printStackTrace();
		}
	}

	public boolean is_terminated() {
		return _terminated;
	}

	public void set_terminated(boolean _terminated) {
		this._terminated = _terminated;
	}
}