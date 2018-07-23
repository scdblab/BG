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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import edu.usc.bg.base.Client;

public class UpdateProcessorThread extends Thread{
	Semaphore _semaphore;
	Properties _props;
	ConcurrentHashMap<String, resourceUpdateStat> _updateStats;
	Vector<logObject> _updatesToBeProcessed;
	Semaphore _putSemaphore;

	UpdateProcessorThread(Properties props, ConcurrentHashMap<String, resourceUpdateStat> updateStats, Vector<logObject> updatesToBeProcessed, Semaphore semaphore, Semaphore putSemaphore){
		_semaphore = semaphore;
		_props = props;
		_updateStats = updateStats;
		_updatesToBeProcessed = updatesToBeProcessed;
		_putSemaphore = putSemaphore;
	}


	public void run(){
		String url = _props.getProperty(ValidationMainClass.VALIDATION_DBURL_PROPERTY,
				ValidationMainClass.VALIDATION_DBURL_PROPERTY_DEFAULT);
		String user = _props.getProperty(ValidationMainClass.VALIDATION_DBUSER_PROPERTY, ValidationMainClass.VALIDATION_DBUSER_PROPERTY_DEFAULT);
		String passwd = _props.getProperty(ValidationMainClass.VALIDATION_DBPWD_PROPERTY, ValidationMainClass.VALIDATION_DBPWD_PROPERTY_DEFAULT);
		String driver = _props.getProperty(ValidationMainClass.VALIDATION_DBDRIVER_PROPERTY,
				ValidationMainClass.VALIDATION_DBDRIVER_PROPERTY_DEFAULT);
		int machineid =Integer.parseInt(_props.getProperty(Client.MACHINE_ID_PROPERTY, Client.MACHINE_ID_PROPERTY_DEFAULT));
		String tenant = _props.getProperty(ValidationMainClass.DB_TENANT_PROPERTY, ValidationMainClass.DB_TENANT_PROPERTY_DEFAULT);
		String approach = _props.getProperty(ValidationMainClass.VALIDATION_APPROACH_PROPERTY,
				ValidationMainClass.VALIDATION_APPROACH_PROPERTY_DEFAULT);
		Connection conn = null;
		Statement st = null;
		try {
			//needed to make sure only vThreads are active at a time 
			//allows the main thread to create a new update processor thread once this one is completed
			_semaphore.acquire();
			if(approach.equalsIgnoreCase("RDBMS")){
				try {
					Class.forName(driver);
					conn = DriverManager.getConnection(url, user, passwd);
					st = conn.createStatement();
				}catch(Exception e){
					e.printStackTrace(System.out);
				}
			}
			Iterator<logObject> it = _updatesToBeProcessed.iterator();
			while(it.hasNext()){
				logObject record = (logObject)(it.next());

				updateResource(record);
				
				if(approach.equalsIgnoreCase("RDBMS")){
					try {
						String sqlStr = "";
						int tableId = 1;
						if(!tenant.equalsIgnoreCase("single")) {
							tableId = Integer.parseInt(record.getThreadId())+1;
						}
						sqlStr = "INSERT INTO tupdate"+machineid+"c"+tableId+" (opType, seqid, threadid, rid, starttime, endtime, numofupdate, updatetype) VALUES ("
								+ "'"
								+ record.getMopType()
								+ "', "
								+ record.getSeqId()
								+ ", "
								+ record.getThreadId()
								+ ", "
								+ record.getRid()
								+ ", "
								+ record.getStarttime()
								+ ", "
								+ record.getEndtime()
								+ ", "
								+ record.getValue()
								+ ", '"
								+ record.getUpdatetype() + "')";
						st.executeUpdate(sqlStr);
					} catch (SQLException e) {
						e.printStackTrace(System.out);
						System.exit(0);
					} 
				}else{
					//add the interval to this resource's interval tree
					Long updateTypeInLong = 0L;
					if(record.getUpdatetype().equals("I")) {
						updateTypeInLong = 1L;
					}
					else {
						updateTypeInLong = -1L;
					}
					_updateStats.get(record.getMopType()+"-"+record.getRid()).addInterval(Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), updateTypeInLong);
					
					if (record.getMopType().equals("PENDFRND") || record.getMopType().equals("ACCEPTFRND"))
						_updateStats.get("TOTALCNT-" + record.getRid()).addInterval(Long.parseLong(record.getStarttime()), Long.parseLong(record.getEndtime()), updateTypeInLong);
				}	

			}
			//needed to make sure only vThreads are active at a time 
			//allows the main thread to create a new update processor thread once this one is completed
			_semaphore.release();
		} catch (InterruptedException e1) {
			e1.printStackTrace(System.out);
		}finally{
			try {
				if(st != null) st.close();
				if(conn != null) conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}


	/**
	 * @param record
	 * @throws InterruptedException
	 * check if an update has been seen for this resource before
	 * if so then update the available structures
	 */
	private void updateResource(logObject record) throws InterruptedException {
		_putSemaphore.acquire();
		resourceUpdateStat newVal = _updateStats.get(record.getMopType()+"-"+record.getRid());
		if(newVal == null) {//record doesn't exist
			newVal = new resourceUpdateStat();
			newVal.setMinStartTime(record.getStarttime());
			newVal.setMaxEndTime(record.getEndtime());
			String updateTypeNewValue = (record.getUpdatetype().equalsIgnoreCase("I")) ? "1" : "-1";
			newVal.setFinalVal(updateTypeNewValue);
			_updateStats.put(record.getMopType()+"-"+record.getRid(), newVal);
		}
		else{
			String tempValMinS = newVal.getMinStartTime();
			String tempValMaxE = newVal.getMaxEndTime();
			String tempValV = newVal.getFinalVal();

			//update min start time if needed
			if(Long.parseLong(tempValMinS) > Long.parseLong(record.getStarttime()))
				newVal.setMinStartTime(record.getStarttime());
			else
				newVal.setMinStartTime(tempValMinS);

			//update max end  time if needed
			if(Long.parseLong(tempValMaxE) < Long.parseLong(record.getEndtime())) {
				newVal.setMaxEndTime(record.getEndtime());
			}
			else {
				newVal.setMaxEndTime(tempValMaxE);
			}
			
			if(record.getUpdatetype().equalsIgnoreCase("I")) {
				newVal.setFinalVal((Integer.parseInt(tempValV)+1)+"");
			}
			else {
				newVal.setFinalVal((Integer.parseInt(tempValV)-1)+"");
			}
		}
		
		if (record.getMopType().equals("ACCEPTFRND") || record.getMopType().equals("PENDFRND")) {
			// update the TOTALCOUNT number
			resourceUpdateStat totalCountStats = _updateStats.get("TOTALCNT-"+record.getRid());
			
			if(totalCountStats == null) {//record doesn't exist
				totalCountStats = new resourceUpdateStat();
				totalCountStats.setMinStartTime(record.getStarttime());
				totalCountStats.setMaxEndTime(record.getEndtime());
				String updateTypeNewValue = (record.getUpdatetype().equalsIgnoreCase("I")) ? "1" : "-1";
				totalCountStats.setFinalVal(updateTypeNewValue);
				_updateStats.put("TOTALCNT-"+record.getRid(), totalCountStats);
			}
			else{
				String tempValMinS = totalCountStats.getMinStartTime();
				String tempValMaxE = totalCountStats.getMaxEndTime();
				String tempValV = totalCountStats.getFinalVal();
	
				//update min start time if needed
				if(Long.parseLong(tempValMinS) > Long.parseLong(record.getStarttime()))
					totalCountStats.setMinStartTime(record.getStarttime());
				else
					totalCountStats.setMinStartTime(tempValMinS);
	
				//update max end  time if needed
				if(Long.parseLong(tempValMaxE) < Long.parseLong(record.getEndtime())) {
					totalCountStats.setMaxEndTime(record.getEndtime());
				}
				else {
					totalCountStats.setMaxEndTime(tempValMaxE);
				}
				
				if(record.getUpdatetype().equalsIgnoreCase("I")) {
					totalCountStats.setFinalVal((Integer.parseInt(tempValV)+1)+"");
				}
				else {
					totalCountStats.setFinalVal((Integer.parseInt(tempValV)-1)+"");
				}
			} 
		}
		
		_putSemaphore.release();
	}
}