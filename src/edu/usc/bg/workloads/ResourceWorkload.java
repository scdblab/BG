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


package edu.usc.bg.workloads;


import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.Random;
import java.util.Vector;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.ObjectByteIterator;
import edu.usc.bg.base.RandomByteIterator;
import edu.usc.bg.base.Workload;
import edu.usc.bg.base.WorkloadException;
import edu.usc.bg.base.generator.CounterGenerator;
/**
 * Used for loading resources for the members
 * @author barahman
 *
 */
public class ResourceWorkload extends Workload {

	// The name of the table to insert records.
	public static String table = "resources";
	// The number of fields in a record.
	public static int fieldCount = 5;
	// The length of each field for a record in Byte.
	public static int fieldLength = 100;
	// The name of each field for the "resource" table.
	public static String[] fieldName = {"rid", "creatorid", "walluserid", "type", "body", "doc"};
	// The base number for generating the random date.
	public final static long MAX_INTERVAL = 50000000000L;
	// The start of the date for generating the random date.
	public final static long BASE_INTERVAL = 1250000000000L;

	// These following fields could be kept in the property file.		
	// The average number of resources per user.
	public static int avgResourceCount = 10;
	// The number of record to be inserted.
	// The number of users.
	int userCount = 100;	
    int recordCount = userCount * avgResourceCount; // Resource number.
	int keyCounter = -1; // Counter for controlling the creator ID.
	CounterGenerator creatorSequence;
	Random random = new Random();
	int creatorNum;
	Vector<Integer> _members;
	int keyIdx;


	public ResourceWorkload() {
	}

	// Initialize all of the threads with the same configuration.
	public void init(Properties p,  Vector<Integer> members) throws WorkloadException {
		_members = members;
		userCount=Integer.parseInt(p.getProperty(Client.USER_COUNT_PROPERTY, Client.USER_COUNT_PROPERTY_DEFAULT));
		avgResourceCount=Integer.parseInt(p.getProperty(Client.RESOURCE_COUNT_PROPERTY, Client.RESOURCE_COUNT_PROPERTY_DEFAULT));
		recordCount = userCount * avgResourceCount; // number of resources
		creatorSequence = new CounterGenerator(0); // For generating creator ID.
		keyIdx = creatorSequence.nextInt();
		return;
	}


	// Return a date using the specific format.
	public String getDate(){
		Date date = new Date(random.nextLong()%MAX_INTERVAL + BASE_INTERVAL);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		String dateString = sdf.format(date);
		return dateString;
	}

	// Prepare the record for "resource" table.
	// rid, creatorId, wallUserId, type, body, doc.
	private LinkedHashMap<String, ByteIterator> buildValues(String dbKey, int creatorNum) {
		LinkedHashMap<String, ByteIterator> values = new LinkedHashMap<String, ByteIterator>();
		String fieldKey;
		ByteIterator data;
		for (int i = 1; i <= fieldCount; ++i) {
			// Generate the fields using StringByteIterator and RandomByteIterator.
			fieldKey = fieldName[i];
			if(1 == i){
				data = new ObjectByteIterator(Integer.toString(creatorNum).getBytes()); // Creator ID.
			}else if(2 == i){
				data = new ObjectByteIterator(Integer.toString(_members.get(random.nextInt(userCount))).getBytes()); // WallUser ID.
			}else if(6 == i){
				data = new ObjectByteIterator(getDate().getBytes()); // Doc.
			}else{
				data = new RandomByteIterator(fieldLength); // Other fields.
			}
			values.put(fieldKey, data);
		}
		return values;
	}

	// Prepare the primary key.
	public String buildKeyName(long keyNum) {
		String keyNumStr = "" + keyNum;
		return keyNumStr;
	}

	@Override
	public boolean doInsert(DB db, Object threadState) {

		creatorNum = _members.get(keyIdx); // Creator ID.
		if(++keyCounter == avgResourceCount){
			keyIdx = creatorSequence.nextInt();
			creatorNum = _members.get(keyIdx); // Creator ID.
			keyCounter = 0;
		}
		String dbKey = buildKeyName(creatorNum*avgResourceCount+keyCounter);
		LinkedHashMap<String, ByteIterator> values = buildValues(dbKey, creatorNum);
		//resources dont have images so insertImage = false
		if (db.insertEntity(table, dbKey, values, false) >= 0)
			return true;
		else{
			return false;
		}
	}

	@Override
	public HashMap<String, String> getDBInitialStats(DB db) {
		HashMap<String, String> stats = new HashMap<String, String>();
		stats = db.getInitialStats();
		return stats;
	}

	@Override
	public int doTransaction(DB db, Object threadstate, int threadid,
			StringBuilder updateLog, StringBuilder readLog, int seqID,
			HashMap<String, Integer> resUpdateOperations,
			HashMap<String, Integer> frienshipInfo,
			HashMap<String, Integer> pendingInfo, int thinkTime,
			boolean insertImage, boolean warmup) {
		return 0;
	}
}