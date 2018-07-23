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

package sqlTrigSemiData;

import com.rays.cosar.COSARException;
import com.rays.cosar.COSARInterface;
import com.rays.cosar.CacheConnectionPool;
import common.CacheUtilities;
import edu.usc.bg.base.ByteArrayByteIterator;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;

public class SqlTrigSemiDataClient extends DB implements JdbcDBClientConstants {

	
	private static boolean verbose = false;
	
	private static boolean initialized = false;
	private Properties props;
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	private PreparedStatement preparedStatement = null;
	
	private Connection conn;
	
	private static int GETFRNDCNT_STMT = 2;
	private static int GETPENDCNT_STMT = 3;
	private static int GETRESCNT_STMT = 4;
	private static int GETPROFILE_STMT = 5;
	private static int GETPROFILEIMG_STMT = 6;
	private static int GETFRNDS_STMT = 7;
	private static int GETFRNDSIMG_STMT = 8;
	private static int GETPEND_STMT = 9;
	private static int GETPENDIMG_STMT = 10;
	private static int REJREQ_STMT = 11;
	private static int ACCREQ_STMT = 12;
	private static int INVFRND_STMT = 13;
	private static int UNFRNDFRND_STMT = 14;
	private static int GETTOPRES_STMT = 15;
	private static int GETRESCMT_STMT = 16;
	private static int POSTCMT_STMT = 17;
	private static int DELCMT_STMT = 18;
	
	private static int IMAGE_SIZE_GRAN = 1024;
	private int THUMB_IMAGE_SIZE = 2*1024;
	
	//String cache_cmd = "C:\\PSTools\\psexec \\\\"+COSARServer.cacheServerHostname+" -u shahram -p 2Shahram C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";	
	//String cache_cmd = "C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
	
	private static boolean ManageCOSAR = false;
	private static String LOGGER = "ERROR, A1";
	private String cache_hostname = "";
	private Integer cache_port = -1;
	private PreparedStatement lockStatement;
	//StartCOSAR st;
	private static final int COSAR_WAIT_TIME = 10000;
	
	public static final int CACHE_POOL_NUM_CONNECTIONS = 200;
	private static AtomicInteger NumThreads = null;
	private static Semaphore crtcl = new Semaphore(1, true);
	//private static final int deserialize_buffer_size = 65536;
	//private static int NumThreads = 0;
	private String FSimagePath = ""; 
	
	//private static boolean useTTL = false;

	
		
	private static int incrementNumThreads() {
        int v;
        do {
            v = NumThreads.get();
        } while (!NumThreads.compareAndSet(v, v + 1));
        return v + 1;
    }
	
	private static int decrementNumThreads() {
        int v;
        do {
            v = NumThreads.get();
        } while (!NumThreads.compareAndSet(v, v - 1));
        return v - 1;
    }
	
		
	
	
	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}
	
	private void cleanupAllConnections() throws SQLException {
		//close all cached prepare statements
		Set<Integer> statementTypes = newCachedStatements.keySet();
		Iterator<Integer> it = statementTypes.iterator();
		while(it.hasNext()){
			int stmtType = it.next();
			if(newCachedStatements.get(stmtType) != null) newCachedStatements.get(stmtType).close();
		}
		if (conn != null) conn.close();
	}
	

	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 */
	@Override

	public boolean init() throws DBException {

		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);

		String driver = props.getProperty(DRIVER_CLASS, DEFAULT_PROP);  //"oracle.jdbc.driver.OracleDriver"
		FSimagePath = props.getProperty(common.RDBMSClientConstants.FS_PATH, common.RDBMSClientConstants.FS_PATH_DEFAULT);
		
		boolean compressPayload = Boolean.parseBoolean(props.getProperty(ENABLE_COMPRESSION_PROPERTY, ENABLE_COMPRESSION_PROPERTY_DEFAULT));
		
		cache_hostname = props.getProperty(CACHE_SERVER_HOST, CACHE_SERVER_HOST_DEFAULT);
		cache_port = Integer.parseInt(props.getProperty(CACHE_SERVER_PORT, CACHE_SERVER_PORT_DEFAULT));
		
		ManageCOSAR = Boolean.parseBoolean(
				props.getProperty(MANAGE_CACHE_PROPERTY, 
						MANAGE_CACHE_PROPERTY_DEFAULT));
		
		
		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url: urls.split(",")) {
				System.out.println("Adding shard node URL: " + url);
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface, all
				// operations should auto commit.
				conn.setAutoCommit(true); 
			}

			newCachedStatements = new ConcurrentHashMap<Integer, PreparedStatement>();
			lockStatement = conn.prepareStatement("LOCK TABLE ? IN EXCLUSIVE MODE");
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBS driver: " + e);
			e.printStackTrace(System.out);
			return false;
		} catch (SQLException e) {
			System.out.println("Error in database operation: " + e);
			System.out.println("Continuing execution...");
			e.printStackTrace(System.out);
			return false;
			//throw new DBException(e);
		} catch (NumberFormatException e) {
			System.out.println("Invalid value for fieldcount property. " + e);
			e.printStackTrace(System.out);
			return false;
		}


		try {
			crtcl.acquire();
			if(NumThreads == null)
			{
				NumThreads = new AtomicInteger();
				NumThreads.set(0);
			}
			
			incrementNumThreads();
		}catch (Exception e){
			System.out.println("SQLTrigQR init failed to acquire semaphore.");
			e.printStackTrace(System.out);
		}
		if (initialized) {
			crtcl.release();
			//System.out.println("Client connection already initialized.");
			return true;
		}

		//Register functions that invoke the cache manager's delete method
		MultiDeleteFunction.RegFunctions(driver, urls, user, passwd);

		com.rays.cosar.COSARInterface.SEND_ENABLED = true;
		com.rays.cosar.COSARInterface.OBJECT_CACHING_ENABLED = true;
		com.rays.cosar.COSARInterface.COMPRESS_QUERY_RESULTS = compressPayload;
		com.rays.cosar.COSARInterface.STRICT_CONSISTENCY_ENABLED = true; //Gumball requires this to be true
		com.rays.cosar.COSARInterface.TRIGGER_REGISTRATION_ENABLED = true;
		com.rays.sql.Connection.setQueryCachingEnabled(! com.rays.cosar.COSARInterface.OBJECT_CACHING_ENABLED);
		com.rays.sql.Statement.setTransparentCaching(false); //When true, it attempts to generate triggers and cache queries before using the developer provided re-writes.
		
		//com.rays.cosar.COSARInterface.setTTL(Integer.parseInt(props.getProperty("ttlvalue", "0")));
		//useTTL = Integer.parseInt(props.getProperty("ttlvalue", "0")) != 0;

		if (ManageCOSAR) {
			int listener_port = Integer.parseInt(props.getProperty(common.CacheClientConstants.LISTENER_PORT, common.CacheClientConstants.LISTENER_PORT_DEFAULT));
			common.CacheUtilities.startCOSAR(cache_hostname, listener_port);
		}

		Properties prop = new Properties();
		prop.setProperty("log4j.rootLogger", LOGGER);
		//prop.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
		prop.setProperty("log4j.appender.A1", "org.apache.log4j.FileAppender");
		prop.setProperty("log4j.appender.A1.File", "workloadgen.out");
		prop.setProperty("log4j.appender.A1.Append", "false");
		prop.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
		prop.setProperty("log4j.appender.A1.layout.ConversionPattern", "%-4r %-5p [%t] %37c %3x - %m%n");
		PropertyConfigurator.configure(prop);

		int CACHE_NUM_PARTITIONS=1;

		CacheConnectionPool.clearServerList();
		System.out.println("Add connection to "+CONNECTION_URL);
		for( int i = 0; i < CACHE_NUM_PARTITIONS; i++ )
		{
			CacheConnectionPool.addServer(cache_hostname, cache_port + i);
		}


		try {
			CacheConnectionPool.init(CACHE_POOL_NUM_CONNECTIONS, true);
		} catch (COSARException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		
		//register triggers for all actions by one thread
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		viewProfile(1, 1, result, Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT)), false);
		Vector<HashMap<String, ByteIterator>> result2 = new Vector<HashMap<String, ByteIterator>>();
		listFriends(1, 1, null, result2, Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT)), false);
		result2 = new Vector<HashMap<String, ByteIterator>>();
		viewFriendReq(1, result2, Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT)), false);
		result2 = new Vector<HashMap<String, ByteIterator>>();
		viewTopKResources(1, 1, 5, result2);		
		result2 = new Vector<HashMap<String, ByteIterator>>();
		viewCommentOnResource(1, 1, 1, result2);
		
		try {
			System.out.println("Waiting 5 seconds for trigger registrations.");
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}


		initialized = true;
		try {
			crtcl.release();
		} catch (Exception e) {
			System.out.println("SQLTrigQR cleanup failed to release semaphore.");
			e.printStackTrace(System.out);
		}
		return true;
	}

	@Override
	public void cleanup(boolean warmup) throws DBException {
		try {
			if (warmup) decrementNumThreads();
			if (verbose) System.out.println("Cleanup (before warmup-chk):  NumThreads="+NumThreads.get());
			if(!warmup){
				crtcl.acquire();
				decrementNumThreads();

				if (verbose) System.out.println("Cleanup (after warmup-chk):  NumThreads="+NumThreads.get());
				if (NumThreads.get() > 0){
					crtcl.release();
					//cleanupAllConnections();
					//System.out.println("Active clients; one of them will clean up the cache manager.");
					return;
				}

				COSARInterface cosar = CacheConnectionPool.getConnection(0);
				cosar.printStats();
				cosar.resetStats();
				CacheConnectionPool.returnConnection(cosar, 0);

				CacheConnectionPool.shutdown();

				if (ManageCOSAR){
					COSARInterface cache_conn = new COSARInterface(cache_hostname, cache_port);			
					cache_conn.shutdownServer();
					System.out.print("Stopping COSAR...");
					Thread.sleep(10000);
					System.out.println("Done!");
				}
				cleanupAllConnections();
				crtcl.release();
			}
		} catch (InterruptedException IE) {
			System.out.println("Error in cleanup:  Semaphore interrupt." + IE);
			throw new DBException(IE);
		} catch (Exception e) {

			System.out.println("Error in closing the connection. " + e);
			throw new DBException(e);
		}
	}

	
	
	
	@Override
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
		return common.RdbmsUtilities.insertEntity(entitySet, entityPK, values, insertImage, conn, "");
	}

	
	
	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {

		if (verbose) System.out.print("Get Profile "+requesterID+" "+profileOwnerID);
		ResultSet rs = null;
		int retVal = SUCCESS;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		byte[] payload;
		String key;
		String query="";

		COSARInterface cosar=null;
		HashMap<String, ByteIterator> SR = new HashMap<String, ByteIterator>(); 

		
		
		// Check Cache first
		if(insertImage)
		{
			key = "profile"+profileOwnerID;
		}
		else
		{
			key = "profileNoImage"+profileOwnerID;
		}
		
		if(requesterID == profileOwnerID)
		{
			key = "own" + key;
		}
		
		try {
			cosar = CacheConnectionPool.getConnection(key);
			payload = cosar.retrieveBytesFromCache(key);
			if (payload != null && CacheUtilities.unMarshallHashMap(result, payload)){
				if (verbose) System.out.println("... Cache Hit!");
				CacheConnectionPool.returnConnection(cosar, key);
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (COSARException e1) {
			e1.printStackTrace(System.out);
			System.exit(-1);
		}
			

		// Initialize query logging for the send procedure
		cosar.startQueryLogging();
				
		try {
			//friend count
			if (verbose) System.out.print("... Query DB!");
			
			query = "SELECT count(*) FROM  friendship WHERE (inviterID = ? OR inviteeID = ?) AND status = 2 ";
			//query = "SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ";
			if((preparedStatement = newCachedStatements.get(GETFRNDCNT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETFRNDCNT_STMT, query);
			
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, profileOwnerID);
			cosar.addQuery("SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ");

			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "friendship");			

			String Value="0";
			if (rs.next()){
				Value = rs.getString(1);
				result.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
			}else
				result.put("friendcount", new ObjectByteIterator("0".getBytes())) ;

			//serialize the result hashmap and insert it in the cache for future use
			SR.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes()));


		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}

		//pending friend request count
		//if owner viwing her own profile, she can view her pending friend requests
		if(requesterID == profileOwnerID){
			//key = "friendReqCnt"+profileOwnerID;

			try {
				query = "SELECT count(*) FROM  friendship WHERE inviteeID = ? AND status = 1 ";
				//preparedStatement = conn.prepareStatement(query);
				if((preparedStatement = newCachedStatements.get(GETPENDCNT_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDCNT_STMT, query);
				
				preparedStatement.setInt(1, profileOwnerID);
				cosar.addQuery("SELECT count(*) FROM  friendship WHERE inviteeID = "+profileOwnerID+" AND status = 1 ");
				
				rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "friendship");
				String Value = "0";
				if (rs.next()){
					Value = rs.getString(1);
					result.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
				}
				else
					result.put("pendingcount", new ObjectByteIterator("0".getBytes())) ;

				//serialize the result hashmap and insert it in the cache for future use
				SR.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
			}catch(SQLException sx){
				retVal = -2;
				sx.printStackTrace(System.out);
			}finally{
				try {
					if (rs != null)
						rs.close();
					if(preparedStatement != null)
						preparedStatement.clearParameters();
						//preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
					retVal = -2;
				}
			}
		}

		
		//resource count
		query = "SELECT count(*) FROM  resources WHERE wallUserID = ?";

		try {
			//preparedStatement = conn.prepareStatement(query);
			if((preparedStatement = newCachedStatements.get(GETRESCNT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCNT_STMT, query);
			
			preparedStatement.setInt(1, profileOwnerID);
			cosar.addQuery("SELECT count(*) FROM  resources WHERE wallUserID = "+profileOwnerID);
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "resources");
			if (rs.next()){
				SR.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
				result.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
			} else {
				SR.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
				result.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = ?";
				cosar.addQuery("SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = "+profileOwnerID);
				if((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);
				
			}
			else{
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
				cosar.addQuery("SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = "+profileOwnerID);
				if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);
				
			}
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "users");
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			String uid = null;
			if(rs.next()){
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value ="";
					if(col_name.equalsIgnoreCase("pic")){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-ctprofimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}
						}
						
						//byte[] val = new byte[allBytesInBlob.length];
						//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
						SR.put(col_name, new ObjectByteIterator(allBytesInBlob));
					} else if (col_name.equalsIgnoreCase("userid")){
						uid = rs.getString(col_name);
					} else{
						value = rs.getString(col_name);
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
						SR.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
				}
				
				//Fetch the profile image from the file system
				if (insertImage && !FSimagePath.equals("") ){
					//Get the profile image from the file
					byte[] profileImage = common.RdbmsUtilities.GetImageFromFS(uid, true, FSimagePath);
					if(testMode){
						//dump to file
						try{
							FileOutputStream fos = new FileOutputStream(profileOwnerID+"-proimage.bmp");
							fos.write(profileImage);
							fos.close();
						}catch(Exception ex){
						}
					}
					result.put("pic", new ObjectByteIterator(profileImage) );
					SR.put("pic", new ObjectByteIterator(profileImage) );
				}
			}
			
			

		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		//serialize the result hashmap and insert it in the cache for future use
		payload = CacheUtilities.SerializeHashMap(SR);
		try {
			cosar.sendToCache(key, payload, this.conn);
			CacheConnectionPool.returnConnection(cosar, key);
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}
		return retVal;
	}


	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {
		if (verbose) System.out.print("List friends... "+profileOwnerID);
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		//String key = "lsFrds:"+requesterID+":"+profileOwnerID;
		String key;
		String query="";
		COSARInterface cosar=null;
		
		if(insertImage)
		{
			key = "lsFrds:"+profileOwnerID;
		}
		else
		{
			key = "lsFrdsNoImage:"+profileOwnerID;
		}
		// Check Cache first
		try {
			cosar = CacheConnectionPool.getConnection(key);
			byte[] payload = cosar.retrieveBytesFromCache(key);
			if (payload != null){
				if (verbose) System.out.println("... Cache Hit!");
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
				CacheConnectionPool.returnConnection(cosar, key);
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (COSARException e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to serialize a vector of hashmaps.");
			retVal = -2;
		}
		// Initialize query logging for the send procedure
		cosar.startQueryLogging();

		
		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
				cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
				if((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);
				
			}else{
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
				cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
				if((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);
				
			}
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, profileOwnerID);
			String tables_to_lock[] = {"friendship", "users"};
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, tables_to_lock);
			int cnt=0;
			String uid = null;
			while (rs.next()){
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				if (fields != null) {
					for (String field : fields) {
						String value = rs.getString(field);
						if(field.equalsIgnoreCase("userid"))
							field = "userid";
						values.put(field, new ObjectByteIterator(value.getBytes()));
					}
					result.add(values);
				}else{
					//get the number of columns and their names
					//Statement st = conn.createStatement();
					//ResultSet rst = st.executeQuery("SELECT * FROM users");
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++){
						String col_name = md.getColumnName(i);
						String value ="";
						if(col_name.equalsIgnoreCase("tpic")){
							// Get as a BLOB
							Blob aBlob = rs.getBlob(col_name);
							byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
							
							if(testMode){
								//dump to file
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-cthumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								}catch(Exception ex){
								}
							}
							
							//byte[] val = new byte[allBytesInBlob.length];
							//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
							values.put(col_name, new ObjectByteIterator(allBytesInBlob));
						}else{
							value = rs.getString(col_name);
							if(col_name.equalsIgnoreCase("userid")){
								uid = value;
								col_name = "userid";
							}
							values.put(col_name, new ObjectByteIterator(value.getBytes()));
						}						
					}
					//Fetch the thumbnail image from the file system
					if (insertImage && !FSimagePath.equals("") ){
						byte[] thumbImage = common.RdbmsUtilities.GetImageFromFS(uid, false, FSimagePath);
						//Get the thumbnail image from the file
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
								fos.write(thumbImage);
								fos.close();
							}catch(Exception ex){
							}
						}
						values.put("tpic", new ObjectByteIterator(thumbImage) );
					}
					result.add(values);
				}
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		//serialize the result hashmap and insert it in the cache for future use
		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
		try {
			cosar.sendToCache(key, payload, this.conn);
			CacheConnectionPool.returnConnection(cosar, key);
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}

		return retVal;		
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result,  boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		ResultSet rs = null;

		if (verbose) System.out.print("viewPendingRequests "+profileOwnerID+" ...");

		if(profileOwnerID < 0)
			return -1;

		String key;
		String query="";
		COSARInterface cosar=null;
		
		if(insertImage)
		{
			key = "viewPendReq:"+profileOwnerID;
		}
		else
		{
			key = "viewPendReqNoImage:"+profileOwnerID;
		}
		// Check Cache first
		try {
			cosar = CacheConnectionPool.getConnection(key);
			byte[] payload = cosar.retrieveBytesFromCache(key);
			if (payload != null){
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
				if (verbose) System.out.println("... Cache Hit!");
				CacheConnectionPool.returnConnection(cosar, key);
				return retVal;
			} else if (verbose) System.out.println("... Query DB.");
		} catch (COSARException e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
			retVal = -2;
		}
		// Initialize query logging for the send procedure
		cosar.startQueryLogging();


		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
				if((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);
				
			}else{ 
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
				if((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);
				
			}
			String tables_to_lock[] = {"friendship", "users"};
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, tables_to_lock);
			int cnt=0;
			String uid = null;
			while (rs.next()){
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = "";
					if(col_name.equalsIgnoreCase("tpic") ){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-ctthumbimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}
						}
						
						//byte[] val = new byte[allBytesInBlob.length];
						//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
						values.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else{
						value = rs.getString(col_name);
						if(col_name.equalsIgnoreCase("userid")){
							uid = value;
							col_name = "userid";
						}
						values.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
				}
				
				//Fetch the thumbnail image from the file system
				if (insertImage && !FSimagePath.equals("") ){
					byte[] thumbImage = common.RdbmsUtilities.GetImageFromFS(uid, false, FSimagePath);
					//Get the thumbnail image from the file
					if(testMode){
						//dump to file
						try{
							FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
							fos.write(thumbImage);
							fos.close();
						}catch(Exception ex){
						}
					}
					values.put("tpic", new ObjectByteIterator(thumbImage) );
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
		try {
			cosar.sendToCache(key, payload, this.conn);
			CacheConnectionPool.returnConnection(cosar, key);
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}
		return retVal;		
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;
		String query;
		query = "UPDATE friendship SET status = 2 WHERE inviterid=? and inviteeid= ? ";
		try{
			if((preparedStatement = newCachedStatements.get(ACCREQ_STMT)) == null)
				preparedStatement = createAndCacheStatement(ACCREQ_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "friendship");
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;		
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;

		String query = "DELETE FROM friendship WHERE inviterid=? and inviteeid= ? ";
		try {
			if((preparedStatement = newCachedStatements.get(REJREQ_STMT)) == null)
					preparedStatement = createAndCacheStatement(REJREQ_STMT, query);
				
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "friendship");
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;	
	}

	@Override
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;

		String query = "INSERT INTO friendship values(?,?,1)";
		try {
			if((preparedStatement = newCachedStatements.get(INVFRND_STMT)) == null)
				preparedStatement = createAndCacheStatement(INVFRND_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "friendship");
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2){
		int retVal = SUCCESS;
		if(friendid1 < 0 || friendid2 < 0)
			return -1;

		String query = "DELETE FROM friendship WHERE (inviterid=? and inviteeid= ?) OR (inviterid=? and inviteeid= ?) and status=2";
		try {
			if((preparedStatement = newCachedStatements.get(UNFRNDFRND_STMT)) == null)
				preparedStatement = createAndCacheStatement(UNFRNDFRND_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, friendid1);
			preparedStatement.setInt(2, friendid2);
			preparedStatement.setInt(3, friendid2);
			preparedStatement.setInt(4, friendid1);

			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "friendship");
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0 || requesterID < 0 || k < 0)
			return -1;
		if (verbose) System.out.print("getTopKResources "+profileOwnerID+" ...");

		String key = "TopKRes:"+profileOwnerID;
		COSARInterface cosar=null;
		// Check Cache first
		try {
			cosar = CacheConnectionPool.getConnection(key);
			byte[] payload = cosar.retrieveBytesFromCache(key);
			if (payload != null && CacheUtilities.unMarshallVectorOfHashMaps(payload,result)){
				if (verbose) System.out.println("... Cache Hit!");
				CacheConnectionPool.returnConnection(cosar, key);
				return retVal;
			}
			else if (verbose) System.out.print("... Query DB!");
		} catch (COSARException e1) {
			e1.printStackTrace(System.out);
			System.exit(-1);
		}
		// Initialize query logging for the send procedure
		cosar.startQueryLogging();


		String query = "SELECT * FROM resources WHERE walluserid = ? AND rownum <? ORDER BY rid desc";
		try {
			if((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k+1));
			//cosar.addQuery("SELECT * FROM resources WHERE walluserid = " + profileOwnerID + " AND rownum < " + (k+1) + " ORDER BY rid desc");
			cosar.addQuery("SELECT * FROM resources WHERE walluserid = " + profileOwnerID);
			
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "resources");
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if(col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					else if(col_name.equalsIgnoreCase("walluserid"))
						col_name = "walluserid";
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		if (retVal == SUCCESS){
			//serialize the result hashmap and insert it in the cache for future use
			byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
			try {
				cosar.sendToCache(key, payload, this.conn);
				CacheConnectionPool.returnConnection(cosar, key);
			} catch (Exception e1) {
				System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
				e1.printStackTrace(System.out);
				retVal = -2;
			}
		}

		return retVal;		
	}

	public int getCreatedResources(int resourceCreatorID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		Statement st = null;
		if(resourceCreatorID < 0)
			return -1;

		String query = "SELECT * FROM resources WHERE creatorid = "+resourceCreatorID;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if(col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}


	@Override
	public int viewCommentOnResource(int requesterID, int profileOwnerID,
			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
		if (verbose) System.out.print("Comments of "+resourceID+" ...");
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;


		String key = "ResCmts:"+resourceID;
		String query="";
		COSARInterface cosar=null;
		// Check Cache first
		try {
			cosar = CacheConnectionPool.getConnection(key);
			byte[] payload = cosar.retrieveBytesFromCache(key);
			if (payload != null){
				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
				//				for (int i = 0; i < result.size(); i++){
				//					HashMap<String, ByteIterator> myhashmap = result.elementAt(i);
				//					if (myhashmap.get("RID") != null)
				//						if (Integer.parseInt(myhashmap.get("RID").toString()) != resourceID)
				//							System.out.println("ERROR:  Expecting results for "+resourceID+" and got results for resource "+myhashmap.get("RID").toString());
				//						else i=result.size();
				//				}
				if (verbose) System.out.println("... Cache Hit!");
				CacheConnectionPool.returnConnection(cosar, key);
				return retVal;
			} else if (verbose) System.out.print("... Query DB!");
		} catch (COSARException e1) {
			e1.printStackTrace(System.out);
			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
			retVal = -2;
		}
		// Initialize query logging for the send procedure
		cosar.startQueryLogging();

		try {	
			query = "SELECT * FROM manipulation WHERE rid = ?";	
			if((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, resourceID);
			cosar.addQuery("SELECT * FROM manipulation WHERE rid = "+resourceID);
			rs = common.CacheUtilities.ExecuteQuery(conn, preparedStatement, lockStatement, "manipulation");
			while (rs.next()){
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				//get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
		try {
			cosar.sendToCache(key, payload, this.conn);
			CacheConnectionPool.returnConnection(cosar, key);
		} catch (Exception e1) {
			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
			e1.printStackTrace(System.out);
			retVal = -2;
		}

		return retVal;		
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> commentValues) {
		int retVal = SUCCESS;

		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;

		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?, ?, ?, ?, ?, ?, ?)";
		try {
			if((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, Integer.parseInt(commentValues.get("mid").toString()));
			preparedStatement.setInt(2, profileOwnerID);
			preparedStatement.setInt(3, resourceID);
			preparedStatement.setInt(4,commentCreatorID);
			preparedStatement.setString(5,commentValues.get("timestamp").toString());
			preparedStatement.setString(6,commentValues.get("type").toString());
			preparedStatement.setString(7,commentValues.get("content").toString());
		
			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "manipulation");

		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
					//preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		int retVal = SUCCESS;

		if(resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
			return -1;

		String query = "DELETE FROM manipulation WHERE mid=? AND rid=?";
		try {
			if((preparedStatement = newCachedStatements.get(DELCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(DELCMT_STMT, query);	
			preparedStatement.setInt(1, manipulationID);
			preparedStatement.setInt(2, resourceID);
			common.CacheUtilities.ExecuteUpdateStmt(conn, preparedStatement, lockStatement, "manipulation");
						
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}
	public HashMap<String, String> getInitialStats() {
		return common.RdbmsUtilities.getInitialStats(conn);
//		HashMap<String, String> stats = new HashMap<String, String>();
//		Statement st = null;
//		ResultSet rs = null;
//		String query = "";
//		try {
//			st = conn.createStatement();
//			//get user count
//			query = "SELECT count(*) from users";
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("usercount",rs.getString(1));
//			}else
//				stats.put("usercount","0"); //sth is wrong - schema is missing
//			if(rs != null ) rs.close();
//			//get user offset
//			query = "SELECT min(userid) from users";
//			rs = st.executeQuery(query);
//			String offset = "0";
//			if(rs.next()){
//				offset = rs.getString(1);
//			}
//			//get resources per user
//			query = "SELECT count(*) from resources where creatorid="+Integer.parseInt(offset);
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("resourcesperuser",rs.getString(1));
//			}else{
//				stats.put("resourcesperuser","0");
//			}
//			if(rs != null) rs.close();	
//			//get number of friends per user
//			query = "select count(*) from friendship where (inviterid="+Integer.parseInt(offset) +" OR inviteeid="+Integer.parseInt(offset) +") AND status=2" ;
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("avgfriendsperuser",rs.getString(1));
//			}else
//				stats.put("avgfriendsperuser","0");
//			if(rs != null) rs.close();
//			query = "select count(*) from friendship where (inviteeid="+Integer.parseInt(offset) +") AND status=1" ;
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("avgpendingperuser",rs.getString(1));
//			}else
//				stats.put("avgpendingperuser","0");
//			
//
//		}catch(SQLException sx){
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if(rs != null)
//					rs.close();
//				if(st != null)
//					st.close();
//			} catch (SQLException e) {
//				e.printStackTrace(System.out);
//			}
//
//		}
//		return stats;
	}

	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		int retVal	 = SUCCESS;
		
//		String key = "PendingFriendship:"+inviteeid;
//		COSARInterface cosar=null;
//		// Check Cache first
//		try {
//			cosar = CacheConnectionPool.getConnection(key);
//			byte[] payload = cosar.retrieveBytesFromCache(key);
//			if (payload != null){
//				if (!CacheUtilities.unMarshallVectorOfInts(payload, pendingIds))
//					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVector");
//				
//				if (verbose) System.out.println("... Cache Hit!");
//				CacheConnectionPool.returnConnection(cosar, key);
//				return retVal;
//			} else if (verbose) System.out.print("... Query DB!");
//		} catch (COSARException e1) {
//			e1.printStackTrace(System.out);
//			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
//			retVal = -2;
//		}
//		// Initialize query logging for the send procedure
//		cosar.startQueryLogging();
		
		try {
			st = conn.createStatement();
			query = "SELECT inviterid from friendship where inviteeid='"+inviteeid+"' and status='1'";
			//cosar.addQuery(query);
			rs = st.executeQuery(query);
			while(rs.next()){
				pendingIds.add(rs.getInt(1));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -2;
			}
		}
		
//		byte[] payload = CacheUtilities.SerializeVectorOfInts(pendingIds);
//		try {
//			cosar.sendToCache(key, payload, this.conn);
//			CacheConnectionPool.returnConnection(cosar, key);
//		} catch (Exception e1) {
//			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//			e1.printStackTrace(System.out);
//			retVal = -2;
//		}

		return retVal;
	}


	public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds){
		Statement st = null;
		ResultSet rs = null;
		String query = "";		
		int retVal	 = SUCCESS;
		
//		String key = "ConfirmedFriendship:"+profileId;
//		COSARInterface cosar=null;
//		// Check Cache first
//		try {
//			cosar = CacheConnectionPool.getConnection(key);
//			byte[] payload = cosar.retrieveBytesFromCache(key);
//			if (payload != null){
//				if (!CacheUtilities.unMarshallVectorOfInts(payload, confirmedIds))
//					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVector");
//				
//				if (verbose) System.out.println("... Cache Hit!");
//				CacheConnectionPool.returnConnection(cosar, key);
//				return retVal;
//			} else if (verbose) System.out.print("... Query DB!");
//		} catch (COSARException e1) {
//			e1.printStackTrace(System.out);
//			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
//			retVal = -2;
//		}
//		// Initialize query logging for the send procedure
//		cosar.startQueryLogging();
		
		try {
			st = conn.createStatement();
			query = "SELECT inviterid, inviteeid from friendship where (inviteeid="+profileId+" OR inviterid="+profileId+") and status='2'";
			//cosar.addQuery(query);
			rs = st.executeQuery(query);
			while(rs.next()){
				if(rs.getInt(1) != profileId)
					confirmedIds.add(rs.getInt(1));
				else
					confirmedIds.add(rs.getInt(2));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -2;
			}
		}

//		byte[] payload = CacheUtilities.SerializeVectorOfInts(confirmedIds);
//		try {
//			cosar.sendToCache(key, payload, this.conn);
//			CacheConnectionPool.returnConnection(cosar, key);
//		} catch (Exception e1) {
//			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//			e1.printStackTrace(System.out);
//			retVal = -2;
//		}

		return retVal;
	}



	public static void main(String[] args) {
		System.out.println("Hello World");
		int CACHE_NUM_PARTITIONS=1;
		//TODO: may need to set all DBMS and COSAR properties
		CacheConnectionPool.clearServerList();
		System.out.println("Add connection to "+CONNECTION_URL);
		for( int i = 0; i < CACHE_NUM_PARTITIONS; i++ )
		{
			CacheConnectionPool.addServer("127.0.0.1", 4343 + i);
		}


		try {
			CacheConnectionPool.init(CACHE_POOL_NUM_CONNECTIONS, true);
		} catch (COSARException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
		}
		SqlTrigSemiDataClient s = new SqlTrigSemiDataClient();
		HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
		s.viewProfile(1, 1, result, false, true);
		try {
			s.cleanup(false);
		} catch (Exception e){
			System.out.println("cleanup failed.");
			e.printStackTrace(System.out);
		}
	}

	@Override
	public int CreateFriendship(int memberA, int memberB) {
		int retVal = SUCCESS;
		if(memberA < 0 || memberB < 0)
			return -1;
		try {
			String DML = "INSERT INTO friendship values(?,?,2)";
			preparedStatement = conn.prepareStatement(DML);
			preparedStatement.setInt(1, memberA);
			preparedStatement.setInt(2, memberB);
			preparedStatement.executeUpdate();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

    @Override
	public void createSchema(Properties props){
		common.RdbmsUtilities.createSchema(props, conn);
	}
    
    @Override
	public void buildIndexes(Properties props){
		common.RdbmsUtilities.buildIndexes(props, conn);
	}

	public static void dropSequence(Statement st, String seqName) {
		try {
			st.executeUpdate("drop sequence " + seqName);
		} catch (SQLException e) {
		}
	}

	public static void dropIndex(Statement st, String idxName) {
		try {
			st.executeUpdate("drop index " + idxName);
		} catch (SQLException e) {
		}
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
		}
	}


}
