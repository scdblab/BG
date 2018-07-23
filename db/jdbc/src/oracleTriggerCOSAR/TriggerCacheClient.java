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

package oracleTriggerCOSAR;

import com.rays.cosar.COSARException;
import com.rays.cosar.COSARInterface;
import com.rays.cosar.CacheConnectionPool;

import common.CacheUtilities;
import common.RdbmsUtilities;
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
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.PropertyConfigurator;

public class TriggerCacheClient extends DB implements JdbcDBClientConstants {

	private static boolean ManageCOSAR = false;
	private static boolean verbose = false;
	private static String LOGGER = "ERROR, A1";
	private static boolean initialized = false;
	private boolean shutdown = false;
	private static final boolean USE_LISTENER_START_CACHE = true;
	
	private static final boolean LOCK_TABLE_EXPLICIT = true;	// Manually lock tables to disable effect of MVCC not working with Gumball
	
	private Properties props;
	private boolean isInsertImage;
	private static final String DEFAULT_PROP = "";
	private static final int COSAR_WAIT_TIME = 10000;
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	private PreparedStatement preparedStatement = null;
	private PreparedStatement lockStatement = null;
	private Connection conn;
	StartProcess st;
	
	private String cache_hostname = "";
	private Integer cache_port = -1;
	//String cache_cmd = "C:\\PSTools\\psexec \\\\"+COSARServer.cacheServerHostname+" -u shahram -p 2Shahram C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
	//String cache_cmd_stop = "java -jar C:\\BG\\ShutdownCOSAR.jar";
	
	//String cache_cmd = "C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
	public static final int CACHE_POOL_NUM_CONNECTIONS = 400;
	private static final int deserialize_buffer_size = 65536;
	private static AtomicInteger NumThreads = null;
	private static Semaphore crtcl = new Semaphore(1, true);
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
	private static boolean useTTL = false;
	private static int IMAGE_SIZE_GRAN = 1024;
	private int THUMB_IMAGE_SIZE = 2*1024;
	
	private static final int MAX_UPDATE_RETRIES = 100;
	private static final int UPDATE_RETRY_SLEEP_TIME = 10; // milliseconds that gets added after every failure
 
	
	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}

	private String getCacheCmd()
	{		
		return "C:\\PSTools\\psexec \\\\"+cache_hostname+
				" -u shahram -p 2Shahram " +
				"C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
	}

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

		String driver = "oracle.jdbc.driver.OracleDriver";
		
		cache_hostname = props.getProperty(CACHE_SERVER_HOST, CACHE_SERVER_HOST_DEFAULT);
		cache_port = Integer.parseInt(props.getProperty(CACHE_SERVER_PORT, CACHE_SERVER_PORT_DEFAULT));
		
		isInsertImage = Boolean.parseBoolean(
				props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
		
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
		
		com.rays.cosar.COSARInterface.OBJECT_CACHING_ENABLED = true;
		com.rays.cosar.COSARInterface.COMPRESS_QUERY_RESULTS = false;
		com.rays.cosar.COSARInterface.STRICT_CONSISTENCY_ENABLED = true; //Gumball requires this to be true
		com.rays.cosar.COSARInterface.TRIGGER_REGISTRATION_ENABLED = false;
		com.rays.sql.Connection.setQueryCachingEnabled(! com.rays.cosar.COSARInterface.OBJECT_CACHING_ENABLED);
		com.rays.sql.Statement.setTransparentCaching(false); //When true, it attempts to generate triggers and cache queries before using the developer provided re-writes.
		
		//com.rays.cosar.COSARInterface.setTTL(Integer.parseInt(props.getProperty("ttlvalue", "0")));
		useTTL = Integer.parseInt(props.getProperty("ttlvalue", "0")) != 0;

		if (ManageCOSAR) {
			if(USE_LISTENER_START_CACHE)
			{
				DataInputStream in = null;
				DataOutputStream out = null;
				
				try {
					int listener_port = Integer.parseInt(props.getProperty(LISTENER_PORT, LISTENER_PORT_DEFAULT));
					Socket conn = new Socket(cache_hostname, listener_port);
					in = new DataInputStream(conn.getInputStream());
					out = new DataOutputStream(conn.getOutputStream());
					
					out.writeBytes("start ");
					String command = "C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
					out.writeInt(command.length());
					out.writeBytes(command);
					out.flush();
					
					int response = in.readInt();
					if(response != 0)
					{
						System.out.println("Error starting process");
					}
					
					out.close();
					in.close();
					conn.close();
					
				} catch (UnknownHostException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("Starting COSAR: "+this.getCacheCmd());
				//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
				this.st = new StartProcess(this.getCacheCmd(), "cache_output.txt");
				this.st.start();
			}

			System.out.println("Wait for "+COSAR_WAIT_TIME/1000+" seconds to allow COSAR to startup.");
			try{
				Thread.sleep(COSAR_WAIT_TIME);
			}catch(Exception e)
			{
				e.printStackTrace(System.out);
			}
		}
		
		ConstructTriggers();

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
			CacheConnectionPool.addServer(this.cache_hostname, this.cache_port + i);
		}


		try {
			CacheConnectionPool.init(CACHE_POOL_NUM_CONNECTIONS, true);
		} catch (COSARException e) {
			e.printStackTrace(System.out);
			System.exit(-1);
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
			//System.out.println("************number of threads="+NumThreads);
			if (warmup) decrementNumThreads();
			if (verbose) System.out.println("Cleanup (before warmup-chk):  NumThreads="+NumThreads);
			if(!warmup){
				crtcl.acquire();
				decrementNumThreads();
				if (verbose) System.out.println("Cleanup (after warmup-chk):  NumThreads="+NumThreads);
				if (NumThreads.get() > 0){
					crtcl.release();
					//cleanupAllConnections();
					//System.out.println("Active clients; one of them will clean up the cache manager.");
					return;
				}

				COSARInterface cosar = CacheConnectionPool.getConnection(0);
				//cosar.printStats();
				cosar.resetStats();
				CacheConnectionPool.returnConnection(cosar, 0);

				CacheConnectionPool.shutdown();

				if (ManageCOSAR){
					COSARInterface cache_conn = new COSARInterface(this.cache_hostname, this.cache_port);			
					cache_conn.shutdownServer();
					System.out.println("Stopping COSAR: "+this.getCacheCmd());
					//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
					//this.st = new StartProcess(this.cache_cmd_stop, "cache_output.txt");
					//this.st.start();
					System.out.print("Waiting for COSAR to finish.");

					if( this.st != null )
						this.st.join();
					Thread.sleep(10000);
					System.out.println("..Done!");
				}
				cleanupAllConnections();
				shutdown = true;
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
		return RdbmsUtilities.insertEntity(entitySet, entityPK, values, insertImage, conn, "");
	}


	private void readObject( DataInputStream in, int num_bytes, byte[] byte_array ) throws IOException
	{
		int total_bytes_read = 0;
		int bytes_read = 0;

		while( total_bytes_read < num_bytes )
		{
			bytes_read = in.read(byte_array, total_bytes_read, num_bytes - total_bytes_read);
			total_bytes_read += bytes_read;
		}		
	}

	
	private static String getLockStatement(String tablename, boolean exclusiveMode )
	{			
		if(exclusiveMode)
		{
			return "LOCK TABLE " + tablename + " IN EXCLUSIVE MODE";
		}
		else
		{
			return "LOCK TABLE " + tablename + " IN ROW SHARE MODE";
		}
	}

	
	private void LockRow(String tablename, boolean exclusiveMode) throws SQLException
	{
		lockStatement.executeUpdate(getLockStatement(tablename, exclusiveMode));
	}
	
	private ResultSet ExecuteQuery(PreparedStatement preparedStatement, String tablename) 
			throws SQLException
	{
		String tablename_array[] = {tablename};
		return ExecuteQuery(preparedStatement, tablename_array);
	}
			
	private ResultSet ExecuteQuery(PreparedStatement preparedStatement, String[] tablenames) 
			throws SQLException
	{
		boolean prev_autocommit_val = conn.getAutoCommit();
		ResultSet rs = null;
		
		conn.setAutoCommit(false);
		try
		{
			if(LOCK_TABLE_EXPLICIT)
			{
				for(String tablename : tablenames)
				{
					LockRow(tablename, false);
				}
			}
			rs = preparedStatement.executeQuery();
			conn.commit();
		}
		catch(SQLException e)
		{
			conn.rollback();
			conn.setAutoCommit(prev_autocommit_val);
			throw e;
		}
		conn.setAutoCommit(prev_autocommit_val);
		return rs;
	}
	
	private boolean ExecuteUpdateStmt(PreparedStatement pstmt, String tablename) throws SQLException
	{
		boolean retVal = false;
		int num_retries = 0;
		int sleep_time = 1;
		Random rand = new Random();
		
		boolean prev_autocommit_val = conn.getAutoCommit();
		conn.setAutoCommit(false);
		while(retVal == false)
		{
			try {
				LockRow(tablename, true);
				pstmt.executeUpdate();				
				retVal = true;
				conn.commit();
				//System.out.println("Successfully updated");
			} catch (SQLException e) {
				conn.rollback();
				if(e.getMessage().indexOf("ORA-08177") >= 0 && num_retries < MAX_UPDATE_RETRIES)
				{					
					// Sleep for awhile to avoid constantly deadlocking with other transactions
					try {
						int temp = rand.nextInt(sleep_time);
						Thread.sleep(temp);
						//System.out.println("Sleep for: " + temp + " ms, " + num_retries + " time");
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					
					// Retry the execution
					num_retries++;
					sleep_time += UPDATE_RETRY_SLEEP_TIME;					
				}
				else
				{
					conn.setAutoCommit(prev_autocommit_val);
					throw e;
				}
			}
		}
		conn.setAutoCommit(prev_autocommit_val);
		return retVal;
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

		// Initialize query logging for the send procedure
		//cosar.startQueryLogging();
		
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
			

		try {
			//friend count
//			key = "fcount"+profileOwnerID;
//			// Check Cache first
//			cosar = CacheConnectionPool.getConnection(key);
//			payload = cosar.retrieveBytesFromCache(key);
//			if (payload != null){
//				if (verbose) System.out.println("... Cache Hit!");
//				CacheConnectionPool.returnConnection(cosar, key);
//				result.put("friendcount", new StringByteIterator((new String(payload)).toString()));
//			}
//			else {
				if (verbose) System.out.print("... Query DB!");

				query = "SELECT count(*) FROM  friendship WHERE (inviterID = ? OR inviteeID = ?) AND status = 2 ";
				//query = "SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ";
				if((preparedStatement = newCachedStatements.get(GETFRNDCNT_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDCNT_STMT, query);
				
				preparedStatement.setInt(1, profileOwnerID);
				preparedStatement.setInt(2, profileOwnerID);
				//cosar.addQuery("SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ");

				rs = ExecuteQuery(preparedStatement, "friendship");

				String Value="0";
				if (rs.next()){
					Value = rs.getString(1);
					result.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
					
					//serialize the result hashmap and insert it in the cache for future use
					SR.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes()));
				}
				else
				{
					result.put("friendcount", new ObjectByteIterator(Value.getBytes())) ;
					SR.put("friendcount", new ObjectByteIterator(Value.getBytes())) ;
				}
//				payload = Value.getBytes();
//				cosar.sendToCache(key, payload, this.conn);
//				CacheConnectionPool.returnConnection(cosar, key);

//			}

		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
//		}catch (COSARException e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
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
//				// Check Cache first
//				cosar = CacheConnectionPool.getConnection(key);
//				payload = cosar.retrieveBytesFromCache(key);
//				if (payload != null){
//					if (verbose) System.out.println("... Cache Hit for friendReqCnt "+(new String(payload)).toString());
//					CacheConnectionPool.returnConnection(cosar, key);
//					result.put("pendingcount", new StringByteIterator((new String(payload)).toString()));
//				}
//				else {
					query = "SELECT count(*) FROM  friendship WHERE inviteeID = ? AND status = 1 ";
					//preparedStatement = conn.prepareStatement(query);
					if((preparedStatement = newCachedStatements.get(GETPENDCNT_STMT)) == null)
						preparedStatement = createAndCacheStatement(GETPENDCNT_STMT, query);
					
					preparedStatement.setInt(1, profileOwnerID);
					//cosar.addQuery("SELECT count(*) FROM  friendship WHERE inviteeID = "+profileOwnerID+" AND status = 1 ");
					rs = ExecuteQuery(preparedStatement, "friendship");
					String Value = "0";
					if (rs.next()){
						Value = rs.getString(1);
						result.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
					}
					else
						result.put("pendingcount", new ObjectByteIterator("0".getBytes())) ;

					//serialize the result hashmap and insert it in the cache for future use
					SR.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//					payload = Value.getBytes();
//					cosar.sendToCache(key, payload, this.conn);
//					CacheConnectionPool.returnConnection(cosar, key);
//				}
			}catch(SQLException sx){
				retVal = -2;
				sx.printStackTrace(System.out);
//			}catch (COSARException e1) {
//				e1.printStackTrace(System.out);
//				System.exit(-1);
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

//		key = "profile"+profileOwnerID;
//		try {
//			cosar = CacheConnectionPool.getConnection(key);
//			payload = cosar.retrieveBytesFromCache(key);
//			if (payload != null && unMarshallHashMap(result, payload)){
//				if (verbose) System.out.println("... Cache Hit!");
//				CacheConnectionPool.returnConnection(cosar, key);
//				return retVal;
//			} else if (verbose) System.out.println("... Query DB.");
//		} catch (COSARException e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}

		
		//resource count
		query = "SELECT count(*) FROM  resources WHERE wallUserID = ?";

		try {
			//preparedStatement = conn.prepareStatement(query);
			if((preparedStatement = newCachedStatements.get(GETRESCNT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCNT_STMT, query);
			
			preparedStatement.setInt(1, profileOwnerID);
			//cosar.addQuery("SELECT count(*) FROM  resources WHERE wallUserID = "+profileOwnerID);
			rs = ExecuteQuery(preparedStatement, "resources");
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
			if(insertImage){
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = ?";

				if((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);
				
			}
			else{
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
				//cosar.addQuery("SELECT * FROM  users WHERE UserID = "+profileOwnerID);
				if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);
				
			}
			
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			////cosar.addQuery("SELECT * FROM users WHERE UserID = "+profileOwnerID);
			rs = ExecuteQuery(preparedStatement, "users");
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
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
						SR.put(col_name, new ObjectByteIterator(allBytesInBlob));
						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else{
						value = rs.getString(col_name);
						SR.put(col_name, new ObjectByteIterator(value.getBytes()));
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
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
			if(insertImage){
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
				//cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
				if((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);
				
			}else{
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
				//cosar.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
				if((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);
				
			}
			String tables_to_lock[] = {"friendship", "users"};
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, profileOwnerID);
			////cosar.addQuery("SELECT * FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
			rs = ExecuteQuery(preparedStatement, tables_to_lock);
			int cnt=0;
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
							value = allBytesInBlob.toString();
							if(testMode){
								//dump to file
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-cthumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								}catch(Exception ex){
								}
							}
							values.put(col_name, new ObjectByteIterator(allBytesInBlob));
						}else{
							value = rs.getString(col_name);
							if(col_name.equalsIgnoreCase("userid"))
								col_name = "userid";
							values.put(col_name, new ObjectByteIterator(value.getBytes()));
						}
						
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
			if(insertImage){
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				//cosar.addQuery("SELECT * FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
				if((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);
				
			}else{ 
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				//cosar.addQuery("SELECT * FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
				if((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);
				
			}
			String tables_to_lock[] = {"friendship", "users"};
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			////cosar.addQuery("SELECT * FROM users, friendship WHERE inviteeid= and status = 1 and inviterid = userid");
			rs = ExecuteQuery(preparedStatement, tables_to_lock);
			int cnt=0;
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
						values.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else{
						value = rs.getString(col_name);
						if(col_name.equalsIgnoreCase("userid"))
							col_name = "userid";
						values.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
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
			ExecuteUpdateStmt(preparedStatement, "friendship");
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
			ExecuteUpdateStmt(preparedStatement, "friendship");
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
			ExecuteUpdateStmt(preparedStatement, "friendship");
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

			ExecuteUpdateStmt(preparedStatement, "friendship");
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

		String query = "SELECT * FROM resources WHERE walluserid = ? AND rownum <? ORDER BY rid desc";
		try {
			if((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k+1));
			rs = ExecuteQuery(preparedStatement, "resources");
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

		try {	
			query = "SELECT * FROM manipulation WHERE rid = ?";	
			if((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);
		
			//preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, resourceID);
			//cosar.addQuery("SELECT * FROM manipulation WHERE rid = "+resourceID);
			rs = ExecuteQuery(preparedStatement, "manipulation");
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
		
			ExecuteUpdateStmt(preparedStatement, "manipulation");

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


	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		try {
			st = conn.createStatement();
			//get user count
			query = "SELECT count(*) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("usercount",rs.getString(1));
			}else
				stats.put("usercount","0"); //sth is wrong - schema is missing
			if(rs != null ) rs.close();
			//get user offset
			query = "SELECT min(userid) from users";
			rs = st.executeQuery(query);
			String offset = "0";
			if(rs.next()){
				offset = rs.getString(1);
			}
			//get resources per user
			query = "SELECT count(*) from resources where creatorid="+Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("resourcesperuser",rs.getString(1));
			}else{
				stats.put("resourcesperuser","0");
			}
			if(rs != null) rs.close();	
			//get number of friends per user
			query = "select count(*) from friendship where (inviterid="+Integer.parseInt(offset) +" OR inviteeid="+Integer.parseInt(offset) +") AND status=2" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgfriendsperuser",rs.getString(1));
			}else
				stats.put("avgfriendsperuser","0");
			if(rs != null) rs.close();
			query = "select count(*) from friendship where (inviteeid="+Integer.parseInt(offset) +") AND status=1" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgpendingperuser",rs.getString(1));
			}else
				stats.put("avgpendingperuser","0");
			

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
			}

		}
		return stats;
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
		TriggerCacheClient s = new TriggerCacheClient();
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


	public void createSchema(Properties props){

		Statement stmt = null;

		try {
			stmt = conn.createStatement();
			
			dropTrigger(stmt, "InviteFriend");
			dropTrigger(stmt, "AcceptFriendReq");
			dropTrigger(stmt, "RejectFriendRequest");
			dropTrigger(stmt, "PostComments");
			
			RdbmsUtilities.createSchema(props, conn);
			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
				}
		}

	}
	
	private void ConstructTriggers(){
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			
			if (isInsertImage) {
				//Deletes keys impacted by InviteFriend action when insertimage=true
				stmt.execute(" create or replace trigger InviteFriend "+
						"before insert on friendship "+
						"for each row "+
						"declare "+
						"   k3 CLOB := TO_CLOB('viewPendReq:'); "+
						"   k4 CLOB := TO_CLOB('ownprofile'); "+
						"	ret_val BINARY_INTEGER; "+	
						"   DELETEKEY CLOB; " +
						"begin "+
						"   k3 := CONCAT(k3, :NEW.inviteeid); "+
						"   k4 := CONCAT(k4, :NEW.inviteeid); "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end; ");
				
				//Deletes those keys impacted by Accept Friend Req
				stmt.execute("create or replace trigger AcceptFriendReq "+
						"before UPDATE on friendship "+
						"for each row "+
						"declare "+
						"   k1 CLOB := TO_CLOB('lsFrds:'); "+
						"   k2 CLOB := TO_CLOB('lsFrds:'); "+
						"   k3 CLOB := TO_CLOB('profile'); "+
						"   k4 CLOB := TO_CLOB('profile'); "+
						"   k5 CLOB := TO_CLOB('ownprofile'); "+
						"   k6 CLOB := TO_CLOB('ownprofile'); "+
						"   k7 CLOB := TO_CLOB('viewPendReq:'); "+
						"   DELETEKEY CLOB; "+
						"	ret_val BINARY_INTEGER; "+
						"begin "+
						"   k1 := CONCAT(k1, :NEW.inviterid); "+
						"   k2 := CONCAT(k2, :NEW.inviteeid); "+
						"   k3 := CONCAT(k3, :NEW.inviterid); "+
						"   k4 := CONCAT(k4, :NEW.inviteeid); "+
						"   k5 := CONCAT(k5, :NEW.inviterid); "+
						"   k6 := CONCAT(k6, :NEW.inviteeid); "+
						"   k7 := CONCAT(k7, :NEW.inviteeid); "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k1));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k5));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k6));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k7));  "+
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end;");
				
				
				//Deletes those keys impacted by two actions:  1)Reject Friend Request and  2) Thaw Friendship
				stmt.execute("create or replace trigger RejFrdReqThawFrd "+
						"before DELETE on friendship "+
						"for each row "+
						"declare "+
						"   k2 CLOB := TO_CLOB('ownprofile'); "+
						"   k3 CLOB := TO_CLOB('viewPendReq:'); "+
						"   k12 CLOB := TO_CLOB('lsFrds:'); "+
						"   k13 CLOB := TO_CLOB('lsFrds:'); "+
						"   k14 CLOB := TO_CLOB('profile'); "+
						"   k15 CLOB := TO_CLOB('profile'); "+
						"   k16 CLOB := TO_CLOB('ownprofile'); "+
						"   k17 CLOB := TO_CLOB('ownprofile'); "+
						"   DELETEKEY CLOB; "+
						"	ret_val BINARY_INTEGER; "+
						"begin "+
						"IF(:OLD.status = 1) THEN "+
						"   k2 := CONCAT(k2, :OLD.inviteeid); "+
						"   k3 := CONCAT(k3, :OLD.inviteeid); "+	
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						
						"ELSE "+						
						"   k12 := CONCAT(k12, :OLD.inviterid); "+
						"   k13 := CONCAT(k13, :OLD.inviteeid); "+						
						"   k14 := CONCAT(k14, :OLD.inviterid); "+
						"   k15 := CONCAT(k15, :OLD.inviteeid); "+						
						"   k16 := CONCAT(k16, :OLD.inviterid); "+
						"   k17 := CONCAT(k17, :OLD.inviteeid); "+					
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k12));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k13));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k14));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k15));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k16));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k17));  "+
						"END IF; "+
						
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end;");
						//*/
			} else {
				//Deletes keys impacted by InviteFriend action when insertimage=false
				stmt.execute("create or replace trigger InviteFriend "+
						"before insert on friendship "+
						"for each row "+
						"declare "+
						"   k3 CLOB := TO_CLOB('viewPendReqNoImage:'); "+
						"   k4 CLOB := TO_CLOB('ownprofileNoImage'); "+
						"   DELETEKEY CLOB; "+
						"	ret_val BINARY_INTEGER; "+
						"begin "+
						"   k3 := CONCAT(k3, :NEW.inviteeid); "+
						"   k4 := CONCAT(k4, :NEW.inviteeid); "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end;");
				
				//Deletes those keys impacted by Accept Friend Req
				stmt.execute("create or replace trigger AcceptFriendReq "+
						"before UPDATE on friendship "+
						"for each row "+
						"declare "+
						"   k1 CLOB := TO_CLOB('lsFrdsNoImage:'); "+
						"   k2 CLOB := TO_CLOB('lsFrdsNoImage:'); "+
						"   k3 CLOB := TO_CLOB('profileNoImage'); "+
						"   k4 CLOB := TO_CLOB('profileNoImage'); "+
						"   k5 CLOB := TO_CLOB('ownprofileNoImage'); "+
						"   k6 CLOB := TO_CLOB('ownprofileNoImage'); "+
						"   k7 CLOB := TO_CLOB('viewPendReqNoImage:'); "+
						"   DELETEKEY CLOB; "+
						"	ret_val BINARY_INTEGER; "+
						"begin "+
						"   k1 := CONCAT(k1, :NEW.inviterid); "+
						"   k2 := CONCAT(k2, :NEW.inviteeid); "+
						"   k3 := CONCAT(k3, :NEW.inviterid); "+
						"   k4 := CONCAT(k4, :NEW.inviteeid); "+
						"   k5 := CONCAT(k5, :NEW.inviterid); "+
						"   k6 := CONCAT(k6, :NEW.inviteeid); "+
						"   k7 := CONCAT(k7, :NEW.inviteeid); "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k1));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k2));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k4));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k5));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k6));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k7));  "+
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end;");
				
				//Deletes those keys impacted by two actions:  1)Reject Friend Request and  2) Thaw Friendship
				stmt.execute("create or replace trigger RejFrdReqThawFrd "+
						"before DELETE on friendship "+
						"for each row "+
						"declare "+
						"   k3 CLOB := TO_CLOB('viewPendReqNoImage:'); "+
						"   k12 CLOB := TO_CLOB('lsFrdsNoImage:'); "+
						"   k13 CLOB := TO_CLOB('lsFrdsNoImage:'); "+
						"   k14 CLOB := TO_CLOB('profileNoImage'); "+
						"   k15 CLOB := TO_CLOB('profileNoImage'); "+
						"   k16 CLOB := TO_CLOB('ownprofileNoImage'); "+
						"   k17 CLOB := TO_CLOB('ownprofileNoImage'); "+
						"   DELETEKEY CLOB; "+
						"	ret_val BINARY_INTEGER; "+
						"begin "+
						"IF(:OLD.status = 1) THEN "+
						"   k3 := CONCAT(k3, :OLD.inviteeid); "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k3));  "+
						
						"ELSE "+
						"   k12 := CONCAT(k12, :OLD.inviterid); "+
						"   k13 := CONCAT(k13, :OLD.inviteeid); "+
						"   k14 := CONCAT(k14, :OLD.inviterid); "+
						"   k15 := CONCAT(k15, :OLD.inviteeid); "+
						"   k16 := CONCAT(k16, :OLD.inviterid); "+
						"   k17 := CONCAT(k17, :OLD.inviteeid); "+				
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k12));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k13));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k14));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k15));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k16));  "+
						"   DELETEKEY := CONCAT(DELETEKEY, CONCAT(' ', k17));  "+
						"END IF; "+
						
						"   ret_val := COSARDeleteMultiCall('RAYS', DELETEKEY, 0); "+
						"end;");
			}
			
			//Deletes keys impacted by the Post Command Action of BG
			stmt.execute("create or replace trigger PostComments "+
					"before INSERT on manipulation "+
					"for each row "+
					"declare "+
					"   k1 CLOB := TO_CLOB('ResCmts:'); "+
					"	ret_val BINARY_INTEGER; "+
					"begin "+
					"   k1 := CONCAT(k1, :NEW.rid); "+
					"   ret_val := COSARDeleteMultiCall('RAYS', k1, 0); "+
					"end;");
			
			// TODO: add delete comment trigger
				
		} catch (Exception e) {
			e.printStackTrace(System.out);
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
	}
    @Override
	public void buildIndexes(Properties props){
    	RdbmsUtilities.buildIndexes(props, conn);

		//ConstructTriggers();
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
	
	public static void dropTrigger(Statement st, String triggerName) {
		try {
			st.executeUpdate("drop trigger " + triggerName);
		} catch (SQLException e) {
		}
	}


	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}


}

