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
package finalRelational;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;


/**
 * A class that wraps a JDBC compliant database to allow it to be interfaced with BG.
 * This class extends {@link DB} and implements the database interface used by BG client.
 *
 * <br> Each client will have its own instance of this class. This client is
 * not thread safe.
 *
 * <br> This interface expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key. Therefore,
 * only one index on the primary key is needed.
 *
 * <p> The following options must be passed when using this database client.
 *
 * <ul>
 * <li><b>db.driver</b> The JDBC driver class to use.</li>
 * <li><b>db.url</b> The Database connection URL.</li>
 * <li><b>db.user</b> User name for the connection.</li>
 * <li><b>db.passwd</b> Password for the connection.</li>
 * </ul>
 *
 * 
 *
 */


public class Boosted2Table1RecordOracleClient extends DB implements JdbcDBClientConstants {
	private static String FSimagePath = "";
	private boolean initialized = false;
	private Properties props;
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	private PreparedStatement preparedStatement;
	private boolean verbose = false;
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
	private static Semaphore imgctrl = new Semaphore(10, true);
	
	private boolean StoreImageInFS(String userid, byte[] image, boolean profileimg){
		boolean result = true;
		String ext = "thumbnail";
		
		if (profileimg) ext = "profile";
		
		String ImageFileName = FSimagePath+"\\img"+userid+ext;
		
		File tgt = new File(ImageFileName);
		if ( tgt.exists() ){
			if (! tgt.delete() ) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}

		//Write the file
		
		try{
			imgctrl.acquire();
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		}catch(Exception ex){
			System.out.println("Error in writing the file"+ImageFileName);
			ex.printStackTrace(System.out);
			imgctrl.release();
		}finally{
			imgctrl.release();
		}
		

		return result;
	}
	
	 private byte[] GetImageFromFS(String userid, boolean profileimg){
         int filelength = 0;
         String ext = "thumbnail";
         byte[] imgpayload = null;

        
         if (profileimg) ext = "profile";
        
         String ImageFileName = FSimagePath+"\\img"+userid+ext;
        int attempt = 100;
        while(attempt>0){
	         try {
	        	 imgctrl.acquire();
            	 FileInputStream fis = null;
	        	 DataInputStream dis = null;
	        	 File fsimage = new File(ImageFileName); 
	             filelength = (int) fsimage.length();
	             imgpayload = new byte[filelength];
	             fis = new FileInputStream(fsimage);
	             dis = new DataInputStream(fis);
	             int read = 0;
	             int numRead = 0;
	             while (read < filelength && (numRead=dis.read(imgpayload, read, filelength - read    ) ) >= 0) {
	                     read = read + numRead;
	             }
	             dis.close();
	             fis.close();
	             imgctrl.release();
	             break;
	         } catch (IOException e) {
	             e.printStackTrace(System.out);
	        	 attempt--;
	        	 imgctrl.release();
	         } catch (InterruptedException e) {
				e.printStackTrace();
			}finally{
	        	 imgctrl.release();
	         }
        }
        return imgpayload;
 }
	private void cleanupAllConnections() {
		try {
			//close all cached prepare statements
			Set<Integer> statementTypes = newCachedStatements.keySet();
			Iterator<Integer> it = statementTypes.iterator();
			while(it.hasNext()){
				int stmtType = it.next();
				if(newCachedStatements.get(stmtType) != null) newCachedStatements.get(stmtType).close();
			}
			if(conn != null) conn.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 * 
	 */
	@Override
	public boolean init() throws DBException {
		if (initialized) {
			System.out.println("Client connection already initialized.");
			return true;
		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);
		FSimagePath = props.getProperty(FS_PATH, DEFAULT_PROP);
		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url: urls.split(",")) {
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface, all
				// operations should auto commit.
				conn.setAutoCommit(true);
			}
			newCachedStatements = new ConcurrentHashMap<Integer, PreparedStatement>();
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBS driver: " + e);
			e.printStackTrace(System.out);
			return false;
		} catch (SQLException e) {
			System.out.println("Error in database operation: " + e);
			e.printStackTrace(System.out);
			return false;
		} catch (NumberFormatException e) {
			System.out.println("Invalid value for fieldcount property. " + e);
			e.printStackTrace(System.out);
			return false;
		}

		initialized = true;
		return true;
	}

	@Override
	public void cleanup(boolean warmup) {
		cleanupAllConnections();
	}

	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}
	
	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;
		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call ACCEPTFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
		    proc.setInt(2, inviteeID);
		    proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;		
	}

	
	@Override
	//Load phase query not cached
	public int insertEntity(String entitySet, String entityPK, HashMap<String, ByteIterator> values, boolean insertImage) {
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		ResultSet rs =null;

		try {
			String query;
			int numFields = values.size();
			
			// Remove the pic field and put it in the filesystem
			if(entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields-1;
			
			query = "INSERT INTO "+entitySet+" VALUES (";
			//adding the three new attributes
			if(entitySet.equalsIgnoreCase("users"))
				numFields +=3;
			for(int j=0; j<=numFields; j++){
				if(j==(numFields)){
					query+="?)";
					break;
				}else
					query+="?,";
			}

			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			int cnt=2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				//blobs need to be inserted differently
				if(entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic") )
					continue;
				
				String field = entry.getValue().toString();
				preparedStatement.setString(cnt, field);
				cnt++;
				if (verbose) System.out.println(""+entry.getKey().toString()+":"+field);
			}

			if(entitySet.equalsIgnoreCase("users") && insertImage){
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				if ( FSimagePath.equals("") ){
					preparedStatement.setBinaryStream(cnt, is, profileImage.length);
					cnt++;
				}
				else
					StoreImageInFS(entityPK, profileImage, true);
				
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				
				preparedStatement.setBinaryStream(cnt, is, thumbImage.length);
				cnt++;
				
			}
			if(entitySet.equalsIgnoreCase("users")){
				//pendCount, confCount,resCount
				preparedStatement.setInt(cnt, 0);
				cnt++;
				preparedStatement.setInt(cnt, 0);
				cnt++;
				preparedStatement.setInt(cnt, 0);
				cnt++;
			}else{
				//increament the resource count for the walluserid
				String updateQ = "Update users set rescount=rescount+1 where userid="+values.get("walluserid");
				Statement st = conn.createStatement();
				st.executeUpdate(updateQ);
				st.close();
			}
			rs = preparedStatement.executeQuery();
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return -2;
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {

		ResultSet rs = null;
		int retVal = SUCCESS;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query="";
		String uid="";
		
		try {
			query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pendcount, confcount, rescount FROM  users WHERE UserID = ?";
			if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);	
			
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if(rs.next()){
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value ="";
					if (col_name.equalsIgnoreCase("userid")){
						uid = rs.getString(col_name);
					}
					if (col_name.equalsIgnoreCase("rescount")){
						result.put("resourcecount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else if(col_name.equalsIgnoreCase("pendcount")){
						if(profileOwnerID == requesterID)
							result.put("pendingcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else if(col_name.equalsIgnoreCase("confcount")){
						result.put("friendcount", new ObjectByteIterator(rs.getString(col_name).getBytes())) ;
					}else{
						value = rs.getString(col_name);
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
				}
				
				//Fetch the profile image from the file system
				if (insertImage){
					//Get the profile image from the file
					byte[] profileImage = GetImageFromFS(uid, true);
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
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}


	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		String uid = "0";
		ResultSet rs = null;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query ="";
			
		try {
			if(insertImage){
				query = "SELECT userid,friend1, friend2, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, confirmedfriendship WHERE ((friend1=? and userid=friend2) or (friend2=? and userid=friend1)) ";
				if((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);	
				
			}else{
				query = "SELECT userid,friend1, friend2, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, confirmedfriendship WHERE ((friend1=? and userid=friend2) or (friend2=? and userid=friend1)) ";
				if((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);	
			}
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt =0;
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
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++){
						String col_name = md.getColumnName(i);
						String value="";
						if(col_name.equalsIgnoreCase("tpic")){
							//Get as a BLOB
							Blob aBlob = rs.getBlob(col_name);
							byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
							if(testMode){
								//dump to file
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								}catch(Exception ex){
								}
							}
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
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0)
			return -1;

		String query = "";
		String uid = "0";
		try {
			if(insertImage){
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, pendingfriendship WHERE inviteeid=? and inviterid = userid";
				if((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);	
				
			}else {
				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, pendingfriendship WHERE inviteeid=? and inviterid = userid";
				if((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);		
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
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
					if(col_name.equalsIgnoreCase("tpic")){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-thumbimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}

						}
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

		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call REJECTFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
		    proc.setInt(2, inviteeID);
		    proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;	
	}

	@Override
	//Load phase query not cached
	public int CreateFriendship(int memberA, int memberB) {
		int retVal = SUCCESS;
		if(memberA < 0 || memberB < 0)
			return -1;
		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call INSERTFRIEND(?, ?) }");
			proc.setInt(1, memberA);
		    proc.setInt(2, memberB);
		    proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
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
		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call INVITEFRIEND(?, ?) }");
			proc.setInt(1, inviterID);
		    proc.setInt(2, inviteeID);
		    proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
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

		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call THAWFRIEND(?, ?) }");
			proc.setInt(1, friendid1);
		    proc.setInt(2, friendid2);
		    proc.execute();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace(System.out);
		}finally{
			try {
				if(proc != null)
					proc.close();
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

		String query = "SELECT * FROM resources WHERE walluserid = ? AND rownum <? ORDER BY rid desc";
		try {
			if((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);		
		
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k+1));
			rs = preparedStatement.executeQuery();
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
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
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
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;
		String query;
		//get comment cnt
		try {	
			query = "SELECT * FROM manipulation WHERE rid = ?";		
			if((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);		
			preparedStatement.setInt(1, resourceID);
			rs = preparedStatement.executeQuery();
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
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String,ByteIterator> commentValues) {
		int retVal = SUCCESS;

		if(profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;

		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?,?,?, ?,?, ?, ?)";
		try {
			if((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);	
			preparedStatement.setInt(1, Integer.parseInt(commentValues.get("mid").toString()));
			preparedStatement.setInt(2, profileOwnerID);
			preparedStatement.setInt(3, resourceID);
			preparedStatement.setInt(4,commentCreatorID);
			preparedStatement.setString(5,commentValues.get("timestamp").toString());
			preparedStatement.setString(6,commentValues.get("type").toString());
			preparedStatement.setString(7,commentValues.get("content").toString());
			preparedStatement.executeUpdate();
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
			preparedStatement.executeUpdate();
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

	@Override
	//init phase query not cached
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
			
			//get resources per user
			query = "SELECT avg(rescount) from users";
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("resourcesperuser",rs.getString(1));
			}else{
				stats.put("resourcesperuser","0");
			}
			if(rs != null) rs.close();	
			//get number of friends per user
			query = "select avg(confcount) from users" ;
			rs = st.executeQuery(query);
			if(rs.next()){
				stats.put("avgfriendsperuser",rs.getString(1));
			}else
				stats.put("avgfriendsperuser","0");
			if(rs != null) rs.close();
			query = "select avg(pendcount) from users" ;
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

	//init phase query not cached
	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(inviteeid < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid from pendingfriendship where inviteeid='"+inviteeid+"'";
			rs = st.executeQuery(query);
			while(rs.next()){
				pendingIds.add(rs.getInt(1));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;
	}

	//init phase query not cached
	public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(profileId < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid, inviteeid from confirmedfriendship where (friend1="+profileId+" OR friend2="+profileId+")";
			rs = st.executeQuery(query);
			while(rs.next()){
				if(rs.getInt(1) != profileId)
					confirmedIds.add(rs.getInt(1));
				else
					confirmedIds.add(rs.getInt(2));
			}	
		}catch(SQLException sx){
			sx.printStackTrace(System.out);
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;

	}

	@Override
	public void createSchema(Properties props){
		Statement stmt = null;

		try {
			stmt = conn.createStatement();

			dropSequence(stmt, "MIDINC");
			dropSequence(stmt, "RIDINC");
			dropSequence(stmt, "USERIDINC");
			dropSequence(stmt, "USERIDS");

			dropTable(stmt, "friendship");
			dropTable(stmt, "pendingfriendship");
			dropTable(stmt, "confirmedfriendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE CONFIRMEDFRIENDSHIP"
					+ "(FRIEND1 NUMBER, FRIEND2 NUMBER"
					+  ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE PENDINGFRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER"
					+  ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(	MID NUMBER," + "CREATORID NUMBER, RID NUMBER,"
					+ "MODIFIERID NUMBER, TIMESTAMP VARCHAR2(200),"
					+ "TYPE VARCHAR2(200), CONTENT VARCHAR2(200)"
					+ ") NOLOGGING");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(	RID NUMBER,CREATORID NUMBER,"
					+ "WALLUSERID NUMBER, TYPE VARCHAR2(200),"
					+ "BODY VARCHAR2(200), DOC VARCHAR2(200)"
					+ ") NOLOGGING");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), TPIC BLOB,"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") NOLOGGING");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200),"
						+ "PENDCOUNT NUMBER, CONFCOUNT NUMBER, RESCOUNT NUMBER"
						+ ") NOLOGGING");

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE CONFIRMEDFRIENDSHIP ADD CONSTRAINT CONFIRMEDFRIENDSHIP_PK PRIMARY KEY (FRIEND1, FRIEND2) ENABLE");
			stmt.executeUpdate("ALTER TABLE CONFIRMEDFRIENDSHIP MODIFY (FRIEND1 NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE CONFIRMEDFRIENDSHIP MODIFY (FRIEND2 NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE PENDINGFRIENDSHIP ADD CONSTRAINT PENDINGFRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE PENDINGFRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE PENDINGFRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			
			stmt.executeUpdate("ALTER TABLE CONFIRMEDFRIENDSHIP ADD CONSTRAINT CONFIRMEDFRIENDSHIP_USERS_FK1 FOREIGN KEY (FRIEND1)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE CONFIRMEDFRIENDSHIP ADD CONSTRAINT CONFIRMEDFRIENDSHIP_USERS_FK2 FOREIGN KEY (FRIEND2)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE PENDINGFRIENDSHIP ADD CONSTRAINT PENDINGFRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE PENDINGFRIENDSHIP ADD CONSTRAINT PENDINGFRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("CREATE OR REPLACE TRIGGER MINC before insert on manipulation "
					+ "for each row "
					+ "WHEN (new.mid is null) begin "
					+ "select midInc.nextval into :new.mid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER MINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER RINC before insert on resources "
					+ "for each row "
					+ "WHEN (new.rid is null) begin "
					+ "select ridInc.nextval into :new.rid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");

			stmt.executeUpdate("CREATE OR REPLACE TRIGGER UINC before insert on users "
					+ "for each row "
					+ "WHEN (new.userid is null) begin "
					+ "select useridInc.nextval into :new.userid from dual;"
					+ "end;");
			stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			
			
			
			//drop stored procedure
			dropStoredProcedure(stmt, "INVITEFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INVITEFRIEND " +
							"("+ 
						         " P_INVITERID   IN PENDINGFRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN PENDINGFRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						      "INSERT INTO PENDINGFRIENDSHIP "+
						             "("+ 
						               "INVITERID     , "+
						               "INVITEEID     "+
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     "+
						             ");"+ 
						      "UPDATE USERS SET "+
								  		"PENDCOUNT=PENDCOUNT+1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END INVITEFRIEND;"
					);

			
			//drop stored procedure
			dropStoredProcedure(stmt, "ACCEPTFRIEND");
			//create stored procedure for accepting friends in the 2 record case
			stmt.execute("CREATE OR REPLACE PROCEDURE ACCEPTFRIEND " +
							"("+ 
						         " P_INVITERID   IN PENDINGFRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN PENDINGFRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM PENDINGFRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
						      "INSERT INTO CONFIRMEDFRIENDSHIP "+
						             "("+ 
						               "FRIEND1     , "+
						               "FRIEND2      "+ 
						              ")"+ 
						     " VALUES "+
						             "("+ 
						               "P_INVITERID     , "+
						               "P_INVITEEID     "+                             
						             ");"+ 
							"UPDATE USERS SET "+
								  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
							"UPDATE USERS SET "+
									  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END ACCEPTFRIEND;"
					);
			
			//drop stored procedure
			dropStoredProcedure(stmt, "REJECTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE REJECTFRIEND " +
							"("+ 
						         " P_INVITERID   IN PENDINGFRIENDSHIP.INVITERID%TYPE    , "+
						         " p_INVITEEID     IN PENDINGFRIENDSHIP.INVITEEID%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM PENDINGFRIENDSHIP "+
							  		"WHERE INVITERID = P_INVITERID AND INVITEEID = P_INVITEEID ;"+
							  "UPDATE USERS SET "+
							  		"PENDCOUNT=PENDCOUNT-1 WHERE USERID = P_INVITEEID;"+
						      "COMMIT;"+
						"END REJECTFRIEND;"
					);
			
			dropStoredProcedure(stmt, "THAWFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE THAWFRIEND " +
							"("+ 
						         " P_INVITERID   IN CONFIRMEDFRIENDSHIP.FRIEND1%TYPE    , "+
						         " p_INVITEEID     IN CONFIRMEDFRIENDSHIP.FRIEND2%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
							  "DELETE FROM CONFIRMEDFRIENDSHIP "+
							  		"WHERE (FRIEND1 = P_INVITERID AND FRIEND2 = P_INVITEEID) OR (FRIEND1 = P_INVITEEID AND FRIEND2 = P_INVITERID);"+
							  "UPDATE USERS SET "+
							  		"CONFCOUNT=CONFCOUNT-1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+ 
						      "COMMIT;"+
						"END THAWFRIEND;"
					);
			
			dropStoredProcedure(stmt, "INSERTFRIEND");
			stmt.execute("CREATE OR REPLACE PROCEDURE INSERTFRIEND " +
							"("+ 
						         " P_INVITERID   IN CONFIRMEDFRIENDSHIP.FRIEND1%TYPE    , "+
						         " p_INVITEEID     IN CONFIRMEDFRIENDSHIP.FRIEND2%TYPE  "+
						     ")" +
						"AS "+ 
						"BEGIN "+ 
						 "INSERT INTO CONFIRMEDFRIENDSHIP "+
			             "("+ 
			               "FRIEND1     , "+
			               "FRIEND2     "+
			              ")"+ 
			     " VALUES "+
			             "("+ 
			               "P_INVITERID     , "+
			               "P_INVITEEID     "+
			             ");"+ 
			      "UPDATE USERS SET "+
					  		"CONFCOUNT=CONFCOUNT+1 WHERE USERID = P_INVITEEID OR USERID=P_INVITERID;"+
						      "COMMIT;"+
						"END INSERTFRIEND;"
					);
			
			
		
			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "CONFIRMEDFRIENDSHIP_FRIEND1");
			dropIndex(stmt, "CONFIRMEDFRIENDSHIP_FRIEND2");
			dropIndex(stmt, "PENDINGFRIENDSHIP_INVITEEID");
			dropIndex(stmt, "PENDINGFRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
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
	
    @Override
	public void buildIndexes(Properties props){
    	Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "CONFIRMEDFRIENDSHIP_FRIEND1");
			dropIndex(stmt, "CONFIRMEDFRIENDSHIP_FRIEND2");
			dropIndex(stmt, "PENDINGFRIENDSHIP_INVITEEID");
			dropIndex(stmt, "PENDINGFRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX CONFIRMEDFRIENDSHIP_FRIEND1 ON CONFIRMEDFRIENDSHIP (FRIEND1)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX PENDINGFRIENDSHIP_INVITEEID ON PENDINGFRIENDSHIP (INVITEEID)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX CONFIRMEDFRIENDSHIP_FRIEND2 ON CONFIRMEDFRIENDSHIP (FRIEND2)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("CREATE INDEX PENDINGFRIENDSHIP_INVITERID ON PENDINGFRIENDSHIP (INVITERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");
			
			stmt.executeUpdate("analyze table users compute statistics");
			stmt.executeUpdate("analyze table resources compute statistics");
			stmt.executeUpdate("analyze table confirmedfriendship compute statistics");
			stmt.executeUpdate("analyze table pendingfriendship compute statistics");
			stmt.executeUpdate("analyze table manipulation compute statistics");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
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
	
	public static void dropStoredProcedure(Statement st, String procName) {
		try {
			st.executeUpdate("drop procedure " + procName);
		} catch (SQLException e) {
		}
	}
}
