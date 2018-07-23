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

package MySQL;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

public class MVMySQLClient extends DB implements MySQLConstants {
	
	/** The code to return when the call succeeds. **/
	public static final int SUCCESS = 0;
	/** The code to return when the call fails. **/
	public static final int ERROR   = -1;
	
	private static String FSimagePath = "";
	private boolean verbose = false;
	private Properties props;
	private Connection conn;
	private PreparedStatement preparedStatement;
	/** Statement pool to cache the statements. **/
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	
	
	
	private static final int GETFRNDCNT_STMT    = 2;
	private static final int GETPENDCNT_STMT    = 3;
	private static final int GETRESCNT_STMT     = 4;
	private static final int GETPROFILE_STMT    = 5;
	private static final int GETPROFILEIMG_STMT = 6;
	private static final int GETFRNDS_STMT      = 7;
	private static final int GETFRNDSIMG_STMT   = 8;
	private static final int GETPEND_STMT       = 9;
	private static final int GETPENDIMG_STMT    = 10;
	private static final int REJREQ_STMT        = 11;
	private static final int ACCREQ_STMT        = 12;
	private static final int INVFRND_STMT       = 13;
	private static final int UNFRNDFRND_STMT    = 14;
	private static final int GETTOPRES_STMT     = 15;
	private static final int GETRESCMT_STMT     = 16;
	private static final int POSTCMT_STMT       = 17;
	private static final int DELCMT_STMT        = 18;
	
		
	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 * 
	 */
	@Override
	public boolean init() throws DBException {
		props = getProperties();
		String urls = props.getProperty(MySQLConstants.CONNECTION_URL, DEFAULT_URL);
		String user = props.getProperty(MySQLConstants.CONNECTION_USER, DEFAULT_USER);
		String passwd = props.getProperty(MySQLConstants.CONNECTION_PASSWD, DEFAULT_PASSWD);
		String driver = props.getProperty(MySQLConstants.DRIVER_CLASS, DEFAULT_DRIVER);
		
		FSimagePath = props.getProperty(MySQLConstants.FS_PATH, "");
		
		try {
			Class.forName(driver);
			for (String url: urls.split(",")) {
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface, all
				// operations should auto commit.
				conn.setAutoCommit(true);
			}
			/** Statement pool initialization. */
			newCachedStatements = new ConcurrentHashMap<Integer, PreparedStatement>();
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBC driver: " + e);
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
		return true;
	}
	
	private void cleanupAllConnections() {
		try {
			// Close all cached prepare statements.
			Set<Integer> statementTypes = newCachedStatements.keySet();
			Iterator<Integer> it = statementTypes.iterator();
			while (it.hasNext()) {
				int stmtType = it.next();
				if (newCachedStatements.get(stmtType) != null) newCachedStatements.get(stmtType).close();
			}
			if (conn != null) conn.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}
	
	@Override
	public void cleanup(boolean warmup) {
		cleanupAllConnections();
	}
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		ResultSet rs =null;

		PreparedStatement preparedStatement = null;
		
		try {
			String query;
			int numFields = values.size();
			
			if(entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields-2;
			
			query = "INSERT INTO "+entitySet+" VALUES (";
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
				InputStream is = null;
				byte[] profileImage = null;
				int image_size = -1;
				profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				is = new ByteArrayInputStream(profileImage);
				image_size = profileImage.length;
				
				if ( FSimagePath.equals("") )
					preparedStatement.setBinaryStream(numFields, is, image_size);
				else
					MySQLCommons.StoreImageInFS(entityPK, profileImage, true, FSimagePath);
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				if (FSimagePath.equals(""))
					preparedStatement.setBinaryStream(numFields+1, is, thumbImage.length);
				else
					MySQLCommons.StoreImageInFS(entityPK, thumbImage, false, FSimagePath);
			}
			preparedStatement.executeUpdate();
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
		return SUCCESS;
	}
	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		String uid="";
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;
		String query="";
		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic, pendcount, confcount, rescount FROM ProfileMV WHERE UserID = ?";
				if((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);	
			}else{
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pendcount, confcount, rescount FROM  ProfileMV WHERE UserID = ?";
				if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);	
			}
			//preparedStatement = conn.prepareStatement(query);
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
					if(col_name.equalsIgnoreCase("pic") ){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						//if test mode dump pic into a file
						if(testMode){
							//dump to file
							try{
								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-proimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							}catch(Exception ex){
							}
						}
						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else if (col_name.equalsIgnoreCase("rescount")){
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
				if (insertImage && !FSimagePath.equals("") ){
					//Get the profile image from the file
					byte[] profileImage = MySQLCommons.GetImageFromFS(uid, true, FSimagePath);
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
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;

		String query = "";
		try {
			if(insertImage && FSimagePath.equals("")){
				//it views the list of friends irrespective of requestorID and retrieves profile info of each friend: Here thumbnail image is retrieved
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, tpic FROM users, friendship WHERE ((inviterid = ? AND userid = inviteeid) or (inviteeid = ? AND userid = inviterid)) AND status = 2";
				if ((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);		
			} else {
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, friendship WHERE ((inviterid = ? AND userid = inviteeid) or (inviteeid = ? AND userid = inviterid)) AND status = 2";
				if ((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);	
			}
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt = 0;
			String uid="";
			while (rs.next()) {
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				if (fields != null) {
					for (String field : fields) {
						String value = rs.getString(field);
						if (field.equalsIgnoreCase("userid"))
							field = "userid";
						values.put(field, new ObjectByteIterator(value.getBytes()));
					}
					result.add(values);
				} else {
					// Get the number of columns and their names.
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++) {
						String col_name = md.getColumnName(i);
						String value = "";
						if (col_name.equalsIgnoreCase("tpic")) {
							// Get as a bytes.
							Blob aBlob = rs.getBlob(col_name);
							byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
							if (testMode){
								// Dump to file.
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								}catch(Exception ex){
								}
							}
							values.put(col_name, new ObjectByteIterator(allBytesInBlob));
						} else {
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
						byte[] thumbImage = MySQLCommons.GetImageFromFS(uid, false, FSimagePath);
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
		} catch(SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}

	@Override
	public int viewFriendReq(int profileOwnerID,
			Vector<HashMap<String, ByteIterator>> results, boolean insertImage,
			boolean testMode) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		if (profileOwnerID < 0)
			return ERROR;

		String query = "";
		try {
			if(insertImage && FSimagePath.equals("")){
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, tpic FROM users, friendship WHERE inviteeid = ? AND status = 1 AND inviterid = userid";
				if ((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);				
			} else {
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM users, friendship WHERE inviteeid = ? AND status = 1 AND inviterid = userid";
				if ((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);		
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt = 0;
			String uid="";
			while (rs.next()) {
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// Get the number of columns and their names.
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = "";
					if (col_name.equalsIgnoreCase("tpic")) {
						// Get as a bytes.
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
						if (testMode) {
							// Dump to file.
							try {
								FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							} catch(Exception ex) {
							}
						}
						values.put(col_name, new ObjectByteIterator(allBytesInBlob));
					} else {
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
					byte[] thumbImage = MySQLCommons.GetImageFromFS(uid, false, FSimagePath);
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
		
				results.add(values);
			}
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}
	@Override
	public int acceptFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		PreparedStatement refreshStatement;
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
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
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		PreparedStatement refreshStatement;
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;

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
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		PreparedStatement refreshStatement;
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
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

	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}

	@Override
	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		
		if (profileOwnerID < 0 || requesterID < 0 || k < 0)
			return ERROR;

		String query = "SELECT * FROM resources WHERE walluserid = ? ORDER BY rid DESC LIMIT ?";
		try {
			if ((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);				
			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, (k));
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// Get the number of columns and their names.
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
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
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}
	@Override
	public int getCreatedResources(int creatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		Statement st = null;
		
		if (creatorID < 0)
			return ERROR;

		String query = "SELECT * FROM resources WHERE creatorid = " + creatorID;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// Get the number of columns and their names.
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if(col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				retVal = ERROR;
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
		
		if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return ERROR;
		
		String query = "";
		
		// Get comment count.
		try {	
			query = "SELECT * FROM manipulation WHERE rid = ?";	
			if ((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);		
			preparedStatement.setInt(1, resourceID);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// Get the number of columns and their names.
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		} catch(SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int postCommentOnResource(int commentCreatorID, int profileOwnerID,
			int resourceID, HashMap<String, ByteIterator> values) {
		int retVal = SUCCESS;

		if (profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return ERROR;

		String query = "INSERT INTO manipulation (mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?, ?, ?, ?, ?, ?, ?)";
		try {
			if ((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);
			preparedStatement.setInt(1, Integer.parseInt(values.get("mid").toString()));
			preparedStatement.setInt(2, profileOwnerID);
			preparedStatement.setInt(3, resourceID);
			preparedStatement.setInt(4, commentCreatorID);
			preparedStatement.setString(5, values.get("timestamp").toString());
			preparedStatement.setString(6, values.get("type").toString());
			preparedStatement.setString(7, values.get("content").toString());
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;		
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		int retVal = SUCCESS;
		
		if (resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
			return ERROR;
		
		String query = "DELETE FROM manipulation WHERE mid = ? AND rid = ?";
		try {
			if ((preparedStatement = newCachedStatements.get(DELCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(DELCMT_STMT, query);	
			preparedStatement.setInt(1, manipulationID);
			preparedStatement.setInt(2, resourceID);
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if(preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}

		return retVal;	
	}
	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;
		PreparedStatement refreshStatement;
		if (friendid1 < 0 || friendid2 < 0)
			return ERROR;

		CallableStatement proc = null;
		try {
            proc = conn.prepareCall("{ call THAWFRIENDSHIP(?, ?) }");
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
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		
		try {
			st = conn.createStatement();
			// Get user count.
			query = "SELECT count(*) FROM users";
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("usercount", rs.getString(1));
			} else
				stats.put("usercount", "0");
			if (rs != null ) rs.close();
			
			// Get user offset.
			query = "SELECT min(userid) FROM users";
			rs = st.executeQuery(query);
			String offset = "0";
			if (rs.next()) {
				offset = rs.getString(1);
			}
			
			// Get resources per user.
			query = "SELECT count(*) FROM resources WHERE creatorid = " + Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("resourcesperuser", rs.getString(1));
			} else {
				stats.put("resourcesperuser", "0");
			}
			if(rs != null) rs.close();	
			
			// Get number of friends per user.
			query = "SELECT count(*) FROM friendship WHERE (inviterid = " + Integer.parseInt(offset) + " OR inviteeid = " + Integer.parseInt(offset) +") AND status = 2";
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("avgfriendsperuser", rs.getString(1));
			} else
				stats.put("avgfriendsperuser", "0");
			if (rs != null) rs.close();
			query = "SELECT count(*) FROM friendship WHERE (inviteeid = " + Integer.parseInt(offset) + ") AND status = 1";
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("avgpendingperuser", rs.getString(1));
			} else
				stats.put("avgpendingperuser", "0");
		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
		} finally {
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

	@Override
	public int CreateFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;
		
		if (friendid1 < 0 || friendid2 < 0)
			return ERROR;
		
		try {
			String query = "INSERT INTO friendship VALUES (?, ?, 2)";
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, friendid1);
			preparedStatement.setInt(2, friendid2);
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			retVal = ERROR;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = ERROR;
				e.printStackTrace(System.out);
			}
		}
		
		return retVal;
	}


	@Override
	public void createSchema(Properties props) {
		Statement stmt = null;

		try {
			stmt = conn.createStatement();
		
			dropConstraint(stmt,"FRIENDSHIP_USERS_FK1","FRIENDSHIP");
			dropConstraint(stmt,"FRIENDSHIP_USERS_FK2","FRIENDSHIP");
			dropConstraint(stmt,"MANIPULATION_RESOURCES_FK1","MANIPULATION");
			dropConstraint(stmt,"MANIPULATION_USERS_FK1","MANIPULATION");
			dropConstraint(stmt,"MANIPULATION_USERS_FK2","MANIPULATION");
			dropConstraint(stmt,"RESOURCES_USERS_FK1","RESOURCES");
			dropConstraint(stmt,"RESOURCES_USERS_FK2","RESOURCES");
			
			dropIndex(stmt, "RESOURCES_CREATORID", "RESOURCES");
			dropIndex(stmt, "RESOURCES_WALLUSERID", "RESOURCES");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID", "FRIENDSHIP");
			dropIndex(stmt, "FRIENDSHIP_INVITERID", "FRIENDSHIP");
			dropIndex(stmt, "FRIENDSHIP_STATUS", "FRIENDSHIP");
			dropIndex(stmt, "MANIPULATION_RID", "MANIPULATION");
			dropIndex(stmt, "MANIPULATION_CREATORID", "MANIPULATION");
			dropIndex(stmt,  "ProfileMV_USERID", "profileMV");
			
			dropView(stmt, "pendcountView");
			dropView(stmt, "confcountView");
			dropView(stmt, "rescountView");
			dropView(stmt, "ViewProfileMV");
			
			dropTable(stmt, "profileMV");
			dropTable(stmt, "FRIENDSHIP");
			dropTable(stmt, "MANIPULATION");
			dropTable(stmt, "RESOURCES");
			dropTable(stmt, "USERS");
			
			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID INTEGER NOT NULL, INVITEEID INTEGER NOT NULL,"
					+ "STATUS INTEGER DEFAULT 1)");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(MID INTEGER NOT NULL," + "CREATORID INTEGER NOT NULL, RID INTEGER NOT NULL,"
					+ "MODIFIERID INTEGER NOT NULL, TIMESTAMP VARCHAR(200),"
					+ "TYPE VARCHAR(200), CONTENT VARCHAR(200))");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(RID INTEGER NOT NULL, CREATORID INTEGER NOT NULL,"
					+ "WALLUSERID INTEGER NOT NULL, TYPE VARCHAR(200),"
					+ "BODY VARCHAR(200), DOC VARCHAR(200))");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT)) && props.getProperty(FS_PATH, "").equals("")) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER NOT NULL, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200), PIC MEDIUMBLOB, TPIC BLOB)");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER NOT NULL, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200))");
			}
			
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID)");
			//stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID)");
			/* setting NOT NULL above*/ 
			//stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER MID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
					+ "REFERENCES RESOURCES (RID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE");
			
			
					
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

	private void dropConstraint(Statement stmt, String constraintName, String tableName) {
		MySQLCommons.dropConstraint(stmt, constraintName, tableName)	;	
	}
	
	private void dropIndex(Statement stmt, String indexName, String tableName) {
		MySQLCommons.dropIndex(stmt, indexName, tableName);
	}
	
	public static void dropTable(Statement st, String tableName) {
		MySQLCommons.dropTable(st, tableName);
	}
	
	@Override
	public int queryPendingFriendshipIds(int inviteeid, Vector<Integer> pendingIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(inviteeid < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid from friendship where inviteeid='"+inviteeid+"' and status='1'";
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

	@Override
	public int queryConfirmedFriendshipIds(int profileId, Vector<Integer> confirmedIds){
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if(profileId < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			query = "SELECT inviterid, inviteeid from friendship where (inviteeid="+profileId+" OR inviterid="+profileId+") and status='2'";
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
	public void buildIndexes(Properties props) {
		Statement stmt  = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();
						
			stmt.executeUpdate("CREATE INDEX RESOURCES_CREATORID ON RESOURCES (CREATORID)");
			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)");
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)");
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)");
			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)");
			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)");
			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)");
			
			//create MV
			stmt.executeUpdate("CREATE OR REPLACE VIEW pendcountView AS "+
					"SELECT userid, count(CASE WHEN status = 1 THEN 1 ELSE NULL END) AS pendcount FROM users  "+
					"LEFT OUTER JOIN friendship on (inviteeID = userid) "+
					"GROUP BY userid");

					stmt.executeUpdate("CREATE OR REPLACE VIEW confcountView AS "+
					"SELECT userid, count(CASE WHEN status = 2 THEN 1 ELSE NULL END) AS confcount FROM users  "+
					"LEFT OUTER JOIN friendship on (inviterid = userid OR inviteeID = userid) "+
					"GROUP BY userid");

					stmt.executeUpdate("CREATE OR REPLACE VIEW rescountView AS "+
					"SELECT userid, count(walluserid) AS rescount FROM users  "+
					"LEFT OUTER JOIN resources on walluserid = userid "+
					"GROUP BY userid");
					
					/*stmt.executeUpdate("CREATE OR REPLACE VIEW profileView AS "+
							"SELECT * FROM users u, pendcountView p, confcountView c, rescountView r "+
							"where u.userid = p.userid and u.userid=c.userid and u.userid=r.userid "+
							"GROUP BY userid");*/

					createMaterializedView(stmt, "CREATE VIEW ViewProfileMV "+
						"AS "+
						"SELECT u.*, p.pendcount, cv.confcount, r.rescount "+
						"FROM users u, pendcountview p, confcountview cv, rescountview r "+
						"WHERE u.userid = p.userid AND  "+
						"  u.userid = cv.userid AND "+
						"  u.userid = r.userid");
					
						
					stmt.executeUpdate("CREATE TABLE profileMV AS (SELECT * from ViewProfileMV)");
					
					stmt.executeUpdate("CREATE INDEX ProfileMV_USERID ON profileMV (USERID)");
					
					MySQLCommons.dropStoredProcedure(stmt, "rejectfriend");
					stmt.executeUpdate(			// rejectFriend
							   " CREATE  PROCEDURE `rejectfriend` (inviter INT, invitee INT) " +
								   " BEGIN " +
								   "START TRANSACTION;"+
								   		" DELETE FROM friendship WHERE inviterid = inviter AND inviteeid = invitee AND status=1; " +
								   		" UPDATE profileMV SET pendcount = pendcount-1 where userid = invitee; " +
								   		" COMMIT;"+
								   		" END"
								   		);
								   		
					MySQLCommons.dropStoredProcedure(stmt, "acceptfriend");
					   stmt.executeUpdate(	   		// AcceptFriend
						   		" CREATE PROCEDURE `acceptfriend` (inviter INT, invitee INT)" +
						   		" BEGIN " +
						   		"START TRANSACTION;"+
						   		"UPDATE friendship SET status = 2 WHERE inviterid = inviter AND inviteeid = invitee;"+
						   		" update profileMV SET pendcount = pendcount-1 where userid = invitee; " +
						   		" update profileMV SET confcount = confcount+1 where userid = invitee; " +
						   		" update profileMV SET confcount = confcount+1 where userid = inviter; " +
								"COMMIT;"+
								" END "
							);
					  MySQLCommons.dropStoredProcedure(stmt, "invitefriend");
					   stmt.executeUpdate(			// inviteFriend
								" CREATE PROCEDURE `invitefriend`(inviter INT, invitee INT)" +
								" BEGIN " +
								"START TRANSACTION;"+
								" INSERT INTO friendship VALUES (inviter, invitee,1);" +
								" UPDATE profileMV SET pendcount = pendcount+1 where userid = invitee; " +
								"COMMIT;"+
								" END "
								);
								
					   
					   MySQLCommons.dropStoredProcedure(stmt, "thawfriendship");
					   stmt.executeUpdate(	// thawFriend
								" CREATE PROCEDURE `thawfriendship`(inviter INT, invitee INT)" +
								" BEGIN " +
								"START TRANSACTION;"+
								" DELETE FROM friendship WHERE (inviterid = inviter and inviteeid = invitee) OR (inviterid = invitee and inviteeid = inviter) AND status=2;" +
								" update profileMV SET confcount = confcount-1 where userid = invitee; " +
						   		" update profileMV SET confcount = confcount-1 where userid = inviter; " +
								"COMMIT;"+
								" END  "
								);	
						
					
					System.out.println("Done building materialized view");

			
			
			
			stmt.executeUpdate("analyze table FRIENDSHIP");
	        stmt.executeUpdate("analyze table USERS");
	        stmt.executeUpdate("analyze table profilemv");
	        stmt.executeUpdate("analyze table RESOURCES");
            stmt.executeUpdate("analyze table MANIPULATION");
			
			long endIdx = System.currentTimeMillis();
			System.out.println("Time to build database index structures(ms):" + (endIdx - startIdx));
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
	

	public static void createMaterializedView(Statement st, String sql)
	{
		try
		{
			st.executeUpdate(sql);
		} catch(SQLException e)
		{
			System.out.println("Error, could not create materialized view");
		}
		
	}
	public static void dropView(Statement st, String mvName)
	{
		try {
			st.executeUpdate("drop view " + mvName);
		} catch (SQLException e) {
		}
	}

	
}
