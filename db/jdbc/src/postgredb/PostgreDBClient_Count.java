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

package postgredb;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
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

public class PostgreDBClient_Count extends DB {
	
	/** The code to return when the call succeeds. **/
	public static final int SUCCESS = 0;
	/** The code to return when the call fails. **/
	public static final int ERROR   = -1;
	
	private static String FSimagePath = "";
	
	private boolean initialized = false;
	private boolean verbose     = false;
	private Properties props;
	private Connection conn;
	private PreparedStatement preparedStatement;
	/** Statement pool to cache the statements. **/
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
	
	private static final String DEFAULT_PROP = "";
	
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
	private static final int INVFRNDPLUS_STMT   = 19;
	private static final int REJREQMINUS_STMT   = 20;
	private static final int ACCREQPLUS_STMT    = 21;
	private static final int UNFRNDFRNDMINUS_STMT = 22;
	
	public static void main(String[] args) {
//		TestImage();
//		TestUnit();
//		PostgreDBClient_Count pdb = new PostgreDBClient_Count();
//		try {
//			pdb.init();
//			pdb.createSchema(null);
//		} catch (DBException e) {
//			e.printStackTrace();
//		}
	}
	
	/**
	 * Test stub for Postgresql database client.
	 */
	public static void TestUnit() {
		PostgreDBClient pgdb = new PostgreDBClient();
		try {
			pgdb.init();
			pgdb.buildIndexes(null);
		} catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Test stub for storing and retrieving images from file system.
	 */
	public static void TestImage() {
		String userid = "0001";
		byte[] image = new byte[100];
		boolean profileimg = true;
		PostgreDBClient_Count pg = new PostgreDBClient_Count();
		pg.StoreImageInFS(userid, image, profileimg);
		pg.GetImageFromFS(userid, profileimg);
	}
	
	private boolean StoreImageInFS(String userid, byte[] image, boolean profileimg) {
		boolean result = true;
		String text = "thumbnail";
		
		if (profileimg) text = "profile";
		String ImageFileName = FSimagePath + File.separator + "img" + userid + text;
		
		File tgt = new File(ImageFileName);
		if (tgt.exists()) {
			if (!tgt.delete()) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}
		
		// Write the file.
		try {
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		} catch (Exception ex) {
			System.out.println("Error in writing the file " + ImageFileName);
			ex.printStackTrace(System.out);
		}
		
		return result;
	}
	
	private byte[] GetImageFromFS(String userid, boolean profileimg) {
        int filelength = 0;
        String text = "thumbnail";
        byte[] imgpayload = null;
       
        if (profileimg) text = "profile";
       
        String ImageFileName = FSimagePath + File.separator + "img" + userid + text;
        int attempt = 100;
        while (attempt > 0) {
	         try {
	        	 FileInputStream fis = null;
	        	 DataInputStream dis = null;
	        	 File fsimage = new File(ImageFileName); 
	             filelength = (int) fsimage.length();
	             imgpayload = new byte[filelength];
	             fis = new FileInputStream(fsimage);
	             dis = new DataInputStream(fis);
	             int read = 0;
	             int numRead = 0;
	             while (read < filelength && (numRead = dis.read(imgpayload, read, filelength - read)) >= 0) {
	            	 read = read + numRead;
	             }
	             dis.close();
	             fis.close();
	             break;
	         } catch (IOException e) {
	             e.printStackTrace(System.out);
	        	 --attempt;
	         }
       }
        
       return imgpayload;
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
		String urls = props.getProperty(PostgreDBClientConstants.CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(PostgreDBClientConstants.CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(PostgreDBClientConstants.CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(PostgreDBClientConstants.DRIVER_CLASS, DEFAULT_PROP);
		FSimagePath   = props.getProperty(PostgreDBClientConstants.FS_PATH, DEFAULT_PROP);

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
	
	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
		if (stmt == null) return newStatement;
		else return stmt;
	}

	// Loading phase query will not be cached.
	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {
		if (null == entitySet || null == entityPK)
			return ERROR;
		
		ResultSet rs = null;
		try {
			StringBuilder query = new StringBuilder("");
			int numFields = values.size();
			
			// Store images into the file system.
			if (entitySet.equalsIgnoreCase("users") && insertImage && !FSimagePath.equals(""))
				numFields = numFields - 2;
			query.append("INSERT INTO " + entitySet + " VALUES (");
			for (int i = 0; i <= numFields; ++i) {
				if (i == numFields) {
					query.append("?)");
					break;
				} else
					query.append("?, ");
			}
			
			preparedStatement = conn.prepareStatement(query.toString());
			// The type of primary key is Integer.
			preparedStatement.setInt(1, Integer.parseInt(entityPK));
			int cnt = 2;
			for (Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				if (entry.getKey().equalsIgnoreCase("pic") || entry.getKey().equalsIgnoreCase("tpic"))
					continue;
				if(entry.getKey().equalsIgnoreCase("creatorid") || entry.getKey().equalsIgnoreCase("walluserid"))
					preparedStatement.setInt(cnt, Integer.parseInt(field));
				else
					preparedStatement.setString(cnt, field);
				cnt++;
				
				if (verbose) {
					System.out.println("" + entry.getKey().toString() + ":" + field);
				}
			}
			
			if (entitySet.equalsIgnoreCase("users") && insertImage) {
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				if (FSimagePath.equals(""))
					preparedStatement.setBinaryStream(numFields, is, profileImage.length);
				else
					StoreImageInFS(entityPK, profileImage, true);
				
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				
				if (FSimagePath.equals(""))
					preparedStatement.setBinaryStream(numFields + 1, is, thumbImage.length);
				else
					StoreImageInFS(entityPK, thumbImage, false);
			}
			preparedStatement.execute();
			
			// Update the resource count field for user.
			if (entitySet.equalsIgnoreCase("resources")) {
				String queryInc = "UPDATE users SET rcount = rcount + 1 WHERE userid = ?";
				preparedStatement = conn.prepareStatement(queryInc);
				preparedStatement.setInt(1, Integer.parseInt(values.get("walluserid").toString()));
				preparedStatement.execute();
			}
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return ERROR;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return ERROR;
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
		
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;

		String query = "";
		String uid   = "";
				
		// Profile details.
		try {
			if (insertImage && FSimagePath.equals("")) {
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic, fcount, pcount, rcount FROM users WHERE UserID = ?";
				if ((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);	
			} else {
				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address, email, tel, fcount, pcount, rcount FROM users WHERE UserID = ?";
				if ((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);	
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if (rs.next()) {
				result.put("friendcount", new ObjectByteIterator(rs.getBytes("fcount")));
				if (requesterID == profileOwnerID)
					result.put("pendingcount", new ObjectByteIterator(rs.getBytes("pcount")));
				result.put("resourcecount", new ObjectByteIterator(rs.getBytes("rcount")));

				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value = "";
					
					if (col_name.equalsIgnoreCase("userid")) {
						uid = rs.getString(col_name);
					}
					
					if (col_name.equalsIgnoreCase("pic") ) {
						// Get as bytes.
						byte[] bytes = rs.getBytes(i);
						value = bytes.toString();
						// If test mode dump pic into a file.
						if (testMode) {
							// Dump to file.
							try {
								FileOutputStream fos = new FileOutputStream(profileOwnerID + "-proimage.bmp");
								fos.write(bytes);
								fos.close();
							} catch (Exception ex) {
								System.out.println(ex.getMessage());
							}
						}
					} else
						value = rs.getString(col_name);
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				
				// Fetch the profile image from the file system.
				if (insertImage && !FSimagePath.equals("") ) {
					// Get the profile image from the file.
					byte[] profileImage = GetImageFromFS(uid, true);
					if (testMode) {
						// Dump to file.
						try {
							FileOutputStream fos = new FileOutputStream(profileOwnerID + "-proimage.bmp");
							fos.write(profileImage);
							fos.close();
						} catch (Exception ex){
						}
					}
					result.put("pic", new ObjectByteIterator(profileImage));
				}
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
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
			boolean insertImage, boolean testMode) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		
		if (requesterID < 0 || profileOwnerID < 0)
			return ERROR;

		String query = "";
		String uid   = "";
		try {
			if (insertImage && FSimagePath.equals("")) {
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
							byte[] bytes = rs.getBytes(i);
							value = bytes.toString();
							if (testMode){
								// Dump to file.
								try{
									FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
									fos.write(bytes);
									fos.close();
								}catch(Exception ex){
								}
							}
						} else {
							value = rs.getString(col_name);
							if (col_name.equalsIgnoreCase("userid")) {
								uid = value;
								col_name = "userid";
							}
						}
						values.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
					// Fetch the thumbnail image from the file system.
					if (insertImage && !FSimagePath.equals("")) {
						byte[] thumbImage = GetImageFromFS(uid, false);
						// Get the thumbnail image from the file.
						if (testMode) {
							// Dump to file.
							try {
								FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
								fos.write(thumbImage);
								fos.close();
							} catch(Exception ex) {
							}
						}
						values.put("tpic", new ObjectByteIterator(thumbImage));
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
		String uid   = "";
		try {
			if (insertImage && FSimagePath.equals("")) {
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
						byte[] bytes = rs.getBytes(i);
						value = bytes.toString();
						if (testMode) {
							// Dump to file.
							try {
								FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
								fos.write(bytes);
								fos.close();
							} catch(Exception ex) {
							}
						}
					} else {
						value = rs.getString(col_name);
						if (col_name.equalsIgnoreCase("userid")) {
							uid = value;
							col_name = "userid";	
						}
					}
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				// Fetch the thumbnail image from the file system.
				if (insertImage && !FSimagePath.equals("") ){
					byte[] thumbImage = GetImageFromFS(uid, false);
					// Get the thumbnail image from the file.
					if (testMode) {
						// Dump to file.
						try {
							FileOutputStream fos = new FileOutputStream(profileOwnerID + "-" + cnt + "-thumbimage.bmp");
							fos.write(thumbImage);
							fos.close();
						} catch(Exception ex) {
						}
					}
					values.put("tpic", new ObjectByteIterator(thumbImage));
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
		
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
		String query;
		query = "UPDATE friendship SET status = 2 WHERE inviterid = ? AND inviteeid = ?";
		try {
			if ((preparedStatement = newCachedStatements.get(ACCREQ_STMT)) == null){
				preparedStatement = createAndCacheStatement(ACCREQ_STMT, query);
			}
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
			query = "UPDATE users SET fcount = fcount + 1 WHERE userid = ? OR userid = ?";
			if ((preparedStatement = newCachedStatements.get(ACCREQPLUS_STMT)) == null)
				preparedStatement = createAndCacheStatement(ACCREQPLUS_STMT, query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
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
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;

		String query = "DELETE FROM friendship WHERE inviterid = ? AND inviteeid = ? AND status = 1";
		try {
			if ((preparedStatement = newCachedStatements.get(REJREQ_STMT)) == null)
				preparedStatement = createAndCacheStatement(REJREQ_STMT, query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
			query = "UPDATE users SET pcount = pcount - 1 WHERE userid = ?";
			if ((preparedStatement = newCachedStatements.get(REJREQMINUS_STMT)) == null)
				preparedStatement = createAndCacheStatement(REJREQMINUS_STMT, query);
			preparedStatement.setInt(1, inviteeID);
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
	public int inviteFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		
		if (inviterID < 0 || inviteeID < 0)
			return ERROR;
		String query = "INSERT INTO friendship VALUES (?, ?, 1)";
		
		try {
			if ((preparedStatement = newCachedStatements.get(INVFRND_STMT)) == null)
				preparedStatement = createAndCacheStatement(INVFRND_STMT, query);	
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
			query = "UPDATE users SET pcount = pcount + 1 WHERE userid = ?";
			if ((preparedStatement = newCachedStatements.get(INVFRNDPLUS_STMT)) == null)
				preparedStatement = createAndCacheStatement(INVFRNDPLUS_STMT, query);
			preparedStatement.setInt(1, inviteeID);
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
			preparedStatement.setInt(2, (k+1));
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

		String query = "INSERT INTO manipulation (mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?, ?, ?, ?, ?, ?)";
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
		
		if (friendid1 < 0 || friendid2 < 0)
			return ERROR;

		String query = "DELETE FROM friendship WHERE (inviterid = ? and inviteeid = ?) OR (inviterid = ? and inviteeid = ?) AND status = 2";
		try {
			if ((preparedStatement = newCachedStatements.get(UNFRNDFRND_STMT)) == null)
				preparedStatement = createAndCacheStatement(UNFRNDFRND_STMT, query);
			preparedStatement.setInt(1, friendid1);
			preparedStatement.setInt(2, friendid2);
			preparedStatement.setInt(3, friendid2);
			preparedStatement.setInt(4, friendid1);
			preparedStatement.executeUpdate();
			query = "UPDATE users SET fcount = fcount - 1 WHERE userid = ? OR userid = ?";
			if ((preparedStatement = newCachedStatements.get(UNFRNDFRNDMINUS_STMT)) == null)
				preparedStatement = createAndCacheStatement(UNFRNDFRNDMINUS_STMT, query);
			preparedStatement.setInt(1, friendid1);
			preparedStatement.setInt(2, friendid2);
			preparedStatement.executeUpdate();
		} catch(SQLException sx) {
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
			query = "UPDATE users SET fcount = fcount + 1 WHERE userid = ? OR userid = ?";
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
			
			dropIndex(stmt, "RESOURCES_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			
			dropTable(stmt, "FRIENDSHIP");
			dropTable(stmt, "MANIPULATION");
			dropTable(stmt, "RESOURCES");
			dropTable(stmt, "USERS");
			
			dropSequence(stmt, "MID_AUTO");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID INTEGER, INVITEEID INTEGER,"
					+ "STATUS INTEGER DEFAULT 1)");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "(MID INTEGER," + "CREATORID INTEGER, RID INTEGER,"
					+ "MODIFIERID INTEGER, TIMESTAMP VARCHAR(200),"
					+ "TYPE VARCHAR(200), CONTENT VARCHAR(200))");

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "(RID INTEGER,CREATORID INTEGER,"
					+ "WALLUSERID INTEGER, TYPE VARCHAR(200),"
					+ "BODY VARCHAR(200), DOC VARCHAR(200))");

			if (Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))) {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200), PIC BYTEA, TPIC BYTEA,"
						+ "FCOUNT INTEGER DEFAULT 0, PCOUNT INTEGER DEFAULT 0, RCOUNT INTEGER DEFAULT 0)");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200),"
						+ "FCOUNT INTEGER DEFAULT 0, PCOUNT INTEGER DEFAULT 0, RCOUNT INTEGER DEFAULT 0)");
			}
			
			/** Auto increment. */
			stmt.executeUpdate("CREATE SEQUENCE MID_AUTO");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER COLUMN MID SET DEFAULT NEXTVAL('MID_AUTO')");
			
			stmt.executeUpdate("ALTER TABLE USERS ALTER USERID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE USERS ADD PRIMARY KEY (USERID)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD PRIMARY KEY (MID)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER MID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER CREATORID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER RID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ALTER MODIFIERID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD PRIMARY KEY (INVITERID, INVITEEID)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ALTER INVITERID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ALTER INVITEEID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD PRIMARY KEY (RID)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ALTER RID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE RESOURCES ALTER CREATORID SET NOT NULL");
			stmt.executeUpdate("ALTER TABLE RESOURCES ALTER WALLUSERID SET NOT NULL");
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
			
			buildIndexes(null);
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

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("DROP TABLE " + tableName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void dropIndex(Statement st, String indexName) {
		try {
			st.executeUpdate("DROP INDEX " + indexName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}
	
	public static void dropSequence(Statement st, String sequenceName) {
		try{
			st.executeUpdate("DROP SEQUENCE " + sequenceName);
		} catch (SQLException e) {
			System.out.println(e.getMessage());
		}
	}

	@Override
	public int queryPendingFriendshipIds(int memberID,
			Vector<Integer> pendingIds) {
		return SUCCESS;
	}

	@Override
	public int queryConfirmedFriendshipIds(int memberID,
			Vector<Integer> confirmedIds) {
		return SUCCESS;
	}


}
