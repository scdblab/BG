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
package KosarSolo;

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


//author sumita barahmand

public class KosarSoloClient extends DB implements JdbcDBClientConstants {

	private ArrayList<Connection> conns;
	private boolean initialized = false;
	private static Properties props;
	private static final String DEFAULT_PROP = "";
	private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
	private PreparedStatement preparedStatement;
	private Connection conn;
	private Statement st = null;  
	private static int IMAGE_SIZE_GRAN = 1024;
	private int THUMB_IMAGE_SIZE = 2*1024;


	/**
	 * The statement type for the prepared statements.
	 */
	private static class StatementType {

		enum Type {
			INSERT(1),
			DELETE(2),
			READ(3),
			UPDATE(4),
			SCAN(5),
			;
			int internalType;
			private Type(int type) {
				internalType = type;
			}

			int getHashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + internalType;
				return result;
			}
		}

		Type type;
		int shardIndex;
		int numFields;
		String tableName;

		StatementType(Type type, String tableName, int numFields, int _shardIndex) {
			this.type = type;
			this.tableName = tableName;
			this.numFields = numFields;
			this.shardIndex = _shardIndex;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + numFields + 100 * shardIndex;
			result = prime * result
					+ ((tableName == null) ? 0 : tableName.hashCode());
			result = prime * result + ((type == null) ? 0 : type.getHashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StatementType other = (StatementType) obj;
			if (numFields != other.numFields)
				return false;
			if (shardIndex != other.shardIndex)
				return false;
			if (tableName == null) {
				if (other.tableName != null)
					return false;
			} else if (!tableName.equals(other.tableName))
				return false;
			if (type != other.type)
				return false;
			return true;
		}
	}

	/**
	 * For the given key, returns what shard contains data for this key
	 *
	 * @param key Data key to do operation on
	 * @return Shard index
	 */
	private int getShardIndexByKey(String key) {
		int ret = Math.abs(key.hashCode()) % conns.size();
		//System.out.println(conns.size() + ": Shard instance for "+ key + " (hash " + key.hashCode()+ " ) " + " is " + ret);
		return ret;
	}

	/**
	 * For the given key, returns Connection object that holds connection
	 * to the shard that contains this key
	 *
	 * @param key Data key to get information for
	 * @return Connection object
	 */
	private Connection getShardConnectionByKey(String key) {
		return conns.get(getShardIndexByKey(key));
	}

	private void cleanupAllConnections() {
		try {
			if (st != null) st.close();
			for(Connection con: conns) {
				if(con != null) con.close();
			}
			if(conn != null) conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void init(Properties props) throws DBException {
		if (initialized) {
			System.out.println("Client connection already initialized.");
			return;
		}

		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);

		try {
			if (driver != null) {
				Class.forName(driver);
			}
			int shardCount = 0;
			conns = new ArrayList<Connection>(3);
			for (String url: urls.split(",")) {
				//System.out.println("Adding shard node URL: " + url);
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface, all
				// operations should auto commit.
				conn.setAutoCommit(true);
				shardCount++;
				conns.add(conn);  //TODO: what is this?
			}

			//System.out.println("Using " + shardCount + " shards");

			cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
		} catch (ClassNotFoundException e) {
			System.out.println("Error in initializing the JDBS driver: " + e);
			throw new DBException(e);
		} catch (SQLException e) {
			System.out.println("Error in database operation: " + e);
			throw new DBException(e);
		} catch (NumberFormatException e) {
			System.out.println("Invalid value for fieldcount property. " + e);
			throw new DBException(e);
		}
		initialized = true;
	}

	/**
	 * Initialize the database connection and set it up for sending requests to the database.
	 * This must be called once per client.
	 * @throws DBException
	 */
	@Override
	public boolean init() throws DBException {
		System.out.println("--->Initialize");
		if (initialized) {
			System.out.println("Client connection already initialized.");
			return true;
		}
		props = getProperties();
		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
		String driver = props.getProperty(DRIVER_CLASS);

		try {
			if (driver != null) {
				Class.forName(driver);
			}
			int shardCount = 0;
			conns = new ArrayList<Connection>(3);
			for (String url: urls.split(",")) {
				//System.out.println("Adding shard node URL: " + url);
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface, all
				// operations should auto commit.
				conn.setAutoCommit(true);
				shardCount++;
				conns.add(conn);  //TODO: what is this?
				
				
				if (conn != null)
					st=conn.createStatement();
				else System.out.println("Error, Connection conn is null.");

			}

			//System.out.println("Using " + shardCount + " shards");

			cachedStatements = new ConcurrentHashMap<StatementType, PreparedStatement>();
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
		System.out.println("KosarSoloClient Close Connection.");
		cleanupAllConnections();
	}

	private PreparedStatement createAndCacheInsertStatement(StatementType insertType, String key)
			throws SQLException {
		StringBuilder insert = new StringBuilder("INSERT INTO ");
		insert.append(insertType.tableName);
		insert.append(" VALUES(?");
		for (int i = 0; i < insertType.numFields; i++) {
			insert.append(",?");
		}
		//the insert is wrong it doesnt insert in the correct order of columns&&&
		// insert.append(");");
		insert.append(")");
		PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(insert.toString());
		PreparedStatement stmt = cachedStatements.putIfAbsent(insertType, insertStatement);
		if (stmt == null) return insertStatement;
		else return stmt;
	}

	private PreparedStatement createAndCacheReadStatement(StatementType readType, String key)
			throws SQLException {
		StringBuilder read = new StringBuilder("SELECT * FROM ");
		read.append(readType.tableName);
		read.append(" WHERE ");
		read.append("key");
		read.append(" = ");
		read.append("?");
		PreparedStatement readStatement = getShardConnectionByKey(key).prepareStatement(read.toString());
		PreparedStatement stmt = cachedStatements.putIfAbsent(readType, readStatement);
		if (stmt == null) return readStatement;
		else return stmt;
	}

	private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType, String key)
			throws SQLException {
		StringBuilder delete = new StringBuilder("DELETE FROM ");
		delete.append(deleteType.tableName);
		delete.append(" WHERE ");
		delete.append("key");
		delete.append(" = ?");
		PreparedStatement deleteStatement = getShardConnectionByKey(key).prepareStatement(delete.toString());
		PreparedStatement stmt = cachedStatements.putIfAbsent(deleteType, deleteStatement);
		if (stmt == null) return deleteStatement;
		else return stmt;
	}

	private PreparedStatement createAndCacheUpdateStatement(StatementType updateType, String key)
			throws SQLException {
		StringBuilder update = new StringBuilder("UPDATE ");
		update.append(updateType.tableName);
		update.append(" SET ");
		for (int i = 1; i <= updateType.numFields; i++) {
			update.append(COLUMN_PREFIX);
			update.append(i);
			update.append("=?");
			if (i < updateType.numFields) update.append(", ");
		}
		//the update doesnt update the right column as well &&&&
		update.append(" WHERE ");
		update.append("key");
		update.append(" = ?");
		PreparedStatement insertStatement = getShardConnectionByKey(key).prepareStatement(update.toString());
		PreparedStatement stmt = cachedStatements.putIfAbsent(updateType, insertStatement);
		if (stmt == null) return insertStatement;
		else return stmt;
	}

	private PreparedStatement createAndCacheScanStatement(StatementType scanType, String key)
			throws SQLException {
		StringBuilder select = new StringBuilder("SELECT * FROM ");
		select.append(scanType.tableName);
		select.append(" WHERE ");
		select.append("key");
		select.append(" >= ");
		select.append("?");
		PreparedStatement scanStatement = getShardConnectionByKey(key).prepareStatement(select.toString());
		PreparedStatement stmt = cachedStatements.putIfAbsent(scanType, scanStatement);
		if (stmt == null) return scanStatement;
		else return stmt;
	}

	

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;
		String query;

		query = "UPDATE friendship SET status = 2 WHERE inviterid=? and inviteeid= ? ";
		try {
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}
		return retVal;		
	}

	

	@Override
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
			//for the additional pic and tpic columns
			if(entitySet.equalsIgnoreCase("users") && insertImage)
				numFields = numFields+2;
			query = "INSERT INTO "+entitySet+" VALUES (";
			for(int j=0; j<=numFields; j++){
				if(j==(numFields)){
					query+="?)";
					break;
				}else
					query+="?,";
			}

			preparedStatement = getShardConnectionByKey(entityPK).prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			int cnt=2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				String field = entry.getValue().toString();
				preparedStatement.setString(cnt, field);
				cnt++;
			}
			if(entitySet.equalsIgnoreCase("users") && insertImage){
				//create the profile image
				byte[] profileImage = ((ObjectByteIterator)values.get("pic")).toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				preparedStatement.setBinaryStream(numFields, is, profileImage.length);
				//create the thumbnail image
				byte[] thumbImage = ((ObjectByteIterator)values.get("tpic")).toArray();
				is = new ByteArrayInputStream(thumbImage);
				preparedStatement.setBinaryStream(numFields+1, is, thumbImage.length);
			}
			rs = preparedStatement.executeQuery();
		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: " + entitySet + e);
			return -2;
		} finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
				return -1;
			}
		}
		return 0;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage, boolean testMode) {
		//if (true) return 0;
		//Statement st = null;
		ResultSet rs = null;
		int retVal = SUCCESS;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query="";
		
		requesterID = 1;
		profileOwnerID = 1;


		try {
			//friend count
			query = "SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ";
			if (st == null)
				System.out.println("Error, Statement st is null.");
			else rs = st.executeQuery(query);
//			preparedStatement = conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			preparedStatement.setInt(2, profileOwnerID);
//			rs = preparedStatement.executeQuery();
			if (rs.next())
				result.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
			else
				result.put("friendcount", new ObjectByteIterator("0".getBytes())) ;


		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
//				if (st != null)
//					st.close();
//				if(preparedStatement != null)
//					preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
				retVal = -2;
			}
		}

		//pending friend request count
		//if owner viwing her own profile, she can view her pending friend requests
		if(requesterID == profileOwnerID){
			//query = "SELECT count(*) FROM  friendship WHERE inviteeID = ? AND status = 1 ";
			try {
				
				query = "SELECT count(*) FROM  friendship WHERE inviteeID = "+profileOwnerID+" AND status = 1 ";
				if (st == null)
					System.out.println("Error, Statement st is null.");
				else rs = st.executeQuery(query);
				if (rs == null)
					System.out.println("Error, result set is null.");
				else if (rs.next())
					result.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
				else
					result.put("pendingcount", new ObjectByteIterator("0".getBytes())) ;
				
//				preparedStatement = conn.prepareStatement(query);
//				preparedStatement.setInt(1, profileOwnerID);
//				preparedStatement.setInt(2, profileOwnerID);
//				rs = preparedStatement.executeQuery();
//				preparedStatement = conn.prepareStatement(query);
//				preparedStatement.setInt(1, profileOwnerID);
//				rs = preparedStatement.executeQuery();
////				if (rs.next())
////					result.put("pendingcount", new StringByteIterator(rs.getString(1))) ;
////				else
////					result.put("pendingcount", new StringByteIterator("0")) ;
			}catch(SQLException sx){
				retVal = -2;
				sx.printStackTrace();
			}finally{
				try {
					if (rs != null)
						rs.close();
					if(preparedStatement != null)
						preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					retVal = -2;
				}
			}
		}

		try {
			//resource count
			query = "SELECT count(*) FROM  resources WHERE wallUserID = "+profileOwnerID;
			if (st == null)
				System.out.println("Error, Statement st is null.");
			else rs = st.executeQuery(query);
			if (rs == null)
				System.out.println("Error, result set is null.");
			else if (rs.next())
				result.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
			else
				result.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
			
//			preparedStatement = conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			rs = preparedStatement.executeQuery();
////			if (rs.next())
////				result.put("resourcecount", new StringByteIterator(rs.getString(1))) ;
////			else
////				result.put("resourcecount", new StringByteIterator("0")) ;
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}

		try {
			//profile details
			if(insertImage)
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = "+profileOwnerID;
			else
				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = "+profileOwnerID;
			if (st == null)
				System.out.println("Error, Statement st is null.");
			else rs = st.executeQuery(query);
			if (rs == null)
				System.out.println("Error, result set is null.");
//			preparedStatement = conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			rs = preparedStatement.executeQuery();
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if(rs.next()){
				for (int i = 1; i <= col; i++){
					String col_name = md.getColumnName(i);
					String value ="";
					if(col_name.equalsIgnoreCase("pic") ){
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
//						//if test mode dump pic into a file
//						if(true){
//							//dump to file
//							try{
//								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-proimage.bmp");
//								fos.write(allBytesInBlob);
//								fos.close();
//							}catch(Exception ex){
//							}
//						}
						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
					}else{
						value = rs.getString(col_name);
						result.put(col_name, new ObjectByteIterator(value.getBytes()));
					}

					
				}
			}

		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}
		
//		result.put("pendingcount", new StringByteIterator("0")) ;
//		result.put("resourcecount", new StringByteIterator("0")) ;
//		result.put("friendcount", new StringByteIterator("0")) ;

		return retVal;
	}


	@Override
	public int listFriends(int requesterID, int profileOwnerID,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result, boolean insertImage, boolean testMode) {

		int retVal = SUCCESS;
		ResultSet rs = null;
		if(requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query ="";
		if(insertImage)
			query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
		else
			query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid=? and userid=inviteeid) or (inviteeid=? and userid=inviterid)) and status = 2";
		try {
			preparedStatement = conn.prepareStatement(query);
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
					//Statement st = conn.createStatement();
					//ResultSet rst = st.executeQuery("SELECT * FROM users");
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++){
						String col_name = md.getColumnName(i);
						String value="";
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
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
		if(insertImage)
			query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
		else 
			query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
		try {
			preparedStatement = conn.prepareStatement(query);
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
						value = allBytesInBlob.toString();
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
						if(col_name.equalsIgnoreCase("userid"))
							col_name = "userid";
						values.put(col_name, new ObjectByteIterator(value.getBytes()));
					}
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}

		return retVal;		
	}

	@Override
	public int rejectFriend(int inviterID, int inviteeID) {
		int retVal = SUCCESS;
		if(inviterID < 0 || inviteeID < 0)
			return -1;

		//System.out.println(inviterID+" "+inviteeID);
		String query = "DELETE FROM friendship WHERE inviterid=? and inviteeid= ? and status=1";
		try {
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}
		return retVal;	
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
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.setInt(2, inviteeID);
			preparedStatement.executeUpdate();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, friendid1);
			preparedStatement.setInt(2, friendid2);
			preparedStatement.setInt(3, friendid2);
			preparedStatement.setInt(4, friendid1);

			preparedStatement.executeUpdate();
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
			preparedStatement = conn.prepareStatement(query);
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
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
			}
		}

		return retVal;		
	}



	public int getCreatedResources(int resourceCreatorID, Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		if(resourceCreatorID < 0)
			return -1;

		String query = "SELECT * FROM resources WHERE creatorid = ?";
		try {
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, resourceCreatorID);
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
					values.put(col_name, new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		}catch(SQLException sx){
			retVal = -2;
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
			preparedStatement = conn.prepareStatement(query);
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
			sx.printStackTrace();
		}finally{
			try {
				if (rs != null)
					rs.close();
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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

		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?,?, ?, ?, ?, ?)";
		try {
			preparedStatement = conn.prepareStatement(query);
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
			sx.printStackTrace();
		}finally{
			try {
				if(preparedStatement != null)
					preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace();
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
			sx.printStackTrace();
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}
		return stats;
	}

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
			sx.printStackTrace();
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace();
				retVal = -2;
			}
		}
		return retVal;
	}


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
			sx.printStackTrace();
			retVal = -2;
		}finally{
			try {
				if(rs != null)
					rs.close();
				if(st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace();
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
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");

			stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			stmt.executeUpdate("CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE ");
			stmt.executeUpdate("CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID NUMBER, INVITEEID NUMBER,"
					+ "STATUS NUMBER DEFAULT 1" + ") NOLOGGING");

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
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200), PIC BLOB, TPIC BLOB"
						+ ") NOLOGGING");
			} else {
				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID NUMBER, USERNAME VARCHAR2(200), "
						+ "PW VARCHAR2(200), FNAME VARCHAR2(200), "
						+ "LNAME VARCHAR2(200), GENDER VARCHAR2(200),"
						+ "DOB VARCHAR2(200),JDATE VARCHAR2(200), "
						+ "LDATE VARCHAR2(200), ADDRESS VARCHAR2(200),"
						+ "EMAIL VARCHAR2(200), TEL VARCHAR2(200)"
						+ ") NOLOGGING");

			}

			stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID) ENABLE");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
					+ "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			stmt.executeUpdate("ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
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

		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (stmt != null)
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
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
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)"
					+ "COMPUTE STATISTICS NOLOGGING");

			stmt.executeUpdate("analyze table users compute statistics");
			stmt.executeUpdate("analyze table resources compute statistics");
			stmt.executeUpdate("analyze table friendship compute statistics");
			stmt.executeUpdate("analyze table manipulation compute statistics");
			long endIdx = System.currentTimeMillis();
			System.out
			.println("Time to build database index structures(ms):"
					+ (endIdx - startIdx));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
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
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int numusers = 10000;
		//Stand alone test
		Properties p = new Properties();
		p.setProperty(CONNECTION_URL, "kosarsolo:jdbc:oracle:thin:kosarsolo/solo@//10.0.0.122:1521/ORCL");
		p.setProperty(CONNECTION_USER, "kosarsolo");
		p.setProperty(CONNECTION_PASSWD, "solo");
		p.setProperty(DRIVER_CLASS, "com.mitrallc.sql.KosarSoloDriver");
		
		KosarSoloClient KS = new KosarSoloClient();
		try {
			KS.init(p);
		} catch (DBException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		int NumThreads = 1;
		try {
			KS.init();
		} catch (DBException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HashMap result = new HashMap<String,String>();
		for (int i=0; i < 1000*1000; i++){
			KS.viewProfile(i%numusers, i%numusers, result, true, false);
		}
		KS.cleanup(false);
	}

	@Override
	public int delCommentOnResource(int resourceCreatorID, int resourceID,
			int manipulationID) {
		// TODO Auto-generated method stub
		return 0;
	}


}