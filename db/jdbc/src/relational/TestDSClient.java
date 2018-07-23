package relational;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
//import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

//import com.mysql.jdbc.Statement;
import edu.usc.bg.base.ByteIterator;
import edu.usc.bg.base.Client;
import edu.usc.bg.base.DB;
import edu.usc.bg.base.DBException;
import edu.usc.bg.base.ObjectByteIterator;

//import com.ibm.db2.jcc.*;

import java.io.*;

public class TestDSClient extends DB implements JdbcDBClientConstants {
	private boolean initialized = false;
	private static String FSimagePath = "";
	private PreparedStatement preparedStatement;
	private boolean verbose = false;
	private static final String DEFAULT_PROP = "";
	private Properties props;
	private Connection conn;
	private static int INVFRND_STMT = 13;
	private static int GETFRNDCNT_STMT = 2;
	private static int GETPENDCNT_STMT = 3;
	private static int GETRESCNT_STMT = 4;
	//private static int GETPROFILE_STMT = 5;
	//private static int GETPROFILEIMG_STMT = 6;
	private static int GETFRNDS_STMT = 7;
	private static int GETFRNDSIMG_STMT = 8;
	private static int GETPEND_STMT = 9;
	private static int GETPENDIMG_STMT = 10;
	private static int REJREQ_STMT = 11;
	private static int ACCREQ_STMT = 12;
	private static int UNFRNDFRND_STMT = 14;
	private static int GETTOPRES_STMT = 15;
	private static int GETRESCMT_STMT = 16;
	private static int POSTCMT_STMT = 17;
	private static int DELCMT_STMT = 18;

	private static int INC_RES_STMT = 19;
	private static int DEC_RES_STMT = 20;
	private static int USER_AGG_ATTRIB_OTHER = 21;
	private static int USER_AGG_ATTRIB_SELF = 21;
	
	
	private static int GETPROFILE_STMT_OTHER = 5;
	private static int GETPROFILEIMG_STMT_OTHER = 6;
	private static int GETPROFILE_STMT_SELF = 5;
	private static int GETPROFILEIMG_STMT_SELF = 6;
	

	/** Statement pool to cache the statements. **/
	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;

	static void tbRunstats(Connection conn) throws Exception {
		System.out
				.print("\n-----------------------------------------------------------\n"
						+ "\nUSE THE SQL STATEMENT:\n"
						+ "  RUNSTATS\n"
						+ "TO UPDATE TABLE STATISTICS.\n");

		// get fully qualified name of the table
		String tableName = "USERS";
		String schemaName = getSchemaName(conn, tableName);

		String fullTableName = schemaName + "." + tableName;

		try {
			// store the CLP commands in a file and execute the file
			File outputFile = new File("RunstatsCmd.db2");
			FileWriter out = new FileWriter(outputFile);

			String cmd = "RUNSTATS ON TABLE " + fullTableName
					+ " WITH DISTRIBUTION ON KEY COLUMNS"
					+ " DEFAULT NUM_FREQVALUES 30 NUM_QUANTILES -1"
					+ " ALLOW READ ACCESS";

			out.write("CONNECT TO SAMPLE;\n");
			out.write(cmd + ";\n");
			out.write("CONNECT RESET;\n");

			out.close();

			Process p = Runtime.getRuntime().exec("db2 -vtf RunstatsCmd.db2");

			// open streams for the process's input and error
			BufferedReader stdInput = new BufferedReader(new InputStreamReader(
					p.getInputStream()));
			BufferedReader stdError = new BufferedReader(new InputStreamReader(
					p.getErrorStream()));
			String s;

			// read the output from the command and set the output variable with
			// the value
			while ((s = stdInput.readLine()) != null) {
				System.out.println(s);
			}

			// read any errors from the attempted command and set the error
			// variable with the value
			while ((s = stdError.readLine()) != null) {
				System.out.println(s);
			}

			// destroy the process created
			p.destroy();

			// delete the temporary file created
			outputFile.deleteOnExit();
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	} // tbRunstats

	static String getSchemaName(Connection conn, String tableName)
			throws Exception {
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT tabschema "
				+ "  FROM syscat.tables " + "  WHERE tabname = '" + tableName
				+ "'");

		boolean result = rs.next();
		String schemaName = rs.getString("tabschema");
		rs.close();
		stmt.close();

		// remove the trailing white space characters from schemaName before
		// returning it to the calling function
		return schemaName.trim();
	} // getSchemaName

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

		try {
			if (driver != null) {
				Class.forName(driver);
			}
			for (String url : urls.split(",")) {
				conn = DriverManager.getConnection(url, user, passwd);
				// Since there is no explicit commit method in the DB interface,
				// all
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

	private void cleanupAllConnections() {
		try {
			// close all cached prepare statements
			Set<Integer> statementTypes = newCachedStatements.keySet();
			Iterator<Integer> it = statementTypes.iterator();
			while (it.hasNext()) {
				int stmtType = it.next();
				if (newCachedStatements.get(stmtType) != null)
					newCachedStatements.get(stmtType).close();
			}
			if (conn != null) {
				// conn.commit();
				conn.close();
			}
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}
	}

	public static void dropTable(Statement st, String tableName) {
		try {
			st.executeUpdate("drop table " + tableName);
		} catch (SQLException e) {
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

	@Override
	public int insertEntity(String entitySet, String entityPK,
			HashMap<String, ByteIterator> values, boolean insertImage) {

		if (entitySet == null) {
			return -1;
		}
		if (entityPK == null) {
			return -1;
		}
		// System.out.println("insert entity");
		String parameters = null;
		String WallID = null;
		try {
			String query;
			String WalluserID = null;
			int numFields = values.size();
			if (entitySet.equalsIgnoreCase("users") && insertImage
					&& !FSimagePath.equals(""))
				numFields = numFields - 2;

			query = "INSERT INTO  " + entitySet + " VALUES (";
			String Proc_query = "INSERT INTO  " + entitySet + " VALUES (";

			if (entitySet.equalsIgnoreCase("users")) {
				for (int j = 0; j <= numFields + 3; j++) {
					if (j == (numFields + 3)) {
						query += "?)";
						break;
					} else
						query += "?,";
				}
			} else {
				for (int j = 0; j <= numFields; j++) {
					if (j == (numFields)) {
						query += "?)";
						break;
					} else
						query += "?,";
				}
			}

			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, entityPK);
			Proc_query += entityPK + ",";
			int cnt = 2;
			for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
				// blobs need to be inserted differently

				String field = null;
				if (entry.getKey().equalsIgnoreCase("pic")
						|| entry.getKey().equalsIgnoreCase("tpic"))
					continue;
				if (entry.getKey().equalsIgnoreCase("num_friends")
						|| entry.getKey().equalsIgnoreCase("num_pend_friends")
						|| entry.getKey().equalsIgnoreCase("num_resources")) {
					field = "0";
				}

				else {
					field = entry.getValue().toString();
				}

				if (entry.getKey().equalsIgnoreCase("WALLUSERID")) {
					WalluserID = entry.getValue().toString();
					;
				}

				parameters = parameters + "\n" + entry.getKey() + "= " + field;
				preparedStatement.setString(cnt, field);
				Proc_query += "'" + field + "',";
				cnt++;
				if (verbose)
					System.out.println("" + entry.getKey().toString() + ":"
							+ field);
			}

			if (entitySet.equalsIgnoreCase("users") && insertImage) {
				byte[] profileImage = ((ObjectByteIterator) values.get("pic"))
						.toArray();
				InputStream is = new ByteArrayInputStream(profileImage);
				if (FSimagePath.equals("")) {
					preparedStatement.setBinaryStream(numFields, is,
							profileImage.length);
					parameters = parameters + "\npic";
				}

				else
					StoreImageInFS(entityPK, profileImage, true);

				byte[] thumbImage = ((ObjectByteIterator) values.get("tpic"))
						.toArray();
				is = new ByteArrayInputStream(thumbImage);

				if (FSimagePath.equals("")) {
					preparedStatement.setBinaryStream(numFields + 1, is,
							thumbImage.length);
					parameters += "\ntpic";
				} else
					StoreImageInFS(entityPK, thumbImage, false);
			}

			if (entitySet.equalsIgnoreCase("users")) {
				preparedStatement.setInt(numFields + 2, 0);
				preparedStatement.setInt(numFields + 3, 0);
				preparedStatement.setInt(numFields + 4, 0);
			}

			// System.out.println("query:" + query);
			// System.out.println("parameters:"+ parameters);
			preparedStatement.executeUpdate();
			WallID = WalluserID;

		} catch (SQLException e) {
			System.out.println("Error in processing insert to table: "
					+ entitySet + e);
			e.printStackTrace();
			return -2;
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close();

			} catch (SQLException e) {
				e.printStackTrace(System.out);
				return -1;
			}
		}

		if (entitySet.equalsIgnoreCase("resources") && WallID != null) // if
																		// exception
																		// occured
																		// b4
																		// then
																		// WallID
																		// will
																		// be
																		// already
																		// null
		{
			// System.out.println("Inside resources increment");
			update_resource_count(WallID, "INCREMENT");
		}

		return 0;
	}

	private boolean StoreImageInFS(String userid, byte[] image,
			boolean profileimg) {
		boolean result = true;
		String ext = "thumbnail";
		if (profileimg)
			ext = "profile";
		String ImageFileName = FSimagePath + "\\img" + userid + ext;
		File tgt = new File(ImageFileName);
		if (tgt.exists()) {
			if (!tgt.delete()) {
				System.out.println("Error, file exists and failed to delete");
				return false;
			}
		}

		// Write the file
		try {
			FileOutputStream fos = new FileOutputStream(ImageFileName);
			fos.write(image);
			fos.close();
		} catch (Exception ex) {
			System.out.println("Error in writing the file" + ImageFileName);
			ex.printStackTrace(System.out);
		}

		return result;
	}

	@Override
	public int viewProfile(int requesterID, int profileOwnerID,
			HashMap<String, ByteIterator> result, boolean insertImage,
			boolean testMode) {
		// System.out.println("viewProfile");
		ResultSet rs = null;
		String execTime = "\n--------\n";
		int retVal = SUCCESS;
		if (requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query = "";
		String uid = "";

		/*
		 try {

			if (requesterID == profileOwnerID) {
				query = "SELECT NUM_FRIENDS , NUM_PEND_FRIENDS , NUM_RESOURCES FROM  USERS WHERE USERID = ? ";

				if ((preparedStatement = newCachedStatements
						.get(USER_AGG_ATTRIB_SELF)) == null)
					preparedStatement = createAndCacheStatement(
							USER_AGG_ATTRIB_SELF, query);
			} else {
				query = "SELECT NUM_FRIENDS  , NUM_RESOURCES FROM  USERS WHERE USERID = ? ";

				if ((preparedStatement = newCachedStatements
						.get(USER_AGG_ATTRIB_OTHER)) == null)
					preparedStatement = createAndCacheStatement(
							USER_AGG_ATTRIB_OTHER, query);
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();

			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = "";

					if (col_name.equalsIgnoreCase("NUM_FRIENDS")) {
						result.put("friendcount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}
					if (col_name.equalsIgnoreCase("NUM_PEND_FRIENDS")) {
						result.put("pendingcount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}
					if (col_name.equalsIgnoreCase("NUM_RESOURCES")) {
						result.put("resourcecount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}

				}
			} else
			{
				
				result.put("friendcount", new ObjectByteIterator(rs
						.getString("0").getBytes()));
				
				result.put("resourcecount", new ObjectByteIterator(rs
						.getString("0").getBytes()));
				if(requesterID==profileOwnerID)
				{
				result.put("pendingcount", new ObjectByteIterator(rs
						.getString("0").getBytes()));
				}
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				// preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		
		*/
		
		
		/*

		try {

			query = "SELECT NUM_FRIENDS FROM  USERS WHERE USERID = ? ";
			if ((preparedStatement = newCachedStatements.get(GETFRNDCNT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETFRNDCNT_STMT,
						query);

			preparedStatement.setInt(1, profileOwnerID);
			execTime += " b4 query 1 " + System.currentTimeMillis();
			rs = preparedStatement.executeQuery();
			execTime += "- after query 1 " + System.currentTimeMillis() + "\n";

			if (rs.next())

				result.put("friendcount", new ObjectByteIterator(rs
						.getString(1).getBytes()));
			else
				result.put("friendcount",
						new ObjectByteIterator("0".getBytes()));

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				// preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}

		// pending friend request count
		// if owner viewing her own profile, she can view her pending friend
		// requests
		if (requesterID == profileOwnerID) {
			// query =
			// "SELECT count(*) FROM  friendship WHERE inviteeID = ? AND status = 1 ";
			// stale data
			query = "SELECT NUM_PEND_FRIENDS FROM  USERS WHERE USERID = ?";
			try {
				// preparedStatement = conn.prepareStatement(query);
				if ((preparedStatement = newCachedStatements
						.get(GETPENDCNT_STMT)) == null)
					preparedStatement = createAndCacheStatement(
							GETPENDCNT_STMT, query);

				preparedStatement.setInt(1, profileOwnerID);
				execTime += " b4 query 2 " + System.currentTimeMillis();
				rs = preparedStatement.executeQuery();
				execTime += " - after query 2 " + System.currentTimeMillis()
						+ "\n";

				if (rs.next())
					result.put("pendingcount", new ObjectByteIterator(rs
							.getString(1).getBytes()));
				else
					result.put("pendingcount",
							new ObjectByteIterator("0".getBytes()));
			} catch (SQLException sx) {
				retVal = -2;
				sx.printStackTrace(System.out);
			} finally {
				try {
					if (rs != null)
						rs.close();
					if (preparedStatement != null)
						preparedStatement.clearParameters();
					// preparedStatement.close();
				} catch (SQLException e) {
					e.printStackTrace(System.out);
					retVal = -2;
				}
			}
		}
		// resource count

		query = "SELECT NUM_RESOURCES FROM  USERS WHERE USERID = ?";
		try {
			// preparedStatement = conn.prepareStatement(query);
			if ((preparedStatement = newCachedStatements.get(GETRESCNT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCNT_STMT,
						query);
			preparedStatement.setInt(1, profileOwnerID);
			execTime += " b4 query 3 " + System.currentTimeMillis();
			rs = preparedStatement.executeQuery();
			execTime += " - after query 3 " + System.currentTimeMillis() + "\n";

			if (rs.next())
				result.put("resourcecount", new ObjectByteIterator(rs
						.getString(1).getBytes()));
			else
				result.put("resourcecount",
						new ObjectByteIterator("0".getBytes()));
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				// preparedStatement.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		
		
		*/
		
		
		try {
			// profile details
			if (insertImage && FSimagePath.equals("")) {
				
				if(requesterID==profileOwnerID)
				{
					query = "SELECT NUM_FRIENDS , NUM_RESOURCES  , NUM_PEND_FRIENDS ,userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = ?";
					if ((preparedStatement = newCachedStatements
							.get(GETPROFILEIMG_STMT_SELF)) == null)
						preparedStatement = createAndCacheStatement(
								GETPROFILEIMG_STMT_SELF, query);
				}
				else
				{
					query = "SELECT NUM_FRIENDS , NUM_RESOURCES, userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel, pic FROM  users WHERE UserID = ?";
					if ((preparedStatement = newCachedStatements
							.get(GETPROFILEIMG_STMT_OTHER)) == null)
						preparedStatement = createAndCacheStatement(
								GETPROFILEIMG_STMT_OTHER, query);
				}
				
				
				
			} else {
				if(requesterID==profileOwnerID)
				{
					query = "SELECT  NUM_FRIENDS , NUM_RESOURCES  , NUM_PEND_FRIENDS,  userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
					if ((preparedStatement = newCachedStatements
							.get(GETPROFILE_STMT_SELF)) == null)
						preparedStatement = createAndCacheStatement(
								GETPROFILE_STMT_SELF, query);
				}
				else
				{
					query = "SELECT NUM_FRIENDS , NUM_RESOURCES  ,userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
					if ((preparedStatement = newCachedStatements
							.get(GETPROFILE_STMT_OTHER)) == null)
						preparedStatement = createAndCacheStatement(
								GETPROFILE_STMT_OTHER, query);
				}
			}
			// preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, profileOwnerID);
			execTime += " b4 query 4 " + System.currentTimeMillis();
			rs = preparedStatement.executeQuery();
			execTime += " -after query 4 " + System.currentTimeMillis() + "\n";
			ResultSetMetaData md = rs.getMetaData();
			int col = md.getColumnCount();
			if (rs.next()) {
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = "";

					if (col_name.equalsIgnoreCase("userid")) {
						uid = rs.getString(col_name);
					}
					
					
					if (col_name.equalsIgnoreCase("NUM_FRIENDS")) {
						result.put("friendcount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}
					if (col_name.equalsIgnoreCase("NUM_PEND_FRIENDS")) {
						result.put("pendingcount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}
					if (col_name.equalsIgnoreCase("NUM_RESOURCES")) {
						result.put("resourcecount", new ObjectByteIterator(rs
								.getString(col_name).getBytes()));
					}
					
					if (col_name.equalsIgnoreCase("pic")) {
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1,
								(int) aBlob.length());
						// if test mode dump pic into a file
						if (testMode) {
							// dump to file
							try {
								FileOutputStream fos = new FileOutputStream(
										profileOwnerID + "-proimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							} catch (Exception ex) {
							}
						}
						result.put(col_name, new ObjectByteIterator(
								allBytesInBlob));
					} else {
						if(!((col_name.equalsIgnoreCase("NUM_FRIENDS")) || (col_name.equalsIgnoreCase("NUM_PEND_FRIENDS")) || (col_name.equalsIgnoreCase("NUM_RESOURCES"))))
						{
							value = rs.getString(col_name);
							result.put(col_name,
									new ObjectByteIterator(value.getBytes()));	
						}
						
					}
				}

				// Fetch the profile image from the file system
				if (insertImage && !FSimagePath.equals("")) {
					// Get the profile image from the file
					byte[] profileImage = GetImageFromFS(uid, true);
					if (testMode) {
						// dump to file
						try {
							FileOutputStream fos = new FileOutputStream(
									profileOwnerID + "-proimage.bmp");
							fos.write(profileImage);
							fos.close();
						} catch (Exception ex) {
						}
					}
					result.put("pic", new ObjectByteIterator(profileImage));
				}
			}

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		// System.out.println(execTime);
		return retVal;
	}

	private byte[] GetImageFromFS(String userid, boolean profileimg) {
		int filelength = 0;
		String ext = "thumbnail";
		byte[] imgpayload = null;
		// BufferedInputStream bis = null;

		if (profileimg)
			ext = "profile";

		String ImageFileName = FSimagePath + "\\img" + userid + ext;
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
				while (read < filelength
						&& (numRead = dis.read(imgpayload, read, filelength
								- read)) >= 0) {
					read = read + numRead;
				}
				dis.close();
				fis.close();
				break;
			} catch (IOException e) {
				e.printStackTrace(System.out);
				attempt--;
			}
		}
		return imgpayload;
	}

	@Override
	public int acceptFriend(int inviterID, int inviteeID) {

		int retVal = SUCCESS;
		if (inviterID < 0 || inviteeID < 0)
			return -1;
		String query;
		CallableStatement cs = null;
		query = "UPDATE friendship SET status = 2 WHERE inviterid=? and inviteeid= ? ";
		try {

			// preparedStatement = conn.prepareStatement(query);
			/*
			 * if((preparedStatement = newCachedStatements.get(ACCREQ_STMT)) ==
			 * null){ preparedStatement = createAndCacheStatement(ACCREQ_STMT,
			 * query); } preparedStatement.setInt(1, inviterID);
			 * preparedStatement.setInt(2, inviteeID);
			 * conn.setAutoCommit(false); preparedStatement.executeUpdate();
			 * 
			 * IncrementFriendCounter(inviterID,"CONFIRMED");
			 * IncrementFriendCounter(inviteeID,"CONFIRMED");
			 * DecrementFriendCounter(inviteeID, "PENDING");
			 * 
			 * conn.commit(); conn.setAutoCommit(true);
			 */
			cs = conn.prepareCall("{call acceptFriend(?,?,?)}");
			cs.setString(1, Integer.toString(inviterID));
			cs.setString(2, Integer.toString(inviteeID));
			cs.registerOutParameter(3, Types.VARCHAR);
			cs.executeUpdate();
			String Status = cs.getString(3);
			if (!Status.equalsIgnoreCase("S"))
				System.out.println("?????acceptFriend = " + Status);
			// cs.close();

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				// preparedStatement.close();
				cs.close();
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
		if (inviterID < 0 || inviteeID < 0)
			return -1;
		System.out.print("inside invite friend\t");
		String query = "INSERT INTO friendship values(?,?,1)";

		CallableStatement cs = null;

		try {

			/*
			 * if((preparedStatement = newCachedStatements.get(INVFRND_STMT)) ==
			 * null) preparedStatement = createAndCacheStatement(INVFRND_STMT,
			 * query); preparedStatement.setInt(1, inviterID);
			 * preparedStatement.setInt(2, inviteeID);
			 * conn.setAutoCommit(false); preparedStatement.executeUpdate();
			 * 
			 * IncrementFriendCounter(inviteeID,"PENDING"); //Increment Pending
			 * Friend Count in User table conn.commit();
			 * conn.setAutoCommit(true);
			 */

			cs = conn.prepareCall("{call inviteFriend(?,?,?)}");
			cs.setString(1, Integer.toString(inviterID));
			cs.setString(2, Integer.toString(inviteeID));
			cs.registerOutParameter(3, Types.VARCHAR);
			// System.out.println("b4 inviteFriend");
			cs.executeUpdate();
			// System.out.println("after inviteFriend");
			String Status = cs.getString(3);
			if (!Status.equalsIgnoreCase("S"))
				System.out.println("??????????????????????????inviteFriend= "
						+ Status + " inviterid=" + inviterID + " invitteid="
						+ inviteeID);
			// cs.close();

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
			System.out.println(inviterID + " invites " + inviteeID);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				cs.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);

			}
		}

		return retVal;
	}

	private PreparedStatement createAndCacheStatement(int stmttype, String query)
			throws SQLException {
		PreparedStatement newStatement = conn.prepareStatement(query);
		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype,
				newStatement);
		if (stmt == null)
			return newStatement;
		else
			return stmt;
	}

	@Override
	public int getCreatedResources(int resourceCreatorID,
			Vector<HashMap<String, ByteIterator>> result) {
		int retVal = SUCCESS;
		ResultSet rs = null;
		Statement st = null;
		if (resourceCreatorID < 0)
			return -1;

		String query = "SELECT * FROM resources WHERE creatorid = "
				+ resourceCreatorID;
		try {
			st = conn.createStatement();
			rs = st.executeQuery(query);
			while (rs.next()) {
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if (col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					values.put(col_name,
							new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}

	@Override
	// init phase query not cached
	public HashMap<String, String> getInitialStats() {
		HashMap<String, String> stats = new HashMap<String, String>();
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		try {
			st = conn.createStatement();
			// get user count
			query = "SELECT count(userid) from users";
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("usercount", rs.getString(1));
			} else
				stats.put("usercount", "0"); // sth is wrong - schema is missing
			if (rs != null)
				rs.close();
			// get user offset
			query = "SELECT min(userid) from users";
			rs = st.executeQuery(query);
			String offset = "0";
			if (rs.next()) {
				offset = rs.getString(1);

			}

			// get resources per user
			query = "SELECT count(rid) from resources where creatorid="
					+ Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("resourcesperuser", rs.getString(1));
			} else {
				stats.put("resourcesperuser", "0");
			}
			if (rs != null)
				rs.close();
			// get number of friends per user
			// query =
			// "select count(*) from friendship where (inviterid="+Integer.parseInt(offset)
			// +" OR inviteeid="+Integer.parseInt(offset) +") AND status=2" ;
			query = "select NUM_FRIENDS from USERS where USERID="
					+ Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("avgfriendsperuser", rs.getString(1));
			} else
				stats.put("avgfriendsperuser", "0");
			if (rs != null)
				rs.close();
			// query =
			// "select count(*) from friendship where (inviteeid="+Integer.parseInt(offset)
			// +") AND status=1" ;
			query = "select NUM_PEND_FRIENDS from USERS where USERID="
					+ Integer.parseInt(offset);
			rs = st.executeQuery(query);
			if (rs.next()) {
				stats.put("avgpendingperuser", rs.getString(1));
			} else
				stats.put("avgpendingperuser", "0");

		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}

		}

		return stats;
	}

	@Override
	// Load phase query not cached
	public int CreateFriendship(int memberA, int memberB) {

		int retVal = SUCCESS;
		if (memberA < 0 || memberB < 0)
			return -1;
		CallableStatement cs = null;
		try {
			/*
			 * String DML = "INSERT INTO friendship values(?,?,2)";
			 * preparedStatement = conn.prepareStatement(DML);
			 * preparedStatement.setInt(1, memberA); preparedStatement.setInt(2,
			 * memberB);
			 * 
			 * conn.setAutoCommit(false); preparedStatement.executeUpdate();
			 * 
			 * DecrementFriendCounter(memberA,"PENDING");
			 * 
			 * IncrementFriendCounter(memberA,"CONFIRMED");
			 * IncrementFriendCounter(memberB,"CONFIRMED"); conn.commit();
			 * conn.setAutoCommit(true);
			 */

			// CallableStatement cs = null;
			cs = conn.prepareCall("{call CreateFriendship(?,?,?)}");
			cs.setString(1, Integer.toString(memberA));
			cs.setString(2, Integer.toString(memberB));
			cs.registerOutParameter(3, Types.VARCHAR);
			cs.executeUpdate();
			String Status = cs.getString(3);
			if (!Status.equalsIgnoreCase("S"))
				System.out.println("?????CreateFriendship = " + Status);
			// cs.close();

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.close();
				cs.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;
	}

	@Override
	public int queryPendingFriendshipIds(int inviteeid,
			Vector<Integer> pendingIds) {
		// System.out.println("queryPendingFriendshipIds");
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if (inviteeid < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			// query =
			// "SELECT inviterid from friendship where inviteeid='"+inviteeid+"' and status='1'";
			query = "SELECT inviterid from pending_friendship where inviteeid='"
					+ inviteeid + "'";
			rs = st.executeQuery(query);
			while (rs.next()) {
				pendingIds.add(rs.getInt(1));
			}
		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
			retVal = -2;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;
	}

	@Override
	public int queryConfirmedFriendshipIds(int profileId,
			Vector<Integer> confirmedIds) {
		// System.out.println("queryConfirmedFriendshipIds");
		int retVal = SUCCESS;
		Statement st = null;
		ResultSet rs = null;
		String query = "";
		if (profileId < 0)
			retVal = -1;
		try {
			st = conn.createStatement();
			// query =
			// "SELECT inviterid, inviteeid from friendship where (inviteeid="+profileId+" OR inviterid="+profileId+") and status='2'";
			// query =
			// "SELECT inviterid, inviteeid from friendship where (inviteeid="+profileId+" ) and status='2'";
			query = "SELECT inviterid, inviteeid from friendship where (inviteeid="
					+ profileId + " )";
			rs = st.executeQuery(query);
			while (rs.next()) {
				if (rs.getInt(1) != profileId)
					confirmedIds.add(rs.getInt(1));
				else
					confirmedIds.add(rs.getInt(2));
			}
		} catch (SQLException sx) {
			sx.printStackTrace(System.out);
			retVal = -2;
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (st != null)
					st.close();
			} catch (SQLException e) {
				e.printStackTrace(System.out);
				retVal = -2;
			}
		}
		return retVal;

	}

	public void createSchema(Properties props) {

		Statement stmt = null;
		// System.out.println("Inside Create Schema");

		try {
			stmt = conn.createStatement();

			// dropSequence(stmt, "MIDINC");
			/*
			 * dropSequence(stmt, "RIDINC"); dropSequence(stmt, "USERIDINC");
			 * dropSequence(stmt, "USERIDS");
			 */
			dropTable(stmt, "friendship");
			dropTable(stmt, "manipulation");
			dropTable(stmt, "resources");
			dropTable(stmt, "users");
			dropTable(stmt, "pending_friendship");

			// stmt.executeUpdate("CREATE SEQUENCE  MIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 201 CACHE 20 NOORDER  NOCYCLE");
			/*
			 * stmt.executeUpdate(
			 * "CREATE SEQUENCE  RIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE "
			 * ); stmt.executeUpdate(
			 * "CREATE SEQUENCE  USERIDINC  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 21 CACHE 20 NOORDER  NOCYCLE "
			 * ); stmt.executeUpdate(
			 * "CREATE SEQUENCE  USERIDS  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 1 CACHE 20 NOORDER  NOCYCLE"
			 * );
			 */

			if (Boolean.parseBoolean(props.getProperty(
					Client.INSERT_IMAGE_PROPERTY,
					Client.INSERT_IMAGE_PROPERTY_DEFAULT))
					&& props.getProperty(FS_PATH, "").equals("")) {

				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER NOT NULL PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200),"
						+ "PIC BLOB   NOT LOGGED COMPACT, TPIC BLOB  NOT LOGGED COMPACT, NUM_FRIENDS INTEGER  DEFAULT  0, NUM_PEND_FRIENDS INTEGER  DEFAULT 0, NUM_RESOURCES INTEGER  DEFAULT 0"
						+ ")");
			} else {

				stmt.executeUpdate("CREATE TABLE USERS"
						+ "(USERID INTEGER  NOT NULL PRIMARY KEY, USERNAME VARCHAR(200), "
						+ "PW VARCHAR(200), FNAME VARCHAR(200), "
						+ "LNAME VARCHAR(200), GENDER VARCHAR(200),"
						+ "DOB VARCHAR(200),JDATE VARCHAR(200), "
						+ "LDATE VARCHAR(200), ADDRESS VARCHAR(200),"
						+ "EMAIL VARCHAR(200), TEL VARCHAR(200),"
						+ "NUM_FRIENDS INTEGER DEFAULT 0, NUM_PEND_FRIENDS INTEGER DEFAULT 0, NUM_RESOURCES INTEGER DEFAULT 0"
						+ ")");

			}

			stmt.executeUpdate("CREATE TABLE RESOURCES"
					+ "( RID INTEGER NOT NULL PRIMARY KEY ,CREATORID INTEGER NOT NULL,"
					+ "WALLUSERID INTEGER NOT NULL, TYPE VARCHAR(200),"
					+ "BODY VARCHAR(200), DOC VARCHAR(200),"
					+ "FOREIGN KEY(CREATORID) REFERENCES USERS(USERID) ON DELETE CASCADE,"
					+ "FOREIGN KEY(WALLUSERID) REFERENCES USERS(USERID) ON DELETE CASCADE"
					+ ")");

			stmt.executeUpdate("CREATE TABLE PENDING_FRIENDSHIP"
					+ "(INVITERID INTEGER NOT NULL, INVITEEID INTEGER NOT NULL,"
					+ "STATUS INTEGER DEFAULT 1,"
					+ "PRIMARY KEY (INVITERID,INVITEEID),"
					+ "FOREIGN KEY(INVITERID) REFERENCES USERS(USERID) ON DELETE CASCADE,"
					+ "FOREIGN KEY(INVITEEID) REFERENCES USERS(USERID) ON DELETE CASCADE)");

			stmt.executeUpdate("CREATE TABLE FRIENDSHIP"
					+ "(INVITERID INTEGER NOT NULL, INVITEEID INTEGER NOT NULL,"
					+ "STATUS INTEGER DEFAULT 1,"
					+ "PRIMARY KEY (INVITERID,INVITEEID),"
					+ "FOREIGN KEY(INVITERID) REFERENCES USERS(USERID) ON DELETE CASCADE,"
					+ "FOREIGN KEY(INVITEEID) REFERENCES USERS(USERID) ON DELETE CASCADE)");

			System.out.println("Friendship created");

			stmt.executeUpdate("CREATE TABLE MANIPULATION"
					+ "( MID INTEGER NOT NULL,"
					+ "CREATORID INTEGER NOT NULL, RID INTEGER NOT NULL,"
					+ "MODIFIERID INTEGER NOT NULL, TIMESTAMP VARCHAR(200),"
					+ "TYPE VARCHAR(200), CONTENT VARCHAR(200),"
					+ "PRIMARY KEY (MID,RID),"
					+ "FOREIGN KEY(RID) REFERENCES RESOURCES(RID) ON DELETE CASCADE,"
					+ "FOREIGN KEY(CREATORID) REFERENCES USERS(USERID) ON DELETE CASCADE,"
					+ "FOREIGN KEY(MODIFIERID) REFERENCES USERS(USERID) ON DELETE CASCADE)");

			stmt.executeUpdate("alter table users activate not logged initially");
			stmt.executeUpdate("alter table FRIENDSHIP activate not logged initially");
			stmt.executeUpdate("alter table PENDING_FRIENDSHIP activate not logged initially");
			stmt.executeUpdate("alter table RESOURCES activate not logged initially");
			stmt.executeUpdate("alter table MANIPULATION activate not logged initially");

			// stmt.executeUpdate("ALTER TABLE USERS MODIFY (USERID NOT NULL ENABLE)");
			// stmt.executeUpdate("ALTER TABLE USERS ADD CONSTRAINT USERS_PK PRIMARY KEY (USERID) ENABLE");
			// stmt.executeUpdate("ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_PK PRIMARY KEY (MID,RID) ENABLE");
			// stmt.executeUpdate("ALTER TABLE MANIPULATION MODIFY (MID NOT NULL ENABLE)");
			/*
			 * stmt.executeUpdate(
			 * "ALTER TABLE MANIPULATION MODIFY (CREATORID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE MANIPULATION MODIFY (RID NOT NULL ENABLE)");
			 * stmt.executeUpdate
			 * ("ALTER TABLE MANIPULATION MODIFY (MODIFIERID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_PK PRIMARY KEY (INVITERID, INVITEEID) ENABLE"
			 * ); stmt.executeUpdate(
			 * "ALTER TABLE FRIENDSHIP MODIFY (INVITERID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE FRIENDSHIP MODIFY (INVITEEID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_PK PRIMARY KEY (RID) ENABLE"
			 * ); stmt.executeUpdate(
			 * "ALTER TABLE RESOURCES MODIFY (RID NOT NULL ENABLE)");
			 * stmt.executeUpdate
			 * ("ALTER TABLE RESOURCES MODIFY (CREATORID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE RESOURCES MODIFY (WALLUSERID NOT NULL ENABLE)");
			 * stmt.executeUpdate(
			 * "ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK1 FOREIGN KEY (INVITERID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE FRIENDSHIP ADD CONSTRAINT FRIENDSHIP_USERS_FK2 FOREIGN KEY (INVITEEID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_RESOURCES_FK1 FOREIGN KEY (RID)"
			 * + "REFERENCES RESOURCES (RID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK1 FOREIGN KEY (CREATORID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE MANIPULATION ADD CONSTRAINT MANIPULATION_USERS_FK2 FOREIGN KEY (MODIFIERID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK1 FOREIGN KEY (CREATORID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 * stmt.executeUpdate(
			 * "ALTER TABLE RESOURCES ADD CONSTRAINT RESOURCES_USERS_FK2 FOREIGN KEY (WALLUSERID)"
			 * + "REFERENCES USERS (USERID) ON DELETE CASCADE ENABLE");
			 */
			/*
			 * stmt.executeUpdate(
			 * "CREATE OR REPLACE TRIGGER RINC before insert on resources " +
			 * "for each row " + "WHEN (new.rid is null) begin " +
			 * "select ridInc.nextval into :new.rid from dual;" + "end;");
			 * stmt.executeUpdate("ALTER TRIGGER RINC ENABLE");
			 * 
			 * stmt.executeUpdate(
			 * "CREATE OR REPLACE TRIGGER UINC before insert on users " +
			 * "for each row " + "WHEN (new.userid is null) begin " +
			 * "select useridInc.nextval into :new.userid from dual;" + "end;");
			 * stmt.executeUpdate("ALTER TRIGGER UINC ENABLE");
			 */

			/*
			 * stmt.executeUpdate(analyze table users"); //compute statistics");
			 * stmt.executeUpdate("analyze table resources"); //compute
			 * statistics"); stmt.executeUpdate("analyze table friendship");
			 * //compute statistics");
			 * stmt.executeUpdate("analyze table manipulation")
			 */

			CreateStoredProcs(stmt);

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
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
	public void buildIndexes(Properties props) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			long startIdx = System.currentTimeMillis();

			dropIndex(stmt, "RESOURCE_CREATORID");
			dropIndex(stmt, "RESOURCES_WALLUSERID");
			dropIndex(stmt, "FRIENDSHIP_STATUS");
			dropIndex(stmt, "FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "FRIENDSHIP_INVITERID");
			dropIndex(stmt, "MANIPULATION_RID");
			dropIndex(stmt, "MANIPULATION_CREATORID");
			dropIndex(stmt, "PENDING_FRIENDSHIP_STATUS");
			dropIndex(stmt, "PENDING_FRIENDSHIP_INVITEEID");
			dropIndex(stmt, "PENDING_FRIENDSHIP_INVITERID");

			stmt.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (CREATORID)");
			// + "COMPUTE STATISTICS");//NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (INVITEEID)");
			// + "COMPUTE STATISTICS");// NOLOGGING");

			// stmt.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (RID)");
			// + "COMPUTE STATISTICS");// NOLOGGING");

			// stmt.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (WALLUSERID)");
			// + "COMPUTE STATISTICS");// NOLOGGING");

			stmt.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP (INVITERID)");
			// + "COMPUTE STATISTICS");/// NOLOGGING");

			// stmt.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (STATUS)");
			// + "COMPUTE STATISTICS");// NOLOGGING");

			// stmt.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (CREATORID)");
			// + "COMPUTE STATISTICS");/// NOLOGGING");

			stmt.executeUpdate("CREATE INDEX PENDING_FRIENDSHIP_INVITEEID ON PENDING_FRIENDSHIP (INVITEEID)");
			// stmt.executeUpdate("CREATE INDEX PENDING_FRIENDSHIP_STATUS ON PENDING_FRIENDSHIP (STATUS)");
			stmt.executeUpdate("CREATE INDEX PENDING_FRIENDSHIP_INVITERID ON PENDING_FRIENDSHIP (INVITERID)");

			stmt.executeUpdate("CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE users ON KEY COLUMNS and INDEXES ALL')");
			stmt.executeUpdate("CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE friendship ON KEY COLUMNS and INDEXES ALL')");
			stmt.executeUpdate("CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE manipulation ON KEY COLUMNS and INDEXES ALL')");
			stmt.executeUpdate("CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE resources ON KEY COLUMNS and INDEXES ALL')");
			stmt.executeUpdate("CALL SYSPROC.ADMIN_CMD ('RUNSTATS ON TABLE pending_friendship ON KEY COLUMNS and INDEXES ALL')");

			/*
			 * stmt.executeUpdate(analyze table users"); //compute statistics");
			 * stmt.executeUpdate("analyze table resources"); //compute
			 * statistics"); stmt.executeUpdate("analyze table friendship");
			 * //compute statistics");
			 * stmt.executeUpdate("analyze table manipulation");// compute
			 * statistics");
			 */

			/*
			 * try { //pankaj String query="";
			 * query="CREATE OR REPLACE PROCEDURE ANALYZE_TABLES()"; query+=
			 * "BEGIN DECLARE a integer; select mid into a from manipulation; ";
			 * query+= " RUNSTATS ON table users;";
			 * query+=" select mid into a from manipulation; "; query+="END";
			 * stmt.execute(query);
			 * 
			 * } catch(Exception e) { System.out.println("!!!!exception");
			 * System.out.println(e); }
			 */
			// tbRunstats(conn);

			String Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE PROCEDURE Runstatisctics() ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			// Proc_Code+="  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += " RUNSTATS ON table users;";
			// Proc_Code+=" execute immediate 'RUNSTATS ON table users  WITH DISTRIBUTION ON KEY COLUMNS ';";
			// Proc_Code+=" execute immediate 'insert into test values(12)';";

			// Proc_Code+=" UPDATE friendship SET status = 2 WHERE inviterid= I_inviterID and inviteeid= I_inviteeID; ";
			// Proc_Code+=
			// " update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(I_inviterID);";
			// Proc_Code+=" update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(I_inviteeID); ";
			// Proc_Code+=" update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (I_inviteeID) AND NUM_PEND_FRIENDS!=0;";
			Proc_Code += " ";
			Proc_Code += " END";

			// stmt.execute(Proc_Code);
			tbRunstats(conn);

			CallableStatement cs = null;
			cs = conn.prepareCall("{call Runstatisctics()}");
			// 3cs.executeUpdate();
			cs.close();

			System.out.println("#############################");
			try {
				// stmt.executeUpdate("RUNSTATS ON table users"); //compute
				// statistics");
				// stmt.executeUpdate("RUNSTATS ON table resources"); //compute
				// statistics");
				// stmt.executeUpdate("RUNSTATS ON table friendship"); //compute
				// statistics");
				// stmt.executeUpdate("RUNSTATS ON table manipulation");//
				// compute statistics");
			} catch (Exception ex) {
				System.out.println("\n\n\n\n\n");
				System.out.println("??????????????????exception");
				System.out.println(ex);
				System.out.println("\n\n\n\n\n");
			}
			long endIdx = System.currentTimeMillis();
			System.out.println("Time to build database index structures(ms):"
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

	public int IncrementFriendCounter(int inviterID, String status) {
		int retVal = SUCCESS;
		if (inviterID < 0)
			return -1;
		String query = null;
		if (status.equals("PENDING")) {
			query = "update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS + 1) where USERID= (?)";
		} else if (status.equals("CONFIRMED")) {
			query = "update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(?)";

		}
		try {
			/*
			 * if((preparedStatement = newCachedStatements.get(INVFRND_STMT)) ==
			 * null) preparedStatement = createAndCacheStatement(INVFRND_STMT,
			 * query);
			 */
			// System.out.println(query);
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.executeUpdate();
			// System.out.println("##############################incremented successfully");
		} catch (SQLException sx) {
			retVal = -2;
			// sx.printStackTrace(System.out);
			System.out.println(sx.getMessage());
			// System.out.println("exception for : " + query + "?=" +
			// inviterID);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);

			}
		}
		return retVal;
	}

	public int DecrementFriendCounter(int inviterID, String status) {
		int retVal = SUCCESS;
		if (inviterID < 0)
			return -1;
		String query = null;
		if (status.equals("PENDING")) {
			query = "update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (?) AND NUM_PEND_FRIENDS!=0";
		} else if (status.equals("CONFIRMED")) {
			query = "update USERS set NUM_FRIENDS = (NUM_FRIENDS - 1) where USERID=(?)";
		}

		try {
			/*
			 * if((preparedStatement = newCachedStatements.get(INVFRND_STMT)) ==
			 * null) preparedStatement = createAndCacheStatement(INVFRND_STMT,
			 * query);
			 */
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setInt(1, inviterID);
			preparedStatement.executeUpdate();
			// System.out.println("##############################decremented successfully");
		} catch (SQLException sx) {
			retVal = -2;
			// sx.printStackTrace(System.out);
			System.out.println(sx.getMessage());
			// System.out.println("Pending friends 1counter incremented for " +
			// inviterID);
		} finally {
			try {
				if (preparedStatement != null)
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
		// System.out.println("listFriends");

		int retVal = SUCCESS;
		String uid = "0";
		ResultSet rs = null;
		if (requesterID < 0 || profileOwnerID < 0)
			return -1;

		String query = "";

		try {
			if (insertImage && FSimagePath.equals("")) {
				// query =
				// "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviteeid=? and userid=inviterid)) and status = 2";
				// query =
				// "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users inner join friendship on ((inviteeid=? and userid=inviterid)) and status = 2";

				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users inner join friendship on ((inviteeid=? and userid=inviterid))";

				// query=" SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users inner join (select inviterid,inviteeid  from friendship where inviteeid=?) on  userid=inviterid";

				if ((preparedStatement = newCachedStatements
						.get(GETFRNDSIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(
							GETFRNDSIMG_STMT, query);

			} else {
				// query =
				// "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users inner join friendship on ( (inviteeid=? and userid=inviterid)) and status = 2";

				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users inner join friendship on ( (inviteeid=? and userid=inviterid))";

				// query=" SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users inner join (select inviterid,inviteeid  from friendship where inviteeid=?) on  userid=inviterid";

				if ((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETFRNDS_STMT,
							query);
			}
			preparedStatement.setInt(1, profileOwnerID);
			// preparedStatement.setInt(2, profileOwnerID);
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
						values.put(field,
								new ObjectByteIterator(value.getBytes()));
					}
					result.add(values);
				} else {
					// get the number of columns and their names
					ResultSetMetaData md = rs.getMetaData();
					int col = md.getColumnCount();
					for (int i = 1; i <= col; i++) {
						String col_name = md.getColumnName(i);
						String value = "";
						if (col_name.equalsIgnoreCase("tpic")) {
							// Get as a BLOB
							Blob aBlob = rs.getBlob(col_name);
							byte[] allBytesInBlob = aBlob.getBytes(1,
									(int) aBlob.length());
							if (testMode) {
								// dump to file
								try {
									FileOutputStream fos = new FileOutputStream(
											profileOwnerID + "-" + cnt
													+ "-thumbimage.bmp");
									fos.write(allBytesInBlob);
									fos.close();
								} catch (Exception ex) {
								}
							}
							values.put(col_name, new ObjectByteIterator(
									allBytesInBlob));
						} else {
							value = rs.getString(col_name);
							if (col_name.equalsIgnoreCase("userid")) {
								uid = value;
								col_name = "userid";
							}
							values.put(col_name,
									new ObjectByteIterator(value.getBytes()));
						}

					}
					// Fetch the thumbnail image from the file system
					if (insertImage && !FSimagePath.equals("")) {
						byte[] thumbImage = GetImageFromFS(uid, false);
						// Get the thumbnail image from the file
						if (testMode) {
							// dump to file
							try {
								FileOutputStream fos = new FileOutputStream(
										profileOwnerID + "-" + cnt
												+ "-thumbimage.bmp");
								fos.write(thumbImage);
								fos.close();
							} catch (Exception ex) {
							}
						}
						values.put("tpic", new ObjectByteIterator(thumbImage));
					}
					result.add(values);
				}
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
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
			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
			boolean testMode) {
		// System.out.println("viewFriendReq");
		int retVal = SUCCESS;
		ResultSet rs = null;
		if (profileOwnerID < 0)
			return -1;

		String query = "";
		String uid = "0";
		try {
			if (insertImage && FSimagePath.equals("")) {
				// query =
				// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				// query =
				// "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users inner join friendship on inviterid = userid and inviteeid=? and status = 1 ";
				query = "SELECT userid, inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users inner join pending_friendship on (inviterid = userid and inviteeid=?)";
				if ((preparedStatement = newCachedStatements
						.get(GETPENDIMG_STMT)) == null)
					preparedStatement = createAndCacheStatement(
							GETPENDIMG_STMT, query);

			} else {
				// query =
				// "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE inviteeid=? and status = 1 and inviterid = userid";
				query = "SELECT userid,inviterid, inviteeid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users inner join pending_friendship on (inviterid = userid and inviteeid=?)";
				if ((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
					preparedStatement = createAndCacheStatement(GETPEND_STMT,
							query);
			}
			preparedStatement.setInt(1, profileOwnerID);
			rs = preparedStatement.executeQuery();
			int cnt = 0;
			while (rs.next()) {
				cnt++;
				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = "";
					if (col_name.equalsIgnoreCase("tpic")) {
						// Get as a BLOB
						Blob aBlob = rs.getBlob(col_name);
						byte[] allBytesInBlob = aBlob.getBytes(1,
								(int) aBlob.length());
						if (testMode) {
							// dump to file
							try {
								FileOutputStream fos = new FileOutputStream(
										profileOwnerID + "-" + cnt
												+ "-thumbimage.bmp");
								fos.write(allBytesInBlob);
								fos.close();
							} catch (Exception ex) {
							}

						}
						values.put(col_name, new ObjectByteIterator(
								allBytesInBlob));
					} else {
						value = rs.getString(col_name);
						if (col_name.equalsIgnoreCase("userid")) {
							uid = value;
							col_name = "userid";
						}
						values.put(col_name,
								new ObjectByteIterator(value.getBytes()));
					}

				}
				// Fetch the thumbnail image from the file system
				if (insertImage && !FSimagePath.equals("")) {
					byte[] thumbImage = GetImageFromFS(uid, false);
					// Get the thumbnail image from the file
					if (testMode) {
						// dump to file
						try {
							FileOutputStream fos = new FileOutputStream(
									profileOwnerID + "-" + cnt
											+ "-thumbimage.bmp");
							fos.write(thumbImage);
							fos.close();
						} catch (Exception ex) {
						}
					}
					values.put("tpic", new ObjectByteIterator(thumbImage));
				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
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
		if (inviterID < 0 || inviteeID < 0)
			return -1;

		String query = "DELETE FROM friendship WHERE inviterid=? and inviteeid= ? and status=1";
		CallableStatement cs = null;
		try {
			/*
			 * if((preparedStatement = newCachedStatements.get(REJREQ_STMT)) ==
			 * null) preparedStatement = createAndCacheStatement(REJREQ_STMT,
			 * query);
			 * 
			 * 
			 * preparedStatement.setInt(1, inviterID);
			 * preparedStatement.setInt(2, inviteeID);
			 * 
			 * conn.setAutoCommit(false); preparedStatement.executeUpdate();
			 * DecrementFriendCounter(inviteeID,"PENDING"); conn.commit();
			 * conn.setAutoCommit(true);
			 */

			cs = conn.prepareCall("{call rejectFriend(?,?,?)}");
			cs.setString(1, Integer.toString(inviterID));
			cs.setString(2, Integer.toString(inviteeID));
			cs.registerOutParameter(3, Types.VARCHAR);
			cs.executeUpdate();
			String Status = cs.getString(3);
			if (!Status.equalsIgnoreCase("S"))
				System.out.println("??????????????????????????rejectFriend= "
						+ Status);
			// cs.close();

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				cs.close();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}
		return retVal;
	}

	@Override
	public int thawFriendship(int friendid1, int friendid2) {
		int retVal = SUCCESS;
		if (friendid1 < 0 || friendid2 < 0)
			return -1;
		CallableStatement cs = null;
		// incorrect query for 1 table , 2 records String query =
		// "DELETE FROM friendship WHERE (inviterid=? and inviteeid= ?) OR (inviterid=? and inviteeid= ?) and status=2";
		try {
			/*
			 * if((preparedStatement = newCachedStatements.get(UNFRNDFRND_STMT))
			 * == null) preparedStatement =
			 * createAndCacheStatement(UNFRNDFRND_STMT, query);
			 * preparedStatement.setInt(1, friendid1);
			 * preparedStatement.setInt(2, friendid2);
			 * preparedStatement.setInt(3, friendid2);
			 * preparedStatement.setInt(4, friendid1);
			 * 
			 * conn.setAutoCommit(false);
			 * if(preparedStatement.executeUpdate()>0) { //Decrement total
			 * number of friendships in USERS table
			 * DecrementFriendCounter(friendid1,"CONFIRMED");
			 * DecrementFriendCounter(friendid2,"CONFIRMED"); } conn.commit();
			 * conn.setAutoCommit(true);
			 */

			cs = conn.prepareCall("{call thawFriendship(?,?,?)}");
			cs.setString(1, Integer.toString(friendid1));
			cs.setString(2, Integer.toString(friendid2));
			cs.registerOutParameter(3, Types.VARCHAR);
			cs.executeUpdate();

			String Status = cs.getString(3);
			if (!Status.equalsIgnoreCase("S"))
				System.out.println("??????????????????????????thawFriendship= "
						+ Status);
			// cs.close();

		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
				cs.close();
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
		// System.out.println("viewTopKResources");
		int retVal = SUCCESS;
		ResultSet rs = null;
		if (profileOwnerID < 0 || requesterID < 0 || k < 0)
			return -1;
		// //System.out.println("Here before Select");
		// MYSQL////String query =
		// "SELECT * FROM resources WHERE walluserid = ? ORDER BY rid desc LIMIT ?";

		// /String query =
		// "SELECT * FROM resources WHERE walluserid = ? ORDER BY rid desc ";
		// //query+=" fetch first ? rows only";

		String query = " select r.* from resources r, (select rs.rid res_id ,ROW_NUMBER() OVER(order by rs.rid) as row_num from resources rs where rs.walluserid =?) where r.rid=res_id and  row_num <=?";
		// String query =
		// "SELECT * FROM resources WHERE walluserid = ? ORDER BY rid desc fetch first ? rows only";

		try {
			if ((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETTOPRES_STMT,
						query);

			preparedStatement.setInt(1, profileOwnerID);
			preparedStatement.setInt(2, k);

			rs = preparedStatement.executeQuery();

			while (rs.next()) {

				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					if (col_name.equalsIgnoreCase("rid"))
						col_name = "rid";
					else if (col_name.equalsIgnoreCase("walluserid"))
						col_name = "walluserid";
					else if (col_name.equalsIgnoreCase("creatorid"))
						col_name = "creatorid";
					values.put(col_name,
							new ObjectByteIterator(value.getBytes()));

				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
					preparedStatement.clearParameters();

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
		if (profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
			return -1;
		String query;
		// get comment cnt
		HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
		try {
			query = "SELECT * FROM manipulation WHERE rid = ?";
			if ((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(GETRESCMT_STMT,
						query);
			preparedStatement.setInt(1, resourceID);
			rs = preparedStatement.executeQuery();
			while (rs.next()) {
				values = new HashMap<String, ByteIterator>();
				// get the number of columns and their names
				ResultSetMetaData md = rs.getMetaData();
				int col = md.getColumnCount();
				for (int i = 1; i <= col; i++) {
					String col_name = md.getColumnName(i);
					String value = rs.getString(col_name);
					values.put(col_name,
							new ObjectByteIterator(value.getBytes()));
				}
				result.add(values);
			}
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (preparedStatement != null)
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
			int resourceID, HashMap<String, ByteIterator> commentValues) {
		int retVal = SUCCESS;

		if (profileOwnerID < 0 || commentCreatorID < 0 || resourceID < 0)
			return -1;

		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?,?,?, ?,?, ?, ?)";
		try {
			if ((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);
			preparedStatement.setInt(1,
					Integer.parseInt(commentValues.get("mid").toString()));
			preparedStatement.setInt(2, profileOwnerID);
			preparedStatement.setInt(3, resourceID);
			preparedStatement.setInt(4, commentCreatorID);
			preparedStatement.setString(5, commentValues.get("timestamp")
					.toString());
			preparedStatement
					.setString(6, commentValues.get("type").toString());
			preparedStatement.setString(7, commentValues.get("content")
					.toString());
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
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

		if (resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
			return -1;

		String query = "DELETE FROM manipulation WHERE mid=? AND rid=?";
		try {
			if ((preparedStatement = newCachedStatements.get(DELCMT_STMT)) == null)
				preparedStatement = createAndCacheStatement(DELCMT_STMT, query);
			preparedStatement.setInt(1, manipulationID);
			preparedStatement.setInt(2, resourceID);
			preparedStatement.executeUpdate();
		} catch (SQLException sx) {
			retVal = -2;
			sx.printStackTrace(System.out);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {
				retVal = -2;
				e.printStackTrace(System.out);
			}
		}

		return retVal;

	}

	public void update_resource_count(String userId, String operation) {

		String query = null;
		try {
			if (operation.equals("INCREMENT")) {
				query = "update USERS set NUM_RESOURCES =(NUM_RESOURCES + 1) where USERID= (?)";

				if ((preparedStatement = newCachedStatements.get(INC_RES_STMT)) == null)
					preparedStatement = createAndCacheStatement(INC_RES_STMT,
							query);

				preparedStatement.setString(1, userId);
				preparedStatement.executeUpdate();
			} else if (operation.equals("DECREMENT")) {
				query = "update USERS set NUM_RESOURCES = (NUM_RESOURCES - 1) where USERID=(?) AND NUM_RESOURCES>0";

				if ((preparedStatement = newCachedStatements.get(DEC_RES_STMT)) == null)
					preparedStatement = createAndCacheStatement(DEC_RES_STMT,
							query);

				preparedStatement.setString(1, userId);
				preparedStatement.executeUpdate();

			}

		} catch (SQLException sx) {

			sx.printStackTrace(System.out);
			// System.out.println("Pending friends counter incremented for " +
			// inviterID);
		} finally {
			try {
				if (preparedStatement != null)
					preparedStatement.clearParameters();
			} catch (SQLException e) {

				e.printStackTrace(System.out);
			}
		}

	}

	private void CreateStoredProcs(Statement stmt) {
		try {

			// System.out.println("Inside CreateStoredProcs");
			String Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE PROCEDURE acceptFriend(IN I_inviterID VARCHAR(200),IN I_inviteeID VARCHAR(100), OUT O_SUCCESS VARCHAR(1)) ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += " delete from pending_friendship WHERE inviterid= I_inviterID and inviteeid= I_inviteeID; ";
			Proc_Code += " INSERT into friendship (inviterid,inviteeid,status) values (I_inviteeID,I_inviterID,2);";
			Proc_Code += " INSERT into friendship (inviterid,inviteeid,status) values (I_inviterID,I_inviteeID,2);";
			Proc_Code += " update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(I_inviterID);";
			Proc_Code += " update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(I_inviteeID); ";
			Proc_Code += " update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (I_inviteeID) AND NUM_PEND_FRIENDS!=0;";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE  PROCEDURE  inviteFriend(IN I_inviterID VARCHAR(200),IN I_inviteeID VARCHAR(100), OUT O_SUCCESS VARCHAR(1)) ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += "INSERT INTO pending_friendship(inviterid,inviteeid,status) values(I_inviterID,I_inviteeID,1);";
			Proc_Code += "UPDATE USERS set NUM_PEND_FRIENDS=(NUM_PEND_FRIENDS +1) where USERID=I_inviteeID;";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE  PROCEDURE  CreateFriendship(IN memberA VARCHAR(200),IN memberB VARCHAR(100), OUT O_SUCCESS VARCHAR(1)) ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += " INSERT INTO friendship values(memberA,memberB,2);";
			Proc_Code += " INSERT INTO friendship values(memberB,memberA,2);";
			Proc_Code += " update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (memberA) AND NUM_PEND_FRIENDS!=0;";
			Proc_Code += " update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (memberB) AND NUM_PEND_FRIENDS!=0;";
			Proc_Code += " update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(memberA);";
			Proc_Code += " update USERS set NUM_FRIENDS = (NUM_FRIENDS + 1) where USERID=(memberB);";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE  PROCEDURE  rejectFriend(IN I_inviterID VARCHAR(200),IN I_inviteeID VARCHAR(100), OUT O_SUCCESS VARCHAR(1)) ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += "DELETE FROM pending_friendship WHERE inviterid=I_inviterID and inviteeid= I_inviteeID and status=1;";
			Proc_Code += " update USERS set NUM_PEND_FRIENDS =(NUM_PEND_FRIENDS - 1) where USERID= (I_inviteeID) AND NUM_PEND_FRIENDS!=0;";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE  PROCEDURE  thawFriendship(IN friendid1 VARCHAR(200),IN friendid2 VARCHAR(100), OUT O_SUCCESS VARCHAR(1)) ";
			Proc_Code += " language sql ";
			Proc_Code += "  BEGIN ";
			Proc_Code += " DECLARE num_rows int default 0;";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += " SELECT count(*)/2 into num_rows FROM friendship WHERE (inviterid=friendid1 and inviteeid= friendid2) OR (inviterid=friendid2 and inviteeid= friendid1);";
			Proc_Code += " DELETE FROM friendship WHERE (inviterid=friendid1 and inviteeid= friendid2) OR (inviterid=friendid2 and inviteeid= friendid1);";
			Proc_Code += "if (num_rows > 0 ) then update USERS set NUM_FRIENDS = (NUM_FRIENDS - 1) where USERID=(friendid1); update USERS set NUM_FRIENDS = (NUM_FRIENDS - 1) where USERID=(friendid2); end if;";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			Proc_Code = "";
			Proc_Code = "CREATE OR REPLACE  PROCEDURE INSERTENTITY(IN Query varchar(5000),IN WalluserId varchar(200),IN EntitySet varchar(20) ,OUT O_SUCCESS VARCHAR(1) )  ";
			Proc_Code += " language sql ";
			Proc_Code += " BEGIN ";
			Proc_Code += "  DECLARE EXIT HANDLER FOR SQLEXCEPTION  begin SET O_SUCCESS = 'F'; ROLLBACK; end; ";
			Proc_Code += " PREPARE s1 from  Query ; execute s1;";
			// Proc_Code+=" if(compare(EntitySet,'resources')=0) then ";
			Proc_Code += " if (EntitySet='resources') then ";
			Proc_Code += " update USERS set NUM_RESOURCES =(NUM_RESOURCES + 1) where USERID= (WalluserId);";
			Proc_Code += " end if;";
			Proc_Code += " SET O_SUCCESS ='S'; COMMIT;";
			Proc_Code += " END";

			stmt.execute(Proc_Code);

			/*
			 * Proc_Code=
			 * "create or replace procedure proctest(in i varchar(10),out o varchar(1)) "
			 * ; Proc_Code+=
			 * "language sql  begin  DECLARE EXIT HANDLER FOR SQLEXCEPTION ";
			 * Proc_Code
			 * +=" begin SET o = 'F'; end; update users set userid=10000 ;  end "
			 * ;
			 */
			// System.out.println("proc created");

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}


}
