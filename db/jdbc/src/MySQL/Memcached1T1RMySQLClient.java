//package MySQL;
//
//import java.io.DataInputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.UnsupportedEncodingException;
//import java.sql.Blob;
//import java.sql.Date;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.ResultSetMetaData;
//import java.sql.SQLException;
//import java.util.HashMap;
//import java.util.Map.Entry;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.Semaphore;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.zip.DataFormatException;
//import java.util.Properties;
//import java.util.Random;
//import java.util.Set;
//import java.util.Vector;
//
//import memcached.StartProcess;
//
//import org.apache.log4j.PropertyConfigurator;
//
//import com.meetup.memcached.MemcachedClient;
//import com.meetup.memcached.SockIOPool;
//import com.mysql.jdbc.Connection;
//import com.mysql.jdbc.PreparedStatement;
//import com.mysql.jdbc.Statement;
//import common.CacheUtilities;
//
//import edu.usc.bg.base.ByteIterator;
//import edu.usc.bg.base.Client;
//import edu.usc.bg.base.DB;
//import edu.usc.bg.base.DBException;
//import edu.usc.bg.base.ObjectByteIterator;
//
//public class Memcached1T1RMySQLClient extends DB implements MySQLConstants{
//
//	/** The code to return when the call succeeds. **/
//	public static final int SUCCESS = 0;
//	/** The code to return when the call fails. **/
//	public static final int ERROR   = -1;
//
//	private static String FSimagePath = "";
//
//	
//	private Properties props;
//	private boolean initialized = false;
//	com.mysql.jdbc.Connection conn=null;
//	private static final String DEFAULT_PROP = "";
//	Statement st = null;
//    ResultSet rs = null;
//    private static boolean ManageCache = true;
//	private static boolean verbose = false;
//	private static final int CACHE_START_WAIT_TIME = 10000;
//	private ConcurrentMap<Integer, PreparedStatement> newCachedStatements;
//	private PreparedStatement preparedStatement = null;
//	StartProcess star;
//	//String cache_cmd = "C:\\PSTools\\psexec \\\\"+COSARServer.cacheServerHostname+" -u shahram -p 2Shahram C:\\cosar\\configurable\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
//	//String cache_cmd_stop = "java -jar C:\\BG\\ShutdownCOSAR.jar";
//	
//	//private static Vector<MemcachedClient> cacheclient_vector = new Vector<MemcachedClient>();
//	SockIOPool cacheConnectionPool;
//	private MemcachedClient cacheclient = null;
//	private static String cache_hostname = "";
//	private static Integer cache_port = -1;
//	private boolean isInsertImage;
//	
//	private static final int MAX_NUM_RETRIES = 10;
//	private static final int TIMEOUT_WAIT_MILI = 100;
//	
//	//String cache_cmd = "C:\\cosar\\conficacheclientle\\TCache.NetworkInterface.exe C:\\cosar\\configurable\\V2gb.xml ";
//	public static final int CACHE_POOL_NUM_CONNECTIONS = 400;
//	
//	private static AtomicInteger NumThreads = null;
//	private static Semaphore crtcl = new Semaphore(1, true);
//
//
//	private static final double TTL_RANGE_PERCENT = 0.2; 
//	private boolean useTTL = false;
//	private int TTLvalue = 0;
//	private boolean compressPayload = false;
//    	
//	
//	private static int incrementNumThreads() {
//        int v;
//        do {
//            v = NumThreads.get();
//        } while (!NumThreads.compareAndSet(v, v + 1));
//        return v + 1;
//    }
//    
//	private String getCacheCmd()
//	{		
//		return "C:\\PSTools\\psexec \\\\"+cache_hostname+" -u Administrator -p change C:\\Users\\parin\\Downloads\\mem\\memcached\\memcached.exe -d start ";
//	}
//	
//	private String getCacheStopCmd()
//	{
//		return "C:\\PSTools\\psexec \\\\"+cache_hostname+" -u Administrator -p change C:\\Users\\parin\\Downloads\\mem\\memcached\\memcached.exe -d stop ";
//	}
//	
//	
//	public boolean init() throws DBException
//	{
//		if (initialized) {
//			System.out.println("Client connection already initialized.");
//			return true;
//		}
//		props = getProperties();
//		String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
//		String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
//		String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
//		cache_hostname = props.getProperty(MEMCACHED_SERVER_HOST, MEMCACHED_SERVER_HOST_DEFAULT);
//		cache_port = Integer.parseInt(props.getProperty(MEMCACHED_SERVER_PORT,MEMCACHED_SERVER_PORT_DEFAULT));
//		//ManageCache=true;
//		ManageCache = Boolean.parseBoolean(
//				props.getProperty(MANAGE_CACHE_PROPERTY, 
//						MANAGE_CACHE_PROPERTY_DEFAULT));
//		
//		isInsertImage = Boolean.parseBoolean(props.getProperty(Client.INSERT_IMAGE_PROPERTY, Client.INSERT_IMAGE_PROPERTY_DEFAULT));
//		TTLvalue = Integer.parseInt(props.getProperty(TTL_VALUE, TTL_VALUE_DEFAULT));
//		useTTL = (TTLvalue != 0);
//		
//		compressPayload = Boolean.parseBoolean(props.getProperty(ENABLE_COMPRESSION_PROPERTY, ENABLE_COMPRESSION_PROPERTY_DEFAULT));
//		
//		try {
//			System.out.println("Connecting to DB..");
//			conn =(Connection) DriverManager.getConnection(urls, user,passwd);
//			System.out.println(", connected");
//
//		}
//		catch(SQLException e){
//			System.out.println("error while connecting to DB: "+ e.toString());
//			e.printStackTrace();
//			return false;
//			
//		}
//		
//		// memcache
//		
//		try {
//			crtcl.acquire();
//			
//			if(NumThreads == null)
//			{
//				NumThreads = new AtomicInteger();
//				NumThreads.set(0);
//			}
//			
//			incrementNumThreads();
//			
//		}catch (Exception e){
//			System.out.println("SQLTrigQR init failed to acquire semaphore.");
//			e.printStackTrace(System.out);
//		}
//		if (initialized) {
//			cacheclient = new MemcachedClient();
//			//cacheclient_vector.add(cacheclient);
//			
//			crtcl.release();
//			System.out.println("Client connection already initialized.");
//			return true;
//		}
//		
//		//useTTL = Integer.parseInt(props.getProperty("ttlvalue", "0")) != 0;
//		
//		Properties prop = new Properties();
//		prop.setProperty("log4j.rootLogger", "ERROR, A1");
//		//prop.setProperty("log4j.appender.A1", "org.apache.log4j.ConsoleAppender");
//		prop.setProperty("log4j.appender.A1", "org.apache.log4j.FileAppender");
//		prop.setProperty("log4j.appender.A1.File", "whalincache.out");
//		prop.setProperty("log4j.appender.A1.Append", "false");
//		prop.setProperty("log4j.appender.A1.layout", "org.apache.log4j.PatternLayout");
//		prop.setProperty("log4j.appender.A1.layout.ConversionPattern", "%-4r %-5p [%t] %37c %3x - %m%n");
//		PropertyConfigurator.configure(prop);
//		
//		//String[] servers = { "192.168.1.1:1624", "192.168.1.1:1625" };
//		String[] servers = { cache_hostname + ":" + cache_port };
//		cacheConnectionPool = SockIOPool.getInstance();
//		cacheConnectionPool.setServers( servers );
//		cacheConnectionPool.setFailover( true );
//		cacheConnectionPool.setInitConn( 10 ); 
//		cacheConnectionPool.setMinConn( 5 );
//		cacheConnectionPool.setMaxConn( CACHE_POOL_NUM_CONNECTIONS );
//		cacheConnectionPool.setMaintSleep( 30 );
//		cacheConnectionPool.setNagle( false );
//		cacheConnectionPool.setSocketTO( 3000 );
//		cacheConnectionPool.setAliveCheck( true );
//		cacheConnectionPool.initialize();
//		
//		if (ManageCache) {
//			System.out.println("Starting Cache: "+this.getCacheCmd());
//			//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
//		//	this.star = new StartProcess(this.getCacheCmd(), "cache_output.txt");
//		//	this.star.start();
//			
//			System.out.println("Wait for "+CACHE_START_WAIT_TIME/1000+" seconds to allow Cache to startup.");
//			try{
//			//	Thread.sleep(CACHE_START_WAIT_TIME);
//			}catch(Exception e)
//			{
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		
//
//		
//		
////		builder = new MemcachedClientBuilder( 
////				AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
////		builder.setConnectionPoolSize(CACHE_POOL_NUM_CONNECTIONS);
//
//		initialized = true;
//		try {
//			cacheclient = new MemcachedClient();
//			//cacheclient_vector.add(cacheclient);
//						
//			crtcl.release();
//		} catch (Exception e) {
//			System.out.println("MemcacheClient init failed to release semaphore.");
//			e.printStackTrace(System.out);
//			return false;
//		}
//		return true;
//		
//	}
//	
//	
//	public void cleanup(boolean warmup) throws DBException
//	{
//		try{
//			conn.close();
//			System.out.println("connection is closed");
//			
//			System.out.println("Stopping Cache: "+this.getCacheStopCmd());
//			//this.st = new StartCOSAR(this.cache_cmd + (RaysConfig.cacheServerPort + i), "cache_output" + i + ".txt"); 
//			star = new StartProcess(this.getCacheStopCmd(), "cache_output.txt");
//			star.start();
//			System.out.print("Waiting for Cache to finish.");
//			
//		}
//		catch(SQLException e){
//			e.printStackTrace();
//			}
//	}
//
//	public void buildIndexes(Properties props){
//		try {
//			st = (Statement) conn.createStatement();
//		} catch (SQLException e) {
//			System.out.println( "Error while creating a statement: "+ e.toString() );
//			e.printStackTrace();
//		}
//
//		try {
//			
//			st.executeUpdate("DROP PROCEDURE IF EXISTS sp_DropIndex");
//
////			
//			st.executeUpdate("CREATE PROCEDURE sp_DropIndex (tblSchema VARCHAR(64), tblName VARCHAR(64),ndxName VARCHAR(64))\n"+
//			"BEGIN\n"+
//			"DECLARE IndexColumnCount INT;\n"+
//			"DECLARE SQLStatement VARCHAR(256);\n"+
//			"SELECT  COUNT(1) INTO IndexColumnCount "+
//			"FROM information_schema.statistics "+
//			 "WHERE table_schema = tblSchema AND table_name = tblName AND index_name = ndxName;\n"+
//			"IF IndexColumnCount > 0 THEN "+
//			"SET SQLStatement = CONCAT('ALTER TABLE `',tblSchema,'`.`',tblName,'` DROP INDEX `',ndxName,'`');"+
//			"SET @SQLStmt = SQLStatement;\n"+
//			"PREPARE s FROM @SQLStmt;\nEXECUTE s;\nDEALLOCATE PREPARE s;\nEND IF;\nEND");
//			
//			
//			st.executeUpdate("call sp_DropIndex ('hello_new','RESOURCES','RESOURCE_CREATORID');");
//			st.executeUpdate("call sp_DropIndex ('hello_new','FRIENDSHIP','FRIENDSHIP_INVITEEID');");
//			st.executeUpdate("call sp_DropIndex ('hello_new','MANIPULATION','MANIPULATION_RID');");
//			st.executeUpdate("call sp_DropIndex ('hello_new','RESOURCES','RESOURCES_WALLUSERID');");
//			st.executeUpdate("call sp_DropIndex ('hello_new','FRIENDSHIP','FRIENDSHIP_STATUS');");
//			st.executeUpdate("call sp_DropIndex ('hello_new','MANIPULATION','MANIPULATION_CREATORID');");
//			
//			/*8
//			st.executeUpdate("DROP INDEX RESOURCE_CREATORID ON RESOURCES");
//			st.executeUpdate("DROP INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP");
//			st.executeUpdate("DROP INDEX MANIPULATION_RID ON MANIPULATION");
//			st.executeUpdate("DROP INDEX RESOURCES_WALLUSERID ON RESOURCES");
//			st.executeUpdate("DROP INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP");
//			st.executeUpdate("DROP INDEX FRIENDSHIP_STATUS ON FRIENDSHIP");
//			st.executeUpdate("DROP INDEX MANIPULATION_CREATORID ON MANIPULATION");
//		*/
//			System.out.println("hetee");
//			st.executeUpdate("CREATE INDEX RESOURCE_CREATORID ON RESOURCES (creatorid)");
//			st.executeUpdate("CREATE INDEX FRIENDSHIP_INVITEEID ON FRIENDSHIP (userid1, userid2)");
//			st.executeUpdate("CREATE INDEX MANIPULATION_RID ON MANIPULATION (rid)");
//			st.executeUpdate("CREATE INDEX RESOURCES_WALLUSERID ON RESOURCES (wallUserId)");
////			st.executeUpdate("CREATE INDEX FRIENDSHIP_INVITERID ON FRIENDSHIP");	
//			st.executeUpdate("CREATE INDEX FRIENDSHIP_STATUS ON FRIENDSHIP (status)");		
//			st.executeUpdate("CREATE INDEX MANIPULATION_CREATORID ON MANIPULATION (creatorid)");
//		} catch (Exception e) {
//			e.printStackTrace(System.out);
//		} finally {
//			try {
//				if (st != null)
//					st.close();
//			} catch (SQLException e) {
//				e.printStackTrace(System.out);
//			}
//		}
//	}
//	
//	@Override
//	public int insertEntity(String entitySet, String entityPK,
//			HashMap<String, ByteIterator> values, boolean insertImage) {
//		// TODO Auto-generated method stub
//		if (entitySet == null) {
//			return -1;
//		}
//		if (entityPK == null) {
//			return -1;
//		}
//		ResultSet rs =null;
//		try {
//			String query="";
//			int numFields = values.size()+1;
//			if(entitySet.equalsIgnoreCase("users") && insertImage){	
//				query = "INSERT INTO "+entitySet+" VALUES (";
//				for(int j=1; j<=numFields; j++){
//					if(j==(numFields)){
//						query+="?)";
//						break;
//					}
//					else
//						query+="?,";
//				}
//			}
//			else if(entitySet.equalsIgnoreCase("users") && !insertImage){
//				query = "INSERT INTO "+entitySet+" VALUES (";
//				for(int j=1; j<=numFields; j++){
//					if(j==(numFields)){
//						query+="?, null,null)";
//						break;
//					}
//					else
//						query+="?,";
//				}
//			}
//			else if(entitySet.equalsIgnoreCase("resources")){
//				query = "CALL createResource_procedure (";
//				for(int j=1; j<=numFields; j++){
//					if(j==(numFields)){
//						query+="?)";
//						break;
//					}
//					else
//						query+="?,";
//				}
//			
//			}
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setString(1, entityPK);
//			int cnt=2;
//			for (Entry<String, ByteIterator> entry : values.entrySet()) {
//				String field = entry.getValue().toString();	
//				preparedStatement.setString(cnt, field);
//				cnt++;
//			}
//			
//			preparedStatement.executeUpdate();
//			
//		} catch (SQLException e) {
//			System.out.println("Error in processing insert to table: " + entitySet + e);
//			return -1;
//		
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.close();
//			} catch (SQLException e) {
//				e.printStackTrace(System.out);
//			}
//		}
//
//		return 0;
//	}
//
//	private PreparedStatement createAndCacheStatement(int stmttype, String query) throws SQLException{
//		PreparedStatement newStatement = (PreparedStatement) conn.prepareStatement(query);
//		PreparedStatement stmt = newCachedStatements.putIfAbsent(stmttype, newStatement);
//		if (stmt == null) return newStatement;
//		else return stmt;
//	}
//
//	
//	private byte[] GetImageFromFS(String userid, boolean profileimg) {
//        int filelength = 0;
//        String text = "thumbnail";
//        byte[] imgpayload = null;
//       
//        if (profileimg) text = "profile";
//       
//        String ImageFileName = FSimagePath + File.separator + "img" + userid + text;
//        int attempt = 100;
//        while (attempt > 0) {
//	         try {
//	        	 FileInputStream fis = null;
//	        	 DataInputStream dis = null;
//	        	 File fsimage = new File(ImageFileName); 
//	             filelength = (int) fsimage.length();
//	             imgpayload = new byte[filelength];
//	             fis = new FileInputStream(fsimage);
//	             dis = new DataInputStream(fis);
//	             int read = 0;
//	             int numRead = 0;
//	             while (read < filelength && (numRead = dis.read(imgpayload, read, filelength - read)) >= 0) {
//	            	 read = read + numRead;
//	             }
//	             dis.close();
//	             fis.close();
//	             break;
//	         } catch (IOException e) {
//	             e.printStackTrace(System.out);
//	        	 --attempt;
//	         }
//       }
//        
//       return imgpayload;
//	}
//	
//	
//	@Override
//	public int viewProfile(int requesterID, int profileOwnerID,
//			HashMap<String, ByteIterator> result, boolean insertImage,
//			boolean testMode) {
//		if (verbose) System.out.print("Get Profile "+requesterID+" "+profileOwnerID);
//		ResultSet rs = null;
//		int retVal = SUCCESS;
//		if(requesterID < 0 || profileOwnerID < 0)
//			return -1;
//
//		byte[] payload;
//		String key;
//		String query="";
//
//		//MemcachedClient cacheclient=null;
//		HashMap<String, ByteIterator> SR = new HashMap<String, ByteIterator>(); 
//
//		// Initialize query logging for the send procedure
//		//cacheclient.startQueryLogging();
//		
//		// Check Cache first
//		if(insertImage)
//		{
//			key = "profile"+profileOwnerID;
//		}
//		else
//		{
//			key = "profileNoImage"+profileOwnerID;
//		}
//		
//		if(requesterID == profileOwnerID)
//		{
//			key = "own" + key;
//		}
//		
//		try {
//			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//			payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
//			if (payload != null && CacheUtilities.unMarshallHashMap(result, payload)){
//				if (verbose) System.out.println("... Cache Hit!");
//					
//				//cacheclient.shutdown();
//				return retVal;
//			} else if (verbose) System.out.println("... Query DB.");
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		} 
//			
//
//		try {
//			if (verbose) System.out.print("... Query DB!");
//
//			query = "SELECT count(*) FROM  friendship WHERE (userid1 = ? OR userid2 = ?) AND status = 2 ";
//			//query = "SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ";
//			/*##############changed
//			 * if((preparedStatement = newCachedStatements.get(GETFRNDCNT_STMT)) == null)
//				preparedStatement = createAndCacheStatement(GETFRNDCNT_STMT, query);
//			*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			preparedStatement.setInt(2, profileOwnerID);
//			//cacheclient.addQuery("SELECT count(*) FROM  friendship WHERE (inviterID = "+profileOwnerID+" OR inviteeID = "+profileOwnerID+") AND status = 2 ");
//
//			rs = preparedStatement.executeQuery();
//
//			String Value="0";
//			if (rs.next()){
//				Value = rs.getString(1);
//				result.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//			}else
//				result.put("friendcount", new ObjectByteIterator("0".getBytes())) ;
//
//			//serialize the result hashmap and insert it in the cache for future use
//			SR.put("friendcount", new ObjectByteIterator(rs.getString(1).getBytes()));
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//			} catch (SQLException e) {
//				e.printStackTrace(System.out);
//				retVal = -2;
//			}
//		}
//
//		//pending friend request count
//		//if owner viwing her own profile, she can view her pending friend requests
//		if(requesterID == profileOwnerID){
//
//			try {
//				//###########changed
//				query = "SELECT count(*) FROM  friendship WHERE (userid1 = ? or userid2=?) AND status = 1 ";
//				//preparedStatement = conn.prepareStatement(query);
//				/*
//				if((preparedStatement = newCachedStatements.get(GETPENDCNT_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETPENDCNT_STMT, query);*/
//				preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//				//#########changed
//				preparedStatement.setInt(1, profileOwnerID);
//				preparedStatement.setInt(2, profileOwnerID);
//				rs = preparedStatement.executeQuery();
//				String Value = "0";
//				if (rs.next()){
//					Value = rs.getString(1);
//					result.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//				}
//				else
//					result.put("pendingcount", new ObjectByteIterator("0".getBytes())) ;
//
//				//serialize the result hashmap and insert it in the cache for future use
//				SR.put("pendingcount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//			}catch(SQLException sx){
//				retVal = -2;
//				sx.printStackTrace(System.out);
//			}finally{
//				try {
//					if (rs != null)
//						rs.close();
//					if(preparedStatement != null)
//						preparedStatement.clearParameters();
//						//preparedStatement.close();
//				} catch (SQLException e) {
//					e.printStackTrace(System.out);
//					retVal = -2;
//				}
//			}
//		}
//		
//		//resource count
//		query = "SELECT count(*) FROM  resources WHERE creatorId = ?";
//
//		try {
//			//preparedStatement = conn.prepareStatement(query);
//			/*
//			if((preparedStatement = newCachedStatements.get(GETRESCNT_STMT)) == null)
//				preparedStatement = createAndCacheStatement(GETRESCNT_STMT, query);
//			*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			//cacheclient.addQuery("SELECT count(*) FROM  resources WHERE wallUserID = "+profileOwnerID);
//			rs = preparedStatement.executeQuery();
//			if (rs.next()){
//				SR.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//				result.put("resourcecount", new ObjectByteIterator(rs.getString(1).getBytes())) ;
//			} else {
//				SR.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
//				result.put("resourcecount", new ObjectByteIterator("0".getBytes())) ;
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		try {
//			if(insertImage){
//				query = "SELECT pic, userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
//				/*
//				if((preparedStatement = newCachedStatements.get(GETPROFILEIMG_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETPROFILEIMG_STMT, query);
//				*/
//			}
//			else{
//				query = "SELECT userid,username, fname, lname, gender, dob, jdate, ldate, address, email, tel FROM  users WHERE UserID = ?";
//				/*
//				if((preparedStatement = newCachedStatements.get(GETPROFILE_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETPROFILE_STMT, query);
//				*/
//			}
//			//preparedStatement = conn.prepareStatement(query);
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			
//			rs = preparedStatement.executeQuery();
//			ResultSetMetaData md = rs.getMetaData();
//			int col = md.getColumnCount();
//			if(rs.next()){
//				for (int i = 1; i <= col; i++){
//					String col_name = md.getColumnName(i);
//					String value ="";
//					if(col_name.equalsIgnoreCase("pic")){
//						// Get as a BLOB
//						Blob aBlob = rs.getBlob(col_name);
//						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
//						if(testMode){
//							//dump to file
//							try{
//								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-ctprofimage.bmp");
//								fos.write(allBytesInBlob);
//								fos.close();
//							}catch(Exception ex){
//							}
//						}
//						
//						// TODO: is this copy necessary? maybe when there are multiple rows in the resultset?
//						//byte[] val = new byte[allBytesInBlob.length];
//						//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
//						SR.put(col_name, new ObjectByteIterator(allBytesInBlob));
//						result.put(col_name, new ObjectByteIterator(allBytesInBlob));
//					}
//					else
//					{
//						value = rs.getString(col_name);
//
//						SR.put(col_name, new ObjectByteIterator(value.getBytes()));
//						result.put(col_name, new ObjectByteIterator(value.getBytes()));
//					}
//				}
//			}
//
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		//serialize the result hashmap and insert it in the cache for future use
//		payload = CacheUtilities.SerializeHashMap(SR);
//		try {
//			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
//			{
//				throw new Exception("Error calling WhalinMemcached set");
//			}
//			//while(setResult.isDone() == false);
//			//cacheclient.shutdown();
//		} catch (Exception e1) {
//			System.out.println("Error in ApplicationCacheClient, failed to insert the key-value pair in the cache.");
//			e1.printStackTrace(System.out);
//			retVal = -2;
//		}
//		return retVal;
//	}
//
//	
//	
//	@Override
//	public int listFriends(int requesterID, int profileOwnerID,
//			Set<String> fields, Vector<HashMap<String, ByteIterator>> result,
//			boolean insertImage, boolean testMode) {
//		// TODO Auto-generated method stub
//		if (verbose) System.out.print("List friends... "+profileOwnerID);
//		int retVal = SUCCESS;
//		ResultSet rs = null;
//		if(requesterID < 0 || profileOwnerID < 0)
//			return -1;
//
//		//String key = "lsFrds:"+requesterID+":"+profileOwnerID;
//		String key;
//		String query="";
//		//MemcachedClient cacheclient=null;
//		if(insertImage)
//		{
//			key = "lsFrds:"+profileOwnerID;
//		}
//		else
//		{
//			key = "lsFrdsNoImage:"+profileOwnerID;
//		}
//		// Check Cache first
//		try {
//			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
//			if (payload != null){
//				if (verbose) System.out.println("... Cache Hit!");
//				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
//					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
//				
//				//cacheclient.shutdown();
//				return retVal;
//			} else if (verbose) System.out.println("... Query DB.");
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to serialize a vector of hashmaps.");
//			retVal = -2;
//		}
//
//		
//		try {
//			if(insertImage){
//				//###########needs change###############
//				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((userid1=? and userid=userid2) or (userid2=? and userid=userid1)) and status = 2";
//				//cacheclient.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
//				/*##################
//				if((preparedStatement = newCachedStatements.get(GETFRNDSIMG_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETFRNDSIMG_STMT, query);
//				*/
//			}else{
//				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((userid1=? and userid=userid2) or (userid2=? and userid=userid1)) and status = 2";
//				//cacheclient.addQuery("SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
//				/*########3
//				if((preparedStatement = newCachedStatements.get(GETFRNDS_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETFRNDS_STMT, query);
//				*/
//			}
//			//###########
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			preparedStatement.setInt(2, profileOwnerID);
//			
//			////cacheclient.addQuery("SELECT * FROM users, friendship WHERE ((inviterid="+profileOwnerID+" and userid=inviteeid) or (inviteeid="+profileOwnerID+" and userid=inviterid)) and status = 2");
//			rs = preparedStatement.executeQuery();
//			int cnt=0;
//			while (rs.next()){
//				cnt++;
//				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//				if (fields != null) {
//					for (String field : fields) {
//						String value = rs.getString(field);
//						if(field.equalsIgnoreCase("userid"))
//							field = "userid";
//						values.put(field, new ObjectByteIterator(value.getBytes()));
//					}
//					result.add(values);
//				}else{
//					//get the number of columns and their names
//					//Statement st = conn.createStatement();
//					//ResultSet rst = st.executeQuery("SELECT * FROM users");
//					ResultSetMetaData md = rs.getMetaData();
//					int col = md.getColumnCount();
//					for (int i = 1; i <= col; i++){
//						String col_name = md.getColumnName(i);
//						String value ="";
//						if(col_name.equalsIgnoreCase("tpic")){
//							// Get as a BLOB
//							Blob aBlob = rs.getBlob(col_name);
//							byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
//							
//							if(testMode){
//								//dump to file
//								try{
//									FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-cthumbimage.bmp");
//									fos.write(allBytesInBlob);
//									fos.close();
//								}catch(Exception ex){
//								}
//							}
//							//byte[] val = new byte[allBytesInBlob.length];
//							//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
//							values.put(col_name, new ObjectByteIterator(allBytesInBlob));
//						}else{
//							value = rs.getString(col_name);
//							if(col_name.equalsIgnoreCase("userid"))
//								col_name = "userid";
//							values.put(col_name, new ObjectByteIterator(value.getBytes()));
//						}
//						
//					}
//					result.add(values);
//				}
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//
//		//serialize the result hashmap and insert it in the cache for future use
//		
//			byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
//			try {
//				if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
//				{
//					throw new Exception("Error calling WhalinMemcached set");
//				}
//				//cacheclient.shutdown();
//			} catch (Exception e1) {
//				System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//				e1.printStackTrace(System.out);
//				retVal = -2;
//			}
//		return retVal;		
//	}
//
//	
//	@Override
//	public int viewFriendReq(int profileOwnerID,
//			Vector<HashMap<String, ByteIterator>> result, boolean insertImage,
//			boolean testMode) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		ResultSet rs = null;
//
//		if (verbose) System.out.print("viewPendingRequests "+profileOwnerID+" ...");
//
//		if(profileOwnerID < 0)
//			return -1;
//
//		String key;
//		String query="";
//		//MemcachedClient cacheclient=null;
//		
//		if(insertImage)
//		{
//			key = "viewPendReq:"+profileOwnerID;
//		}
//		else
//		{
//			key = "viewPendReqNoImage:"+profileOwnerID;
//		}
//		// Check Cache first
//		try {
//			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
//			if (payload != null){
//				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
//					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
//				if (verbose) System.out.println("... Cache Hit!");
//				//cacheclient.shutdown();
//				return retVal;
//			} else if (verbose) System.out.println("... Query DB.");
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
//			retVal = -2;
//		}
//
//
//		try {
//			if(insertImage){
//				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel,tpic FROM users, friendship WHERE userid2=? and status = 1 and userid1 = userid";
//				//cacheclient.addQuery("SELECT * FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
//				/*
//				if((preparedStatement = newCachedStatements.get(GETPENDIMG_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETPENDIMG_STMT, query);
//				*/
//			}else{ 
//				query = "SELECT userid, username, fname, lname, gender, dob, jdate, ldate, address,email,tel FROM users, friendship WHERE userid2=? and status = 1 and userid1 = userid";
//				//cacheclient.addQuery("SELECT * FROM users, friendship WHERE inviteeid="+profileOwnerID+" and status = 1 and inviterid = userid");
//				/*
//				if((preparedStatement = newCachedStatements.get(GETPEND_STMT)) == null)
//					preparedStatement = createAndCacheStatement(GETPEND_STMT, query);
//				*/
//			}
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			////cacheclient.addQuery("SELECT * FROM users, friendship WHERE inviteeid= and status = 1 and inviterid = userid");
//			rs = preparedStatement.executeQuery();
//			int cnt=0;
//			while (rs.next()){
//				cnt++;
//				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//				//get the number of columns and their names
//				ResultSetMetaData md = rs.getMetaData();
//				int col = md.getColumnCount();
//				for (int i = 1; i <= col; i++){
//					String col_name = md.getColumnName(i);
//					String value = "";
//					if(col_name.equalsIgnoreCase("tpic") ){
//						// Get as a BLOB
//						Blob aBlob = rs.getBlob(col_name);
//						byte[] allBytesInBlob = aBlob.getBytes(1, (int) aBlob.length());
//						
//						if(testMode){
//							//dump to file
//							try{
//								FileOutputStream fos = new FileOutputStream(profileOwnerID+"-"+cnt+"-ctthumbimage.bmp");
//								fos.write(allBytesInBlob);
//								fos.close();
//							}catch(Exception ex){
//							}
//						}
//						//byte[] val = new byte[allBytesInBlob.length];
//						//System.arraycopy( allBytesInBlob, 0, val, 0, allBytesInBlob.length ); 
//						values.put(col_name, new ObjectByteIterator(allBytesInBlob));
//					}else{
//						value = rs.getString(col_name);
//						if(col_name.equalsIgnoreCase("userid"))
//							col_name = "userid";
//
//						values.put(col_name, new ObjectByteIterator(value.getBytes()));
//					}
//
//				}
//				result.add(values);
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
//		try {
//			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
//			{
//				throw new Exception("Error calling WhalinMemcached set");
//			}
//			//cacheclient.shutdown();
//		} catch (Exception e1) {
//			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//			e1.printStackTrace(System.out);
//			retVal = -2;
//		}
//		
//		return retVal;		
//
//	}
//
//	@Override
//	public int acceptFriend(int inviterID, int inviteeID) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		if(inviterID < 0 || inviteeID < 0)
//			return -1;
//		String query;
//		query = "UPDATE friendship SET status = 2 WHERE userid1=? and userid2= ? ";
//		try{
//			/*
//			if((preparedStatement = newCachedStatements.get(ACCREQ_STMT)) == null)
//				preparedStatement = createAndCacheStatement(ACCREQ_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, inviterID);
//			preparedStatement.setInt(2, inviteeID);
//			preparedStatement.executeUpdate();
//
//			if(!useTTL)
//			{
//				String key;
//				
//				if(isInsertImage)
//				{
//					key = "lsFrds:"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "lsFrds:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					//Invalidate the friendcount for each member
//					key = "profile"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "profile"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//					
//					key = "ownprofile"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "ownprofile"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReq:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				else
//				{
//					key = "lsFrdsNoImage:"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "lsFrdsNoImage:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					//Invalidate the friendcount for each member
//					key = "profileNoImage"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "profileNoImage"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//					
//					key = "ownprofileNoImage"+inviterID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "ownprofileNoImage"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReqNoImage:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}	
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		return retVal;		
//	}
//
//	@Override
//	public int rejectFriend(int inviterID, int inviteeID) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		if(inviterID < 0 || inviteeID < 0)
//			return -1;
////###################check parameters and query
//		String query = "DELETE FROM friendship WHERE userid1=? and userid2= ? ";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(REJREQ_STMT)) == null)
//					preparedStatement = createAndCacheStatement(REJREQ_STMT, query);
//				*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, inviterID);
//			preparedStatement.setInt(2, inviteeID);
//			preparedStatement.executeUpdate();
//
//			if (!useTTL)
//			{
//				String key;
//				if(isInsertImage)
//				{
//					key = "ownprofile"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReq:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				else
//				{
//					key = "ownprofileNoImage"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReqNoImage:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				
//				key = "PendingFriendship:"+inviteeID;
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		return retVal;	
//	}
//
//	@Override
//	public int inviteFriend(int inviterID, int inviteeID) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		if(inviterID < 0 || inviteeID < 0)
//			return -1;
//
//		String query = "INSERT INTO friendship values(?,?,1)";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(INVFRND_STMT)) == null)
//				preparedStatement = createAndCacheStatement(INVFRND_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, inviterID);
//			preparedStatement.setInt(2, inviteeID);
//			preparedStatement.executeUpdate();
//
//			if (!useTTL)
//			{
//				String key;
//				if(isInsertImage)
//				{
//					key = "ownprofile"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReq:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				else
//				{
//					key = "ownprofileNoImage"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "viewPendReqNoImage:"+inviteeID;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				
//				key = "PendingFriendship:"+inviteeID;
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		return retVal;
//	}
//
//	@Override
//	public int viewTopKResources(int requesterID, int profileOwnerID, int k,
//			Vector<HashMap<String, ByteIterator>> result) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		ResultSet rs = null;
//		if(profileOwnerID < 0 || requesterID < 0 || k < 0)
//			return -1;
//		if (verbose) System.out.print("getTopKResources "+profileOwnerID+" ...");
//
//		String key = "TopKRes:"+profileOwnerID;
//		//MemcachedClient cacheclient=null;
//		// Check Cache first
//		try {
//			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
//			if (payload != null && CacheUtilities.unMarshallVectorOfHashMaps(payload,result)){
//				if (verbose) System.out.println("... Cache Hit!");
//				//cacheclient.shutdown();
//				return retVal;
//			}
//			else if (verbose) System.out.print("... Query DB!");
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}
//
//		String query = "SELECT * FROM resources WHERE walluserid = ? AND rownum <? ORDER BY rid desc";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(GETTOPRES_STMT)) == null)
//				preparedStatement = createAndCacheStatement(GETTOPRES_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, profileOwnerID);
//			preparedStatement.setInt(2, (k+1));
//			rs = preparedStatement.executeQuery();
//			while (rs.next()){
//				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//				//get the number of columns and their names
//				ResultSetMetaData md = rs.getMetaData();
//				int col = md.getColumnCount();
//				for (int i = 1; i <= col; i++){
//					String col_name = md.getColumnName(i);
//					String value = rs.getString(col_name);
//					if(col_name.equalsIgnoreCase("rid"))
//						col_name = "rid";
//					else if(col_name.equalsIgnoreCase("walluserid"))
//						col_name = "walluserid";
//					values.put(col_name, new ObjectByteIterator(value.getBytes()));
//				}
//				result.add(values);
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//
//		if (retVal == SUCCESS){
//			//serialize the result hashmap and insert it in the cache for future use
//			byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
//			try {
//				if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
//				{
//					throw new Exception("Error calling WhalinMemcached set");
//				}
//				//cacheclient.shutdown();
//			} catch (Exception e1) {
//				System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//				e1.printStackTrace(System.out);
//				retVal = -2;
//			}
//		}
//
//		return retVal;		
//	}
//
//	@Override
//	public int getCreatedResources(int creatorID,
//			Vector<HashMap<String, ByteIterator>> result) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		ResultSet rs = null;
//		Statement st = null;
//		if(creatorID < 0)
//			return -1;
//
//		String query = "SELECT * FROM resources WHERE creatorid = "+creatorID;
//		try {
//			st = (Statement) conn.createStatement();
//			rs = st.executeQuery(query);
//			while (rs.next()){
//				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//				//get the number of columns and their names
//				ResultSetMetaData md = rs.getMetaData();
//				int col = md.getColumnCount();
//				for (int i = 1; i <= col; i++){
//					String col_name = md.getColumnName(i);
//					String value = rs.getString(col_name);
//					if(col_name.equalsIgnoreCase("rid"))
//						col_name = "rid";
//					values.put(col_name, new ObjectByteIterator(value.getBytes()));
//				}
//				result.add(values);
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(st != null)
//					st.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//
//		return retVal;		
//	}
//
//	@Override
//	public int viewCommentOnResource(int requesterID, int profileOwnerID,
//			int resourceID, Vector<HashMap<String, ByteIterator>> result) {
//		// TODO Auto-generated method stub
//		if (verbose) System.out.print("Comments of "+resourceID+" ...");
//		int retVal = SUCCESS;
//		ResultSet rs = null;
//		if(profileOwnerID < 0 || requesterID < 0 || resourceID < 0)
//			return -1;
//
//
//		String key = "ResCmts:"+resourceID;
//		String query="";
//		//MemcachedClient cacheclient=null;
//		// Check Cache first
//		try {
//			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
//			if (payload != null){
//				if (!CacheUtilities.unMarshallVectorOfHashMaps(payload,result))
//					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVectorOfHashMaps");
//				//				for (int i = 0; i < result.size(); i++){
//				//					HashMap<String, ByteIterator> myhashmap = result.elementAt(i);
//				//					if (myhashmap.get("RID") != null)
//				//						if (Integer.parseInt(myhashmap.get("RID").toString()) != resourceID)
//				//							System.out.println("ERROR:  Expecting results for "+resourceID+" and got results for resource "+myhashmap.get("RID").toString());
//				//						else i=result.size();
//				//				}
//				if (verbose) System.out.println("... Cache Hit!");
//				//cacheclient.shutdown();
//				return retVal;
//			} else if (verbose) System.out.print("... Query DB!");
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
//			retVal = -2;
//		}
//
//		try {	
//			query = "SELECT * FROM manipulation WHERE rid = ?";	
//			/*
//			if((preparedStatement = newCachedStatements.get(GETRESCMT_STMT)) == null)
//				preparedStatement = createAndCacheStatement(GETRESCMT_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, resourceID);
//			
//			//cacheclient.addQuery("SELECT * FROM manipulation WHERE rid = "+resourceID);
//			rs = preparedStatement.executeQuery();
//			while (rs.next()){
//				HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
//				//get the number of columns and their names
//				ResultSetMetaData md = rs.getMetaData();
//				int col = md.getColumnCount();
//				for (int i = 1; i <= col; i++){
//					String col_name = md.getColumnName(i);
//					String value = rs.getString(col_name);
//					values.put(col_name, new ObjectByteIterator(value.getBytes()));
//				}
//				result.add(values);
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if (rs != null)
//					rs.close();
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		
//		byte[] payload = CacheUtilities.SerializeVectorOfHashMaps(result);
//		try {			
//			if(!common.CacheUtilities.CacheSet(this.cacheclient, key, payload, this.useTTL, this.TTLvalue, this.compressPayload))
//			{
//				throw new Exception("Error calling WhalinMemcached set");
//			}
//			//cacheclient.shutdown();
//		} catch (Exception e1) {
//			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
//			e1.printStackTrace(System.out);
//			retVal = -2;
//		}
//		return retVal;		
//	}
//
//	@Override
//	public int postCommentOnResource(int commentCreatorID,
//			int resourceCreatorID, int resourceID,
//			HashMap<String, ByteIterator> commentValues) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//
//		if(resourceCreatorID < 0 || commentCreatorID < 0 || resourceID < 0)
//			return -1;
//
//		String query = "INSERT INTO manipulation(mid, creatorid, rid, modifierid, timestamp, type, content) VALUES (?, ?, ?, ?, ?, ?, ?)";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(POSTCMT_STMT)) == null)
//				preparedStatement = createAndCacheStatement(POSTCMT_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, Integer.parseInt(commentValues.get("mid").toString()));
//			preparedStatement.setInt(2, resourceCreatorID);
//			preparedStatement.setInt(3, resourceID);
//			preparedStatement.setInt(4,commentCreatorID);
//			preparedStatement.setString(5,commentValues.get("timestamp").toString());
//			preparedStatement.setString(6,commentValues.get("type").toString());
//			preparedStatement.setString(7,commentValues.get("content").toString());
//		
//			preparedStatement.executeUpdate();
//
//			if (!useTTL)
//			{
//				String key = "ResCmts:"+resourceID;
//				//MemcachedClient cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//				//cacheclient.shutdown();
//			}
//
//
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		} catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//
//		return retVal;		
//	}
//
//	@Override
//	public int delCommentOnResource(int resourceCreatorID, int resourceID,
//			int manipulationID) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//
//		if(resourceCreatorID < 0 || resourceID < 0 || manipulationID < 0)
//			return -1;
//
//		String query = "DELETE FROM manipulation WHERE mid=? AND rid=?";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(DELCMT_STMT)) == null)
//				preparedStatement = createAndCacheStatement(DELCMT_STMT, query);
//				*/	
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, manipulationID);
//			preparedStatement.setInt(2, resourceID);
//			preparedStatement.executeUpdate();
//			
//			if (!useTTL)
//			{
//				String key = "ResCmts:"+resourceID;
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//			}
//			
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//
//		return retVal;	
//	}
//
//	@Override
//	public int thawFriendship(int friendid1, int friendid2) {
//		// TODO Auto-generated method stub
//		int retVal = SUCCESS;
//		if(friendid1 < 0 || friendid2 < 0)
//			return -1;
//
//		String query = "DELETE FROM friendship WHERE (userid1=? and userid2= ?) OR (userid1=? and userid2= ?) and status=2";
//		try {
//			/*
//			if((preparedStatement = newCachedStatements.get(UNFRNDFRND_STMT)) == null)
//				preparedStatement = createAndCacheStatement(UNFRNDFRND_STMT, query);
//		*/
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, friendid1);
//			preparedStatement.setInt(2, friendid2);
//			preparedStatement.setInt(3, friendid2);
//			preparedStatement.setInt(4, friendid1);
//
//			preparedStatement.executeUpdate();
//
//			if (!useTTL)
//			{				
//				//Invalidate exisiting list of friends for each member
//				String key;
//				
//				if(isInsertImage)
//				{
//					key = "lsFrds:"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "lsFrds:"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					//Invalidate the friendcount for each member
//					key = "profile"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "profile"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//					
//					key = "ownprofile"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "ownprofile"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				else
//				{
//					key = "lsFrdsNoImage:"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "lsFrdsNoImage:"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					//Invalidate the friendcount for each member
//					key = "profileNoImage"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "profileNoImage"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//					
//					key = "ownprofileNoImage"+friendid1;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//		
//					key = "ownprofileNoImage"+friendid2;
//					if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//					{
//						throw new Exception("Error calling WhalinMemcached delete");
//					}
//				}
//				
//				key = "ConfirmedFriendship:"+friendid1;
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//				
//				key = "ConfirmedFriendship:"+friendid2;				
//				if(!common.CacheUtilities.CacheDelete(this.cacheclient, key))
//				{
//					throw new Exception("Error calling WhalinMemcached delete");
//				}
//			}
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}catch (Exception e1) {
//			e1.printStackTrace(System.out);
//			System.exit(-1);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.clearParameters();
//					//preparedStatement.close();
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		return retVal;
//	}
//
//	@Override
//	public HashMap<String, String> getInitialStats() {
//		// TODO Auto-generated method stub
//		HashMap<String, String> stats = new HashMap<String, String>();
//		Statement st = null;
//		ResultSet rs = null;
//		String query = "";
//		try {
//			st = (Statement) conn.createStatement();
//			//get user count
//			query = "SELECT count(*) from users";
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("usercount",rs.getString(1));
//			}else
//				stats.put("usercount","0"); 
//			rs.close();
//			//get resources per user
//			query = "SELECT count(*) FROM resources where creatorId=1";
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("resourcesperuser",rs.getString(1));
//			}else{
//				stats.put("resourcesperuser","0");
//			}
//			rs.close();
//			//get number of confirmed friends per user
//			query = "SELECT count(*) FROM friendship where userid1=1 or userid2=1 and status=2";
//			rs = st.executeQuery(query);
//			if(rs.next()){
//				stats.put("avgfriendsperuser",rs.getString(1));
//			}else
//				stats.put("avgfriendsperuser","0");
//			if(rs != null) rs.close();
//			
//			//get number of unconfirmed friends per user
//			query = "SELECT count(*) FROM friendship where userid1=1 and status=1" ;
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
//
//	}
//
//	@Override
//	public int CreateFriendship(int friendid1, int friendid2) {
//		// TODO Auto-generated method stub
//		int retVal = 1;
//
//		try {
//			conn.setAutoCommit(false);
//			String query = "CALL createFriendship_procedure(?,?,2)";
//			preparedStatement = (PreparedStatement) conn.prepareStatement(query);
//			preparedStatement.setInt(1, friendid1);
//			preparedStatement.setInt(2, friendid2);
//			preparedStatement.executeUpdate();
//			if(preparedStatement != null)
//				preparedStatement.close();
//			conn.commit();
//			conn.setAutoCommit(true);
//
//		}catch(SQLException sx){
//			retVal = -2;
//			sx.printStackTrace(System.out);
//		}finally{
//			try {
//				if(preparedStatement != null)
//					preparedStatement.close();
//				conn.setAutoCommit(true);
//			} catch (SQLException e) {
//				retVal = -2;
//				e.printStackTrace(System.out);
//			}
//		}
//		return retVal;
//	}
//
//	@Override
//	public void createSchema(Properties props) {
//		// TODO Auto-generated method stub
//		try {
//			st = (Statement) conn.createStatement();
//		} catch (SQLException e) {
//			System.out.println( "Error while creating a statement: "+ e.toString() ); 
//			e.printStackTrace();
//		}
//		
//		try {
//			st.executeUpdate("drop table IF EXISTS manipulation");
//			st.executeUpdate("drop table IF EXISTS resources");
//			st.executeUpdate("drop table IF EXISTS friendship");
//			st.executeUpdate("drop table IF EXISTS users ");
//			st.executeUpdate("drop procedure IF EXISTS accept_friendship" );
//			st.executeUpdate("drop procedure IF EXISTS createFriendship_procedure" );
//			st.executeUpdate("drop procedure IF EXISTS rejectFriend_procedure" );
//			st.executeUpdate("drop procedure IF EXISTS inviteFriend_procedure" );
//			st.executeUpdate("drop procedure IF EXISTS thawFriendship_procedure" );
//			st.executeUpdate("drop procedure IF EXISTS createResource_procedure" );
//			
//			st.executeUpdate("create table users(userid INT , username VARCHAR(200), pw VARCHAR(200), " +
//					"fname VARCHAR(200), lname VARCHAR(200), gender VARCHAR(200), dob DATE, jdate DATE, ldate DATE, address VARCHAR(200), " +
//					"email VARCHAR(200), tel VARCHAR(200), pic BLOB, TPIC BLOB, PRIMARY KEY (userid))");
//					System.out.println("Users table was created successfully");
//
//			st.executeUpdate("CREATE TABLE friendship (userid1 INT, userid2 INT, status INT, "+ 
//					"PRIMARY KEY (userid1,userid2))");
//			System.out.println("Friendship table was created successfully");
//			
//			st.executeUpdate("create procedure accept_friendship (memberA INT, memberB INT) begin " +
//					"declare count INT;" +
//					"INSERT INTO ConfirmedFriendship values (memberA, memberB);" +
//					"select unconfirmedFriendshipNum into count from users where userid=memberA;" +
//					"IF count = 0 THEN " +
//					"UPDATE users AS u1, users AS u2 SET u1.confirmedFriendshipNum = u1.confirmedFriendshipNum + 1 , u2.confirmedFriendshipNum = u2.confirmedFriendshipNum + 1 WHERE u1.userid = memberA and u2.userid = memberB;" +
//					"ELSE " +
//					"DELETE FROM UnconfirmedFriendship  WHERE inviter = memberA and invitee = memberB;" +
//					"UPDATE users AS u1, users AS u2 SET u1.unconfirmedFriendshipNum = u1.unconfirmedFriendshipNum - 1 , u1.confirmedFriendshipNum = u1.confirmedFriendshipNum + 1 , u2.unconfirmedFriendshipNum = u2.unconfirmedFriendshipNum - 1 , u2.confirmedFriendshipNum = u2.confirmedFriendshipNum + 1 WHERE u1.userid = memberA and u2.userid = memberB;" +
//					"END IF;" +
//					"end");
//			System.out.println("Confirmed Friendship procedure was created successfully");
//			
//			st.executeUpdate("create procedure createFriendship_procedure (memberA INT, memberB INT, status INT) begin " +
//					"INSERT INTO friendship values (memberA, memberB, status); end");
//			System.out.println("Create friendship procedure was created successfully");
//						
//			st.executeUpdate("create procedure inviteFriend_procedure (inviterID INT, inviteeID INT, status INT) begin " +
//					"INSERT INTO friendship values (inviterID, inviteeID, status); end");
//			System.out.println("Invite friend procedure was created successfully");
//			
//			st.executeUpdate("create procedure rejectFriend_procedure (inviterID INT, inviteeID INT, status_ INT) begin " +
//					"DELETE FROM friendship WHERE userid1=inviterID and userid2=inviteeID and status=status_; end");
//			System.out.println("Reject friend procedure was created successfully");
//			
//			st.executeUpdate("create procedure thawFriendship_procedure (inviterID INT, inviteeID INT, status_ INT) begin " +
//					"DELETE FROM ConfirmedFriendship WHERE userid1=inviterID and userid2=inviteeID and status=status_; end");
//			System.out.println("Thaw friendship procedure was created successfully");
//			
//			st.executeUpdate("CREATE TABLE resources (rid INT , creatorId INT, wallUserId INT, " +
//					"type VARCHAR (200), body VARCHAR(200), doc VARCHAR (200), PRIMARY KEY (rid))");
//			System.out.println("Resources table was created successfully");
//			
//			st.executeUpdate("create procedure createResource_procedure (rid INT , creatorId INT, wallUserId INT, " +
//					"type VARCHAR (200), body VARCHAR(200), doc VARCHAR (200)) begin " +
//					"INSERT INTO resources values (rid, creatorId, wallUserId, type, body, doc);	end");
//			System.out.println("Create resource procedure was created successfully");
//			
//			st.executeUpdate("CREATE TABLE manipulation ( mid INT , creatorId INT, rid INT, " +
//					"modifierId INT, timestamp_ VARCHAR(200), type_ VARCHAR(200), content VARCHAR(200), PRIMARY KEY(mid))");
//			System.out.println("Modify table was created successfully");
//		} catch (SQLException e) {
//			System.out.println( "Error while creating a table: "+ e.toString() );
//			e.printStackTrace();
//		}
//		
//	}
//
//	@Override
//	public int queryPendingFriendshipIds(int inviteeid,
//			Vector<Integer> pendingIds) {
//		// TODO Auto-generated method stub
//		Statement st = null;
//		ResultSet rs = null;
//		String query = "";
//		int retVal	 = SUCCESS;
//		
//		String key = "PendingFriendship:"+inviteeid;
////		//MemcachedClient cacheclient=null;
////		// Check Cache first
////		try {
////			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
////			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
////			if (payload != null){
////				if (!unMarshallVectorOfInts(payload, pendingIds))
////					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVector");
////				
////				if (verbose) System.out.println("... Cache Hit!");
////				//cacheclient.shutdown();
////				return retVal;
////			} else if (verbose) System.out.print("... Query DB!");
////		} catch (Exception e1) {
////			e1.printStackTrace(System.out);
////			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
////			retVal = -2;
////		}
//		
//		try {
//			st = (Statement) conn.createStatement();
//			//##########changed
//			query = "SELECT userid1 from friendship where userid2='"+inviteeid+"'and status='1'";
//			//cacheclient.addQuery(query);
//			rs = st.executeQuery(query);
//			while(rs.next()){
//				pendingIds.add(rs.getInt(1));
//			}	
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
//				return -2;
//			}
//		}
////		
////		byte[] payload = SerializeVectorOfInts(pendingIds);
////		try {
////			OperationFuture<Boolean> setResult = CacheSet(key, 0, payload);
////			setResult.get();
////			//cacheclient.shutdown();
////		} catch (Exception e1) {
////			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
////			e1.printStackTrace(System.out);
////			retVal = -2;
////		}
//
//		return retVal;
//
//	}
//
//	@Override
//	public int queryConfirmedFriendshipIds(int profileId,
//			Vector<Integer> confirmedIds) {
//		// TODO Auto-generated method stub
//		Statement st = null;
//		ResultSet rs = null;
//		String query = "";		
//		int retVal	 = SUCCESS;
//		
////		String key = "ConfirmedFriendship:"+profileId;
////		MemcachedClient cacheclient=null;
////		// Check Cache first
////		try {
////			//cacheclient = new MemcachedClient(AddrUtil.getAddresses(cache_hostname + ":" + cache_port));
////			byte[] payload = common.CacheUtilities.CacheGet(this.cacheclient, key, this.compressPayload);
////			if (payload != null){
////				if (!unMarshallVectorOfInts(payload, confirmedIds))
////					System.out.println("Error in ApplicationCacheClient: Failed to unMarshallVector");
////				
////				if (verbose) System.out.println("... Cache Hit!");
////				//cacheclient.shutdown();
////				return retVal;
////			} else if (verbose) System.out.print("... Query DB!");
////		} catch (Exception e1) {
////			e1.printStackTrace(System.out);
////			System.out.println("Error in ApplicationCacheClient, viewPendingRequests failed to serialize a vector of hashmaps.");
////			retVal = -2;
////		}
//		
//		try {
//			st = (Statement) conn.createStatement();
//			query = "SELECT userid1, userid2 from friendship where (userid1="+profileId+" OR userid2="+profileId+") and status='2'";
//			//cacheclient.addQuery(query);
//			rs = st.executeQuery(query);
//			while(rs.next()){
//				if(rs.getInt(1) != profileId)
//					confirmedIds.add(rs.getInt(1));
//				else
//					confirmedIds.add(rs.getInt(2));
//			}	
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
//				return -2;
//			}
//		}
//
////		byte[] payload = SerializeVectorOfInts(confirmedIds);
////		try {
////			OperationFuture<Boolean> setResult = CacheSet(key, 0, payload);
////			setResult.get();
////			//cacheclient.shutdown();
////		} catch (Exception e1) {
////			System.out.println("Error in ApplicationCacheClient, getListOfFriends failed to insert the key-value pair in the cache.");
////			e1.printStackTrace(System.out);
////			retVal = -2;
////		}
//		return retVal;
//		
//	}
//
//
//}
