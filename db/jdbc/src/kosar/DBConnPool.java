package kosar;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

/**
 * A general pool 
 * @author hieun
 *
 */
public class DBConnPool extends Pool<DBConn> {	
	/** The class to use as the jdbc driver. */
	public static final String DRIVER_CLASS = "db.driver";

	/** The URL to connect to the database. */
	public static final String CONNECTION_URL = "db.url";

	/** The user name to use to connect to the database. */ 
	public static final String CONNECTION_USER = "db.user";

	/** The password to use for establishing the connection. */
	public static final String CONNECTION_PASSWD = "db.passwd";
	
	Properties p;

	public DBConnPool(Properties p, int con) {
		super(con);
		this.p = p;
		for (int j = 0; j < con; j++) {
			DBConn item = createItem();
			this.availPool.add(item);
		}
	}

	@Override
	public void shutdownPool() {
		while (true) {
			DBConn db = (DBConn)this.availPool.poll();
			if (db == null)
				break;
			
			try {
				HashMap<Integer, PreparedStatement> newCachedStatements = db.getCachedStatements();
				// close all cached prepare statements
				if (newCachedStatements != null) {
					Set<Integer> statementTypes = newCachedStatements.keySet();
					Iterator<Integer> it = statementTypes.iterator();
					while (it.hasNext()) {
						int stmtType = it.next();
						if (newCachedStatements.get(stmtType) != null)
							newCachedStatements.get(stmtType).close();
					}
				}
				
				if (db.conn != null)
					db.conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace(System.out);
			}
		}

//		super.shutdownPool();
	}

	@Override
	protected DBConn createItem() {
		Connection conn = null;
		while (conn == null) {
			String urls = p.getProperty(CONNECTION_URL, "");
			String user = p.getProperty(CONNECTION_USER, "");
			String passwd = p.getProperty(CONNECTION_PASSWD, "");
			String driver = p.getProperty(DRIVER_CLASS);
			try {
				if (driver != null) {
					Class.forName(driver);
				}
				for (String url : urls.split(",")) {
					conn = DriverManager.getConnection(url, user, passwd);
					conn.setAutoCommit(false);
				}
			} catch (ClassNotFoundException e) {
				System.out.println("Error in initializing the JDBS driver: " + e);
				e.printStackTrace(System.out);
			} catch (SQLException e) {
				System.out.println("Error in database operation: " + e);
				e.printStackTrace(System.out);
			} catch (NumberFormatException e) {
				System.out.println("Invalid value for fieldcount property. " + e);
				e.printStackTrace(System.out);
			}		
		}
		return new DBConn(conn);
	}
}
