package kosar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;

public class DBConn {
	Connection conn;
	HashMap<Integer, PreparedStatement> newCachedStatements;
	
	public DBConn(Connection conn) {
		this.conn = conn;
		this.newCachedStatements = new HashMap<Integer, PreparedStatement>();
	}
	
	public HashMap<Integer, PreparedStatement> getCachedStatements() {
		return newCachedStatements;		
	}
	
	public Connection getConnection() {
		return conn;
	}
}
