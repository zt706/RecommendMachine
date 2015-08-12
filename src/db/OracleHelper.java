package db;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Utility class to help manipulating Oracle database.
 */
public class OracleHelper extends DBHelper{
	
	public static final String DB_TABLE_DELETE_SQL = "delete from %s where %s>=TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss') and %s<TO_DATE('%s', 'yyyy-mm-dd hh24:mi:ss')";
	
	/**
	 * OracleHelper constructor.
	 * @param host e.g. "jdbc:oracle:thin:@172.16.0.170:1521:dw01"
	 * @param user user name
	 * @param passwd password
	 * @throws SQLException 
	 */
	public OracleHelper(String host, String user, String passwd) throws SQLException {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		connection = DriverManager.getConnection(host, user, passwd);
	}
	
	public static String dealWithSpecialChar(String originalStr)
	{
		String formatedStr = originalStr.replaceAll("'", "''");
		
//		formatedStr = formatedStr.replaceAll("/", "//");
				
//		formatedStr = formatedStr.replaceAll("%", "/%");
				
//		formatedStr = formatedStr.replaceAll("_", "/_");
		
		return formatedStr;
	}
}
