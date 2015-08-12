package db;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Utility class to help manipulating MySql database.
 */
public class MySqlHelper extends DBHelper{
	
	// constructors
	
	/**
	 * MySqlHelper constructor.
	 * @param host host string, e.g. "jdbc:mysql://192.168.66.189:3306/mc_dev_trunk_litb?characterEncoding=UTF-8"
	 * @param user user name
	 * @param passwd password
	 * @throws SQLException 
	 */
	public MySqlHelper(String host, String user, String passwd) throws SQLException {
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		connection = DriverManager.getConnection(host, user, passwd);
	}

}
