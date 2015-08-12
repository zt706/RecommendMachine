package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * The abstract class to help manipulating all kinds of databases, e.g. MySql, Oracle.
 */
public abstract class DBHelper {
	
	protected Connection connection = null;
	protected Statement statement = null;
	
	/**
	 * Execute single SELECT SQL statement.
	 * @param sql SELECT SQL statement (can not be DELETE, UPDATE, INSERT, CREATE TABLE statements)
	 * @return Result set
	 * @throws SQLException
	 */
	public ResultSet executeQuery(String sql) throws SQLException{
		if(statement == null)
			statement = connection.createStatement();
		return statement.executeQuery(sql);
	}
	
	/**
	 * Execute single UPDATE SQL statement.
	 * @param sql UPDATE SQL statement (e.g. DELETE, UPDATE, INSERT, CREATE TABLE statements)
	 * @throws SQLException
	 */
	public void executeUpdate(String sql) throws SQLException{
		if(statement == null)
			statement = connection.createStatement();
		statement.executeUpdate(sql);
	}
	
	/**
	 * Execute batch of UPDATE SQL statements. All statements execute in a same transition.
	 * @param sql list of UPDATE SQL statements (e.g. DELETE, UPDATE, INSERT, CREATE TABLE statements)
	 * @throws SQLException
	 */
	public void executeUpdates(List<String> sqlList) throws SQLException{
		if(statement == null)
			statement = connection.createStatement();
		connection.setAutoCommit(false);
		for(String sql : sqlList)
			statement.addBatch(sql);
		statement.executeBatch();
		connection.commit();
		connection.setAutoCommit(true);
	}
	
	/**
	 * Disconnect the database.
	 */
	public void close(){
		try {
			if(statement != null)
				statement.close();
			if(connection != null)
				connection.close();
		} catch (SQLException e) {
			System.err.println("Failed to close statement and connection.");
			e.printStackTrace();
		}
	}
}
