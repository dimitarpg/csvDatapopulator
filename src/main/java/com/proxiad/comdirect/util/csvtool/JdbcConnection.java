package com.proxiad.comdirect.util.csvtool;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;


public class JdbcConnection {

	private static Properties dbProperties;
	private static JdbcConnection instance;
	private Connection connection;

	private JdbcConnection(String address, String user, String password)
			throws SQLException, IOException, ClassNotFoundException {
		InputStream propInStream = null;
		try {
			dbProperties = new Properties();
			propInStream = JdbcConnection.class.getClassLoader().getResourceAsStream("app.properties");
			dbProperties.load(propInStream);

			String url = dbProperties.getProperty("connectionURL");
			String driverName = dbProperties.getProperty("driverClassName");

			Class.forName(driverName);
			this.connection = DriverManager.getConnection(url.concat(address), user, password);
		} finally {
			if (propInStream != null) {
				try {
					propInStream.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public Connection getConnection() {
		return connection;
	}

	public static JdbcConnection getInstance(String address, String user, String password)
			throws SQLException, ClassNotFoundException, IOException {
		if (instance == null) {
			instance = new JdbcConnection(address, user, password);
		} else if (instance.getConnection().isClosed()) {
			instance = new JdbcConnection(address, user, password);
		}
		return instance;
	}

	public static void closeConnection(Connection con, Statement st, ResultSet rs) throws SQLException {
		if (rs != null)
			rs.close();
		if (st != null)
			st.close();
		if (con != null)
			con.close();
	}
}
