package com.sds.db;

import java.sql.*;
import java.util.Calendar;

public class DB {

	/*
	 * //check DB size SELECT table_schema AS 'DB Name', ROUND(SUM(data_length +
	 * index_length) / 1024 / 1024, 1) AS 'DB Size in MB' FROM
	 * information_schema.tables GROUP BY table_schema;
	 */
	
	//select a.DATEID, b.CDATE, STOCKID, CLOSE, ATR,TEAL, YELLOW, PINK, SC5,BT9,PTVAL, PTCP, PASS,SC10C, SC15,TSC, MARKCAP, VOLUME FROM BBROCK a, DATES b  WHERE a.DATEID=b.DATEID and STOCKID = 1 order by a.DATEID DESC limit 300;
	
	private static Connection dbcon = null;
	private static PreparedStatement symbolStmnt = null;
	private static PreparedStatement symbolDateIDQuery = null;
	private static PreparedStatement dateStmnt = null;
	private static PreparedStatement rockStmnt = null;
	private static PreparedStatement backTestStmnt = null;
	private static PreparedStatement backTestTealUpdateStmnt = null;
	private static PreparedStatement backTestYellowUpdateStmnt = null;
	private static PreparedStatement backTestPinkUpdateStmnt = null;
	private static PreparedStatement typSumQueryStmnt = null;
	private static PreparedStatement scUpdateStmnt = null;
	private static PreparedStatement queryTealStmnt = null;
	private static PreparedStatement updateT9Stmnt = null;
	private static PreparedStatement queryStockIDStmnt = null;
	private static PreparedStatement typSumbyStockIDStmnt = null;
	private static Statement stmnt = null;

	public static void closeConnection() {
		try {
			if(typSumbyStockIDStmnt != null) {
				typSumbyStockIDStmnt.close();
				typSumbyStockIDStmnt = null;
			}
			if(queryStockIDStmnt != null) {
				queryStockIDStmnt.close();
				queryStockIDStmnt = null;
			}
			if (queryTealStmnt != null) {
				queryTealStmnt.close();
				queryTealStmnt = null;
			}
			if (scUpdateStmnt != null) {
				scUpdateStmnt.close();
				scUpdateStmnt = null;
			}
			if (typSumQueryStmnt != null) {
				typSumQueryStmnt.close();
				typSumQueryStmnt = null;
			}
			if (symbolDateIDQuery != null) {
				symbolDateIDQuery.close();
				symbolDateIDQuery = null;
			}
			if (backTestYellowUpdateStmnt != null) {
				backTestYellowUpdateStmnt.close();
				backTestYellowUpdateStmnt = null;
			}
			if (backTestPinkUpdateStmnt != null) {
				backTestPinkUpdateStmnt.close();
				backTestPinkUpdateStmnt = null;
			}
			if (backTestTealUpdateStmnt != null) {
				backTestTealUpdateStmnt.close();
				backTestTealUpdateStmnt = null;
			}
			if (backTestStmnt != null) {
				backTestStmnt.close();
				backTestStmnt = null;
			}
			if (stmnt != null) {
				stmnt.close();
				stmnt = null;
			}
			if (symbolStmnt != null) {
				symbolStmnt.close();
				symbolStmnt = null;
			}
			if (dateStmnt != null) {
				dateStmnt.close();
				dateStmnt = null;
			}
			if (rockStmnt != null) {
				rockStmnt.close();
				rockStmnt = null;
			}

			if (dbcon != null) {
				dbcon.close();
				dbcon = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() {

		try {
			if (dbcon == null) {
				dbcon = DriverManager.getConnection(
						"jdbc:mysql://127.0.0.1:3306/SDS?allowPublicKeyRetrieval=true&useSSL=false", "root",
						"Goldfish@3224");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return dbcon;
	}

	public static Statement getStatement() {
		getConnection();
		try {
			if (stmnt == null) {
				stmnt = dbcon.createStatement();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return stmnt;
	}

	public static int getNextDateID() {
		int nextID = 1;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM DATES";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				nextID = rs.getInt(1) + 1;
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return nextID;
	}

	public static String getDateStr(int dateID) {
		String dateStr = "";
		getStatement();
		try {
			String query = "SELECT DATE_FORMAT(CDATE, '%Y-%m-%d') FROM DATES WHERE DATEID = " + dateID;

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				dateStr = rs.getString(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return dateStr;
	}

	public static int getDateID(String date) {
		int dateID = 0;
		getStatement();
		try {
			String query = " SELECT DATEID FROM DATES WHERE date(CDATE)='" + date + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				dateID = rs.getInt(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return dateID;
	}

	public static int getSymbolID(String symbol) {
		int stockID = 0;
		getStatement();
		try {
			String query = " SELECT STOCKID FROM SYMBOLS WHERE SYMBOL='" + symbol + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				stockID = rs.getInt(1);
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return stockID;
	}

	public static int getNextSymbolID() {
		int nextID = 1;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM SYMBOLS";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next()) {
				nextID = rs.getInt(1) + 1;
			}
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return nextID;
	}

	// SELECT our_date FROM our_table WHERE idate >= '1997-05-05';
	public static boolean checkDateExist(String date) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM DATES WHERE date(CDATE)='" + date + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	public static boolean checkSymbolExist(String symbol) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM SYMBOLS WHERE SYMBOL='" + symbol + "'";

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	public static boolean checkBBRecordExist(int stockID, int dateID) {
		boolean exist = true;
		getStatement();
		try {
			String query = " SELECT COUNT(*) FROM BBROCK WHERE STOCKID = " + stockID + " and DATEID = " + dateID;

			ResultSet rs = stmnt.executeQuery(query);

			if (rs.next() && rs.getInt(1) == 0) {
				exist = false;
			}
			;
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return exist;
	}

	// scUpdateStmnt
	public static PreparedStatement getSCUpdateStmnt() {
		getConnection();

		if (scUpdateStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET SC5 = ? WHERE STOCKID = ? AND DATEID =? ";

				scUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return scUpdateStmnt;
	}

	//queryStockIDStmnt
	public static PreparedStatement getStockIDQueryStmnt() {
		getConnection();

		if (queryStockIDStmnt == null) {
			try {

			
				String query = "SELECT STOCKID FROM BBROCK WHERE  DATEID =? ORDER BY STOCKID ASC";
				queryStockIDStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryStockIDStmnt;
	}

	
	//updateT9Stmnt = null;
	public static PreparedStatement getUpdateT9Stmnt() {
		getConnection();

		if (updateT9Stmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "UPDATE BBROCK SET BT9 = ? WHERE STOCKID =  ? AND DATEID =?";
				updateT9Stmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateT9Stmnt;
	}

	
	// queryTealStmnt
	public static PreparedStatement getTealQueryStmnt() {
		getConnection();

		if (queryTealStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select TEAL, DATEID, BT9 FROM BBROCK WHERE STOCKID = ? AND DATEID>=?  ORDER BY DATEID ASC";
				queryTealStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryTealStmnt;
	}

	
	public static PreparedStatement getTYPDSumByStockIDStmnt() {
		getConnection();

		if (typSumbyStockIDStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK WHERE STOCKID = ? AND DATEID>=? AND DATEID<=?";

				typSumbyStockIDStmnt  = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return typSumbyStockIDStmnt ;
	}

	
	
	// typSumQueryStmnt
	public static PreparedStatement getTYPDSumQueryStmnt() {
		getConnection();

		if (typSumQueryStmnt == null) {
			try {

				// select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE
				// a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?;

				String query = "select SUM(TEAL), SUM(YELLOW), SUM(PINK) FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and b.SYMBOL = ? AND DATEID>=? AND DATEID<=?";

				typSumQueryStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return typSumQueryStmnt;
	}

	// symbolDateIDQuery
	public static PreparedStatement getSymbolDateIDQueryStmnt() {
		getConnection();

		if (symbolDateIDQuery == null) {
			try {
				String query = "SELECT DATEID FROM BBROCK a, SYMBOLS b WHERE a.STOCKID = b.STOCKID and b.SYMBOL = ? ORDER BY a.DATEID DESC";

				symbolDateIDQuery = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return symbolDateIDQuery;
	}

	public static PreparedStatement getBackTestTealUpdateStmnt() {
		getConnection();

		if (backTestTealUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET TEAL = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestTealUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestTealUpdateStmnt;
	}

	public static PreparedStatement getBackTestYellowUpdateStmnt() {
		getConnection();

		if (backTestYellowUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET YELLOW = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestYellowUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestYellowUpdateStmnt;
	}

	public static PreparedStatement getBackTestPinkUpdateStmnt() {
		getConnection();

		if (backTestPinkUpdateStmnt == null) {
			try {
				String query = "UPDATE BBROCK SET PINK = 1 WHERE STOCKID = ? AND DATEID = ?";

				backTestPinkUpdateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestPinkUpdateStmnt;
	}

	public static PreparedStatement getBackTestInsertStatement() {
		getConnection();

		if (backTestStmnt == null) {
			try {
				String query = "insert into BBROCK (STOCKID,DATEID, CLOSE) values (?, ?,?)";

				backTestStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return backTestStmnt;
	}

	public static PreparedStatement getDateInsertStatement() {
		getConnection();

		if (dateStmnt == null) {
			try {
				String query = " insert into DATES (DATEID, CDATE) values (?, ?)";

				dateStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateStmnt;
	}

	public static PreparedStatement getSymbolInsertStatement() {
		getConnection();

		if (symbolStmnt == null) {
			try {
				String query = " insert into SYMBOLS (STOCKID, SYMBOL) values (?, ?)";

				symbolStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return symbolStmnt;
	}

	public static PreparedStatement getRockInsertStatement() {
		getConnection();

		if (rockStmnt == null) {
			try {
				String query = " insert into BBROCK(STOCKID,DATEID,PERCENT,CLOSE,NETCHANGE,ATR,OPEN,HIGH,LOW,LOW52,HIGH52,MARKCAP,VOLUME,YELLOW,TEAL,PINK)"
						+ " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

				rockStmnt = dbcon.prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return rockStmnt;
	}

	public static void main(String[] args) {

		// https://docs.oracle.com/javase/8/docs/api/java/sql/package-summary.html#package.description
		// auto java.sql.Driver discovery -- no longer need to load a java.sql.Driver
		// class via Class.forName

		// register JDBC driver, optional since java 1.6
		/*
		 * try { Class.forName("com.mysql.jdbc.Driver"); } catch (ClassNotFoundException
		 * e) { e.printStackTrace(); }
		 */

		// $$$$$$$$ SQL
		/*
		 * 1. CREATE TABLE SYMBOLS(STOCKID SMALLINT, SYMBOL VARCHAR(10), PRIMARY KEY
		 * (STOCKID)); 2. CREATE TABLE DATES(DATEID SMALLINT, CDATE DATE, PRIMARY KEY
		 * (DATEID)); 3. CREATE TABLE BBROCK(STOCKID SMALLINT, DATEID SMALLINT, PERCENT
		 * FLOAT DEFAULT 0.0, CLOSE FLOAT DEFAULT 0.0,NETCHANGE FLOAT DEFAULT 0.0, ATR
		 * FLOAT DEFAULT 0.0, OPEN FLOAT DEFAULT 0.0,HIGH FLOAT DEFAULT 0.0, LOW FLOAT
		 * DEFAULT 0.0,LOW52 FLOAT DEFAULT 0.0,HIGH52 FLOAT DEFAULT 0.0, MARKCAP FLOAT
		 * DEFAULT 0.0,VOLUME INT DEFAULT 0, YELLOW TINYINT DEFAULT 0,TEAL TINYINT
		 * DEFAULT 0, PINK TINYINT DEFAULT 0, SC5 SMALLINT DEFAULT 0, SC10C SMALLINT
		 * DEFAULT 0, SC15 SMALLINT DEFAULT 0, TSC INT DEFAULT 0,BT9 SMALLINT DEFAULT 0,
		 * PTVAL FLOAT DEFAULT 0.0, PTCP FLOAT DEFAULT 0.0, PASS TINYINT DEFAULT 0,
		 * PRIMARY KEY (STOCKID, DATEID), FOREIGN KEY (STOCKID) REFERENCES
		 * SYMBOLS(STOCKID) ON DELETE CASCADE, FOREIGN KEY (DATEID) REFERENCES
		 * DATES(DATEID)); //select a.DATEID,CDATE, a.STOCKID, CLOSE, ATR,TEAL, YELLOW,
		 * PINK, BTC,BYC,BPC,BTS,BYS,BPS,BBS,PTVAL, PTCP,BT9, MERGE, MARKCAP, VOLUME
		 * FROM BBROCK a, SYMBOLS b, DATES c WHERE a.STOCKID = b.STOCKID and
		 * a.DATEID=c.DATEID and b.SYMBOL='LYG';
		 */
		// $$$$$$$$$ SQL

		// auto close connection
		try {
			Connection conn = DriverManager.getConnection(
					"jdbc:mysql://127.0.0.1:3306/SDS?allowPublicKeyRetrieval=true&useSSL=false", "root",
					"Goldfish@3224");

			if (conn != null) {
				System.out.println("Connected to the database!");

				/*
				 * create table users ( id int unsigned auto_increment not null, first_name
				 * varchar(32) not null, last_name varchar(32) not null, date_created timestamp
				 * default now(), is_admin boolean, num_points int, primary key (id) );
				 */

				// create a sql date object so we can use it in our INSERT statement
				Calendar calendar = Calendar.getInstance();
				java.sql.Date startDate = new java.sql.Date(calendar.getTime().getTime());

				/*
				 * // the mysql insert statement String query =
				 * " insert into users (first_name, last_name, date_created, is_admin, num_points)"
				 * + " values (?, ?, ?, ?, ?)";
				 * 
				 * // create the mysql insert preparedstatement PreparedStatement preparedStmt =
				 * conn.prepareStatement(query); preparedStmt.setString (1, "Barney");
				 * preparedStmt.setString (2, "Rubble"); preparedStmt.setDate (3, startDate);
				 * preparedStmt.setBoolean(4, false); preparedStmt.setInt (5, 5000);
				 * 
				 * // execute the preparedstatement preparedStmt.execute();
				 * 
				 * conn.close();
				 */
				String query = " insert into HISTORY (STOCKID, DATEID, OPEN, YELLOW)" + " values (?, ?, ?, ?)";

				// create the mysql insert preparedstatement
				PreparedStatement preparedStmt = conn.prepareStatement(query);
				preparedStmt.setInt(1, 3);
				preparedStmt.setInt(2, 1);
				preparedStmt.setFloat(3, 9.80f);
				preparedStmt.setInt(4, 3);

				// execute the preparedstatement
				preparedStmt.execute();

				conn.close();
			} else {
				System.out.println("Failed to make connection!");
			}

		} catch (SQLException e) {
			e.printStackTrace(System.out);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
