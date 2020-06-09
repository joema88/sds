package com.sds.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ALTBT9DB extends DB {

	public ALTBT9DB() {
		// TODO Auto-generated constructor stub
	}

	private static PreparedStatement teal10QueryStmnt = null;
	private static PreparedStatement dateIDQueryStmnt = null;
	private static PreparedStatement altBT9Update = null;
	private static PreparedStatement resetBT9 = null;
	private static PreparedStatement dateStmnt = null;

	public static void closeConnection() {
		try {

			if(dateStmnt != null) {
				dateStmnt.close();
				dateStmnt = null;
			}
			if (resetBT9 != null) {
				resetBT9.close();
				resetBT9 = null;
			}

			if (altBT9Update != null) {
				altBT9Update.close();
				altBT9Update = null;
			}
			if (teal10QueryStmnt != null) {
				teal10QueryStmnt.close();
				teal10QueryStmnt = null;
			}
			if (dateIDQueryStmnt != null) {
				dateIDQueryStmnt.close();
				dateIDQueryStmnt = null;
			}
		} catch (Exception ex) {

		}
	}

	public static PreparedStatement  resetAltBT9() {

		try {

			if (resetBT9 == null) {

				String query = "UPDATE BBROCK SET ABT9 = 0 WHERE STOCKID = ? AND ABT9<>0";

				resetBT9 = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return resetBT9;
	}

	//dateStmnt
	public static PreparedStatement getDateStmnt() {

		if (dateStmnt == null) {
			try {

				String query = "SELECT CDATE FROM DATES WHERE DATEID = ?";

				dateStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateStmnt;
	}
	
	public static PreparedStatement getAltBT9Update() {

		if (altBT9Update == null) {
			try {

				String query = "UPDATE BBROCK SET ABT9 = 1 WHERE STOCKID = ?  AND DATEID = ?";

				altBT9Update = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return altBT9Update;
	}

	public static PreparedStatement getDateIDQuery() {

		if (dateIDQueryStmnt == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK WHERE STOCKID = ?  ORDER BY DATEID ASC";

				dateIDQueryStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dateIDQueryStmnt;
	}

	public static PreparedStatement getALTBT9Stmnt() {

		if (teal10QueryStmnt == null) {
			try {

				String query = "SELECT SUM(TEAL) FROM BBROCK WHERE STOCKID = ?  AND DATEID>=? AND DATEID <=?";

				teal10QueryStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return teal10QueryStmnt;
	}
}
