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
	private static PreparedStatement queryTeal = null;
	private static PreparedStatement passPriceUpdate = null;
	private static PreparedStatement passPointUpdate = null;
	private static PreparedStatement normalBT9Query = null;
	private static PreparedStatement aptvQuery = null;
	private static PreparedStatement passPointsQuery = null;
	private static PreparedStatement passPointsUpdate = null;

	public static void closeConnection() {
		try {
			if(passPointsUpdate != null) {
				passPointsUpdate.close();
				passPointsUpdate = null;
			}
			if(passPointsQuery != null) {
				passPointsQuery.close();
				passPointsQuery = null;
			}
			if(aptvQuery != null) {
				aptvQuery.close();
				aptvQuery = null;
			}
			if(normalBT9Query != null) {
				normalBT9Query.close();
				normalBT9Query = null;
			}
			if(passPointUpdate != null) {
				passPointUpdate.close();
				passPointUpdate = null;
			}
			if(passPriceUpdate != null) {
				passPriceUpdate.close();
				passPriceUpdate = null;
			}
			if(queryTeal != null) {
				queryTeal.close();
				queryTeal = null;
			}
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
	
	//passPointsUpdate
	public static PreparedStatement passPointsUpdate() {

		try {

			if (passPointsUpdate == null) {

				String query = "UPDATE BBROCK SET APAS = ? WHERE STOCKID = ? AND DATEID = ? ";

				passPointsUpdate = DB.getConnection().prepareStatement(query);
				

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return passPointsUpdate;
	}	
	
	
	//passPointsQuery
	public static PreparedStatement passPointsQuery() {

		try {

			if (passPointsQuery == null) {

				String query = "SELECT DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID>? AND DATEID<? AND CLOSE >? ORDER BY DATEID ASC";

				passPointsQuery = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return passPointsQuery;
	}	
	
	
	//aptvQuery
	public static PreparedStatement aptvQuery() {

		try {

			if (aptvQuery == null) {

				String query = "SELECT DATEID, APTV FROM BBROCK WHERE STOCKID = ? AND ABT9 = 1 ORDER BY DATEID ASC";

				aptvQuery = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return aptvQuery;
	}	
	
	//normalBT9Query
	public static PreparedStatement normalBT9Query() {

		try {

			if (normalBT9Query == null) {

				String query = "SELECT COUNT(*) FROM BBROCK WHERE STOCKID = ? AND BT9 = 9";

				normalBT9Query = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return normalBT9Query;
	}	
	//passPointUpdate
	public static PreparedStatement passPointUpdate() {

		try {

			if (passPointUpdate == null) {

				String query = "UPDATE BBROCK SET APAS = ?  WHERE STOCKID = ? AND DATEID = ?";

				passPointUpdate = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return passPointUpdate;
	}
	
	//passPriceUpdate

	public static PreparedStatement passPriceUpdate() {

		try {

			if (passPriceUpdate == null) {

				String query = "UPDATE BBROCK SET APTV = ?  WHERE STOCKID = ? AND DATEID = ?";

				passPriceUpdate = DB.getConnection().prepareStatement(query);

			}

			
		} catch (SQLException e) {
			e.printStackTrace(System.out);
		}

		return passPriceUpdate;
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

	//queryTeal
	public static PreparedStatement getTealQuery() {

		if (queryTeal == null) {
			try {

				String query = "SELECT TEAL FROM BBROCK WHERE STOCKID = ? AND DATEID = ?";

				queryTeal = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryTeal;
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
