package com.sds.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
 * Observation: if after teal9 +5% continued for 10days then drop into bear or correction
   then wait AVG5>AVG20 for 10 continuous days, before buy. This could be W recover 
   or V recover, both applicable. TNDM, WST, MSFT. The uptrend should not have AVG5<AVG20
   to make this rule more accurate.
   Alternatively: 30 day plus AVG5>AVG20 with 1 Teal9, drop, then AVG5>AVG20 for days buy
   BIG
   1. Be careful when the close price falls below AVG20 line, especially at the end,
   usually stock behavior will result in further leg down...(MANUAL CHECK)
   2. THE YELLOW, TEAL, PINK SUM SHOULD BE > 5 FOR THE LAST 10 BARS, IF NOT WAIT FOR DAYS
   
 */
public class TwoBullDB extends DB {
	private static PreparedStatement findPreviousCCXStmnt = null;
	private static PreparedStatement queryCX520 = null;
	private static PreparedStatement updateCCXStmnt = null;

	public static void closeConnection() {
		try {
			if(updateCCXStmnt != null) {
				updateCCXStmnt.close();
				updateCCXStmnt = null;
			}
			if(findPreviousCCXStmnt != null) {
				findPreviousCCXStmnt.close();
				findPreviousCCXStmnt = null;
			}
			if(queryCX520 != null) {
				queryCX520.close();
				queryCX520 = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static PreparedStatement getCCXUpdateStmnt() {
	   if(updateCCXStmnt == null) {
		   try {

				String query = "UPDATE BBROCK SET CCX = ? WHERE  STOCKID = ? AND DATEID = ? ";

				updateCCXStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
	   }
	   return updateCCXStmnt;
	}
	
	public static PreparedStatement getCurrentCX520Stmnt() {

		if (queryCX520 == null) {
			try {

				String query = "SELECT CX520, DATEID FROM BBROCK WHERE  STOCKID = ? ORDER BY DATEID ASC ";

				queryCX520 = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryCX520;
	}

	
	public static PreparedStatement getPreviousCCXStmnt() {

		if (findPreviousCCXStmnt == null) {
			try {

				String query = "SELECT CCX FROM BBROCK WHERE  STOCKID = ? AND DATEID <? ORDER BY DATEID DESC LIMIT 1";

				findPreviousCCXStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findPreviousCCXStmnt;
	}

}
