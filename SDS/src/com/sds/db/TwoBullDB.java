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
   OR 20 DAYS ABOVE AVG20 IF SUM<5
   
 */
public class TwoBullDB extends DB {
	private static PreparedStatement findPreviousCCXStmnt = null;
	private static PreparedStatement queryCX520 = null;
	private static PreparedStatement updateCCXStmnt = null;
	private static PreparedStatement updateBDCXZero = null;
	private static PreparedStatement bdcxQueryStmnt = null;
	private static PreparedStatement queryEndPrice = null;
	private static PreparedStatement queryHighLowPrice = null;
	private static PreparedStatement updateBDW = null;
	private static PreparedStatement updateBDWZero = null;
	private static PreparedStatement bdwQueryStmnt = null;
	private static PreparedStatement updatePTCP2 = null;
	private static PreparedStatement queryLastDayCX520 = null;
	private static PreparedStatement bdcxNoZeroCount = null;
	private static PreparedStatement ptcp2HistoryStmnt = null;
	private static PreparedStatement queryNextCX520 = null;

	public static void closeConnection() {
		try {
			if( queryNextCX520 != null) {
				queryNextCX520.close();
				queryNextCX520 = null;
			}
			if(ptcp2HistoryStmnt != null) {
				ptcp2HistoryStmnt.close();
				ptcp2HistoryStmnt = null;
			}
			if(bdcxNoZeroCount != null) {
				bdcxNoZeroCount.close();
				bdcxNoZeroCount = null;
			}
			if(queryLastDayCX520 != null) {
				queryLastDayCX520.close();
				queryLastDayCX520 = null;
			}
			if(updatePTCP2 != null) {
				updatePTCP2.close();
				updatePTCP2 = null;
			}
			if(bdwQueryStmnt != null) {
				bdwQueryStmnt.close();
				bdwQueryStmnt = null;
			}
			if(updateBDWZero != null) {
				updateBDWZero.close();
				updateBDWZero = null;
			}
			if (queryHighLowPrice != null) {
				queryHighLowPrice.close();
				queryHighLowPrice = null;
			}
			if (queryEndPrice != null) {
				queryEndPrice.close();
				queryEndPrice = null;
			}

			if (bdcxQueryStmnt != null) {
				bdcxQueryStmnt.close();
				bdcxQueryStmnt = null;
			}
			if (updateBDCXZero != null) {
				updateBDCXZero.close();
				updateBDCXZero = null;
			}
			if (updateCCXStmnt != null) {
				updateCCXStmnt.close();
				updateCCXStmnt = null;
			}
			if (findPreviousCCXStmnt != null) {
				findPreviousCCXStmnt.close();
				findPreviousCCXStmnt = null;
			}
			if (queryCX520 != null) {
				queryCX520.close();
				queryCX520 = null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	public static PreparedStatement getQueryBeginPrice() {
		return getQueryEndPrice();
	}

	//queryNextCX520
	public static PreparedStatement getNextCX520Stmnt() {
		if (queryNextCX520 == null) {
			try {

				String query = "SELECT  DATEID, CLOSE FROM BBROCK WHERE  STOCKID = ? AND DATEID >= ? AND CX520 = ? ORDER BY DATEID ASC";

				queryNextCX520  = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return queryNextCX520;
	}
		
	//ptcp2HistoryStmnt
	public static PreparedStatement getPtcp2HistoryStmnt() {
		if (ptcp2HistoryStmnt == null) {
			try {

				String query = "SELECT PTCP2, DAY2, DATEID FROM BBROCK WHERE PTCP2 < -25.0 AND STOCKID = ? AND DATEID >= ? ORDER BY DATEID ASC";

				ptcp2HistoryStmnt  = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return ptcp2HistoryStmnt;
	}
	
	public static PreparedStatement getHighLowPrice() {
		if (queryHighLowPrice == null) {
			try {

				String query = "SELECT MAX(CLOSE),MIN(CLOSE) FROM BBROCK WHERE STOCKID = ? AND DATEID >= ? AND DATEID<= ?";

				queryHighLowPrice = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return queryHighLowPrice;
	}

	public static PreparedStatement getQueryEndPrice() {
		if (queryEndPrice == null) {
			try {

				String query = "SELECT CLOSE FROM BBROCK WHERE STOCKID = ? AND DATEID = ? ";

				queryEndPrice = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return queryEndPrice;
	}

	public static PreparedStatement getPTCP2UpdateStmnt() {
		if (updatePTCP2 == null) {
			try {

				String query = "UPDATE BBROCK SET PTCP2 = ?, DAY2 = ?  WHERE  STOCKID = ? AND DATEID = ? ";

				updatePTCP2 = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return updatePTCP2;
	}

	
	// BDW
	public static PreparedStatement getBDWUpdateStmnt() {
		if (updateBDW == null) {
			try {

				String query = "UPDATE BBROCK SET BDW = ? WHERE  STOCKID = ? AND DATEID = ? ";

				updateBDW = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return updateBDW;
	}


	public static PreparedStatement getBDWUpdateZero() {
		if (updateBDWZero == null) {
			try {

				String query = "UPDATE BBROCK SET BDW = 0 WHERE  STOCKID = ? AND DATEID >? AND DATEID<? AND BDW = ?";

				updateBDWZero = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return updateBDWZero;
	}

	public static PreparedStatement getBDCXUpdateZero() {
		if (updateBDCXZero == null) {
			try {

				String query = "UPDATE BBROCK SET BDCX = 0 WHERE  STOCKID = ? AND DATEID = ? ";

				updateBDCXZero = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return updateBDCXZero;
	}

	
	public static PreparedStatement getBDWQuery() {
		if (bdwQueryStmnt == null) {
			try {
            //take out 13 now, need to think how to handle it
				String query = "SELECT BDW, DATEID FROM BBROCK WHERE BDW<>0 AND STOCKID = ? ORDER BY DATEID DESC ";

				bdwQueryStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return bdwQueryStmnt;
	}

	public static PreparedStatement getBDCXNoZeroCount() {
		if (bdcxNoZeroCount == null) {
			try {
            //take out 13 now, need to think how to handle it
				String query = "SELECT COUNT(*) FROM BBROCK WHERE BDCX<>0 AND STOCKID = ? ";

				bdcxNoZeroCount = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return bdcxNoZeroCount;
	}
	
	public static PreparedStatement getBDCXQuery() {
		if (bdcxQueryStmnt == null) {
			try {
            //take out 13 now, need to think how to handle it
				String query = "SELECT BDCX, DATEID FROM BBROCK WHERE BDCX<>0 AND STOCKID = ? ORDER BY DATEID DESC ";

				bdcxQueryStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return bdcxQueryStmnt;
	}

	public static PreparedStatement getCCXUpdateStmnt() {
		if (updateCCXStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET CCX = ?, BDCX = ? WHERE  STOCKID = ? AND DATEID = ? ";

				updateCCXStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}
		return updateCCXStmnt;
	}

	public static PreparedStatement getLastDayCX520Stmnt() {

		if (queryLastDayCX520 == null) {
			try {

				String query = "SELECT CX520, DATEID FROM BBROCK WHERE  STOCKID = ? ORDER BY DATEID DESC limit 1 ";

				queryLastDayCX520 = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return queryLastDayCX520;
	}
	public static PreparedStatement getCX520HistoryStmnt() {

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

				String query = "SELECT CCX, BDCX FROM BBROCK WHERE  STOCKID = ? AND DATEID <? ORDER BY DATEID DESC LIMIT 1";

				findPreviousCCXStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findPreviousCCXStmnt;
	}

}
