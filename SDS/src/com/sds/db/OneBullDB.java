package com.sds.db;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class OneBullDB extends DB {

	private static PreparedStatement dayLengthStmnt = null;
	private static PreparedStatement querySC5SumStmnt = null;
	private static PreparedStatement aboveSC5CountStmnt = null;
	private static PreparedStatement pt9BullStmnt = null;
	private static PreparedStatement findPreviousPT9Stmnt = null;
	private static PreparedStatement findMaxClose = null;
	private static PreparedStatement findMinClose = null;
	private static PreparedStatement updateMaxStmnt = null;
	private static PreparedStatement boundaryQueryStmnt = null;
	private static PreparedStatement updateBullPointStmnt = null;
	private static PreparedStatement findPreviousYP10ZeroStmnt = null;

	public static void closeConnection() {
		try {
			if (findPreviousYP10ZeroStmnt != null) {
				findPreviousYP10ZeroStmnt.close();
				findPreviousYP10ZeroStmnt = null;
			}
			if (updateBullPointStmnt != null) {
				updateBullPointStmnt.close();
				updateBullPointStmnt = null;
			}
			if (dayLengthStmnt != null) {
				dayLengthStmnt.close();
				dayLengthStmnt = null;
			}
			if (aboveSC5CountStmnt != null) {
				aboveSC5CountStmnt.close();
				aboveSC5CountStmnt = null;
			}
			if (querySC5SumStmnt != null) {
				querySC5SumStmnt.close();
				querySC5SumStmnt = null;
			}
			if (findPreviousPT9Stmnt != null) {
				findPreviousPT9Stmnt.close();
				findPreviousPT9Stmnt = null;
			}
			if (boundaryQueryStmnt != null) {
				boundaryQueryStmnt.close();
				boundaryQueryStmnt = null;
			}
			if (updateMaxStmnt != null) {
				updateMaxStmnt.close();
				updateMaxStmnt = null;
			}
			if (pt9BullStmnt != null) {
				pt9BullStmnt.close();
				pt9BullStmnt= null;
			}
			if (findMaxClose != null) {
				findMaxClose.close();
				findMaxClose= null;
			}
			if (findMinClose != null) {
				findMinClose.close();
				findMinClose= null;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//aboveSC5CountStmnt
	public static PreparedStatement getAboveSC5CountStmnt() {
		
		if (aboveSC5CountStmnt == null) {
			try {

				String query = "SELECT COUNT(*) FROM BBROCK WHERE STOCKID = ? AND DATEID <=? AND DATEID>? AND SC5>=? ";
				
				aboveSC5CountStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return aboveSC5CountStmnt;
	}
	
	//querySC5SumStmnt
	public static PreparedStatement getSC5SumStmnt() {
		
		if (querySC5SumStmnt == null) {
			try {

				String query = "SELECT SUM(SC5) FROM BBROCK WHERE STOCKID = ? AND DATEID <=? AND DATEID>? ";
				
				querySC5SumStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return querySC5SumStmnt;
	}
	
	
public static PreparedStatement getPreviousYP10OneStmnt() {
		
		if (findPreviousYP10ZeroStmnt == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK WHERE YP10 <=1 AND STOCKID = ? AND DATEID <=? ORDER BY DATEID DESC LIMIT 1";
				
				findPreviousYP10ZeroStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findPreviousYP10ZeroStmnt;
	}
	

	public static PreparedStatement getPreviousPT9Stmnt() {
		
		if (findPreviousPT9Stmnt == null) {
			try {

				String query = "SELECT DATEID FROM BBROCK WHERE BT9=9 AND STOCKID = ? AND DATEID <=? ORDER BY DATEID DESC LIMIT 1";
				
				findPreviousPT9Stmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findPreviousPT9Stmnt;
	}
	
	public static PreparedStatement getBoundaryLengthStmnt() {
		
		if (dayLengthStmnt == null) {
			try {

				String query = "SELECT COUNT(*) FROM BBROCK WHERE STOCKID = ? AND DATEID <? AND DATEID>=? ";
				
				dayLengthStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return dayLengthStmnt;
	}
	
	public static PreparedStatement getNextBoundaryStmnt(int stockID, int SC5, int dateID) {
		
		if (boundaryQueryStmnt == null) {
			try {

				String query = "SELECT SC5, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID <=? AND SC5<0 ORDER BY DATEID DESC limit 1";
				if(SC5 < 0){
					query = "SELECT SC5, DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID <=? AND SC5>0 ORDER BY DATEID DESC limit 1";
				}
				boundaryQueryStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return boundaryQueryStmnt;
	}
	
	//updateBullPointStmnt
	public static PreparedStatement getUpdateBullPointStmnt() {
		
		if (updateBullPointStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET PTCP = ?, TSC5 = ?, DAYS = ?, GT10 = ?, GT6 = ? WHERE STOCKID = ? AND DATEID =? ";
				updateBullPointStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateBullPointStmnt;
	}
	
	
	
	public static PreparedStatement getUpdateMaxStmnt() {
		
		if (updateMaxStmnt == null) {
			try {

				String query = "UPDATE BBROCK SET PTVAL = ? WHERE STOCKID = ? AND DATEID =? ";
				updateMaxStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return updateMaxStmnt;
	}
	
	
	public static PreparedStatement getMinCloseFindStmnt() {
		
		if (findMinClose == null) {
			try {

				String query = "SELECT MIN(CLOSE), DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID >=? AND DATEID <=? GROUP BY DATEID ORDER BY MIN(CLOSE) ASC";
				findMinClose = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findMinClose;
	}
	
	
	public static PreparedStatement getMaxCloseFindStmnt() {
	
		if (findMaxClose == null) {
			try {

				String query = "SELECT MAX(CLOSE), DATEID FROM BBROCK WHERE STOCKID = ? AND DATEID >=? AND DATEID <=? GROUP BY DATEID ORDER BY MAX(CLOSE) DESC";
				findMaxClose = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return findMaxClose;
	}
	
	
	
	public static PreparedStatement getPT9BullStmnt(int stockID, int dateID, int limit) {
	
		if (pt9BullStmnt == null) {
			try {

				////the SC5>=8 condition could be SC5>=0 for big cap like MSFT?
				String query = "SELECT STOCKID, DATEID, SC5 FROM BBROCK WHERE SC5>=0 AND BT9 = 9 AND STOCKID = ? AND DATEID =? ";

				if(stockID>0 && dateID>0) {
					query = "SELECT STOCKID, DATEID, SC5 FROM BBROCK WHERE SC5>=0 AND BT9 = 9 AND STOCKID = ? AND DATEID =? ";

				}else if(stockID<=0 && dateID>0) {
					query = "SELECT STOCKID, DATEID, SC5 FROM BBROCK WHERE  SC5>=0 AND BT9 = 9 AND DATEID =? ";

				}else if(stockID>0 && dateID<=0 && limit>0) {
					query = "SELECT STOCKID, DATEID, SC5 FROM BBROCK WHERE  SC5>=0 AND BT9 = 9 AND STOCKID =? ORDER BY DATEID DESC limit ?";

				}else if(stockID>0 && dateID<=0 && limit<=0) {
					query = "SELECT STOCKID, DATEID, SC5 FROM BBROCK WHERE  SC5>=0 AND BT9 = 9 AND STOCKID =? ORDER BY DATEID DESC";

				}
				
				pt9BullStmnt = DB.getConnection().prepareStatement(query);
			} catch (SQLException e) {
				e.printStackTrace(System.out);
			}
		}

		return pt9BullStmnt;
	}

}
