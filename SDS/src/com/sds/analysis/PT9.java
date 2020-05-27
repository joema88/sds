package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class PT9 {

	private static PreparedStatement queryTealStmnt = null;
	private static PreparedStatement updateT9Stmnt = null;

	private static void init() {
		queryTealStmnt = DB.getTealQueryStmnt();
		updateT9Stmnt = DB.getUpdateT9Stmnt();
	}

	public static int getPreviousDayBT9(String symbol, int stockID, int dateID) {
		int bt9 = 0;
		try {
			queryTealStmnt.setInt(1, stockID);
			queryTealStmnt.setInt(2, dateID);
			ResultSet rs = queryTealStmnt.executeQuery();

			if (rs.next()) {
				bt9 = rs.getInt(3);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return bt9;
	}

	public static int getCurrentDayTeal(String symbol, int stockID, int dateID) {
		int teal = 0;
		try {
			queryTealStmnt.setInt(1, stockID);
			queryTealStmnt.setInt(2, dateID);
			ResultSet rs = queryTealStmnt.executeQuery();

			if (rs.next()) {
				teal = rs.getInt(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return teal;
	}

	public static void processStockToday(String symbol, int stockID, int dateID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}

		int bt9 = getPreviousDayBT9(symbol, stockID, dateID - 1);
		int teal = getCurrentDayTeal(symbol, stockID, dateID);
		try {
			updateT9Stmnt.setInt(1, bt9+teal);
			updateT9Stmnt.setInt(2, stockID);
			updateT9Stmnt.setInt(3, dateID);
			updateT9Stmnt.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStockHistory(String symbol, int stockID, int dateID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}

		int t9 = 0;
		try {
			queryTealStmnt.setInt(1, stockID);
			queryTealStmnt.setInt(2, dateID - 1);
			ResultSet rs = queryTealStmnt.executeQuery();

			while (rs.next()) {
				int teal = rs.getInt(1);
				dateID = rs.getInt(2);
				if (teal == 1) {
					t9 = t9 + teal;
				} else {
					t9 = 0;
				}

				updateT9Stmnt.setInt(1, t9);
				updateT9Stmnt.setInt(2, stockID);
				updateT9Stmnt.setInt(3, dateID);
				updateT9Stmnt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void main(String[] args) {

	}

}
