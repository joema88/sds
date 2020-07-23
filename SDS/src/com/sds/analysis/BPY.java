package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class BPY {

	private static PreparedStatement queryPYStmnt = null;
	private static PreparedStatement updateBPYStmnt = null;

	private static void init() {
		queryPYStmnt = DB.getPYQueryStmnt();
		updateBPYStmnt = DB.getUpdateBPYStmnt();
	}

	public static int getPreviousDayBPY(String symbol, int stockID, int dateID) {
		int bpy = 0;
		try {
			queryPYStmnt.setInt(1, stockID);
			queryPYStmnt.setInt(2, dateID);
			ResultSet rs = queryPYStmnt.executeQuery();

			if (rs.next()) {
				bpy = rs.getInt(4);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return bpy;
	}

	public static int getCurrentDayPY(String symbol, int stockID, int dateID) {
		int yellow = 0;
		try {
			queryPYStmnt.setInt(1, stockID);
			queryPYStmnt.setInt(2, dateID);
			ResultSet rs = queryPYStmnt.executeQuery();

			if (rs.next()) {
				yellow = rs.getInt(2);
				int pink  = rs.getInt(1);
				if(yellow==1||pink==1) {
					yellow = 1;
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return yellow;
	}

	public static void processStockToday(String symbol, int stockID, int dateID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}

		int bpy = getPreviousDayBPY(symbol, stockID, dateID - 1);
		int yellow = getCurrentDayPY(symbol, stockID, dateID);
		try {
			if (yellow == 1) {
				updateBPYStmnt.setInt(1, bpy + yellow);
				updateBPYStmnt.setInt(2, stockID);
				updateBPYStmnt.setInt(3, dateID);
				updateBPYStmnt.executeUpdate();
			} else if (yellow == 0) {
				updateBPYStmnt.setInt(1, 0);
				updateBPYStmnt.setInt(2, stockID);
				updateBPYStmnt.setInt(3, dateID);
				updateBPYStmnt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStockHistory(String symbol, int stockID, int dateID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}

		int bpy = 0;
		try {
			queryPYStmnt.setInt(1, stockID);
			queryPYStmnt.setInt(2, dateID - 1);
			ResultSet rs = queryPYStmnt.executeQuery();

			while (rs.next()) {
				int pink = rs.getInt(1);
				int yellow = rs.getInt(2);
				dateID = rs.getInt(3);
				if (pink == 1||yellow==1) {
					bpy = bpy + 1;
				} else {
					bpy = 0;
				}

				updateBPYStmnt.setInt(1, bpy);
				updateBPYStmnt.setInt(2, stockID);
				updateBPYStmnt.setInt(3, dateID);
				updateBPYStmnt.executeUpdate();
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void main(String[] args) {
		for(int k=1; k<=6368; k++) {
		  processStockHistory(null, k, 0);
		  try {
			  Thread.sleep(2000);
		  }catch(Exception ex) {
			  
		  }
		}
	}

}
