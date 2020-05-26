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

	public static void processStock(String symbol) {
		init();
		int stockID = DB.getSymbolID(symbol);
		int t9 = 0;
		try {
			queryTealStmnt.setInt(1, stockID);
			ResultSet rs = queryTealStmnt.executeQuery();

			while (rs.next()) {
				int teal = rs.getInt(1);
				int dateID = rs.getInt(2);
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
