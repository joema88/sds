package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;
import com.sds.db.TwoBullDB;

public class Summary {

	private static PreparedStatement queryStmnt = null;
	private static PreparedStatement typSumQueryStmnt = null;
	private static PreparedStatement scUpdateStmnt = null;
	private static PreparedStatement yp10SumCalStmnt = null;
	private static PreparedStatement yp10SumUpdateStmnt = null;
	private static PreparedStatement queryCX520 = null;
	private static PreparedStatement findPreviousCCX = null;
	private static PreparedStatement updateCCX = null;

	public static void init() {
		queryStmnt = DB.getSymbolDateIDQueryStmnt();
		typSumQueryStmnt = DB.getTYPDSumQueryStmnt();
		scUpdateStmnt = DB.getSCUpdateStmnt();
		yp10SumCalStmnt = DB.getYP10SumCalStmnt();
		yp10SumUpdateStmnt = DB.getYP10SumUpdateStmnt();
		queryCX520 = TwoBullDB.getCurrentCX520Stmnt();
		findPreviousCCX = TwoBullDB.getPreviousCCXStmnt();
		updateCCX = TwoBullDB.getCCXUpdateStmnt();

	}

	public static void processDailyStocks(String date) {
		init();
		int dateID = DB.getDateID(date);

	}
	
	public static void processCCXHistory(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			queryCX520.setInt(1, stockID);
			ResultSet rs4 = queryCX520.executeQuery();
			while(rs4.next()) {
				int cx520 = rs4.getInt(1);
				int dateID = rs4.getInt(2);
				findPreviousCCX.setInt(1, stockID);
				findPreviousCCX.setInt(2, dateID);
				ResultSet rs5 = findPreviousCCX.executeQuery();
				if (rs5.next()) {
					int pccx = rs5.getInt(1);
					int ccx = 0;
					if((pccx>0 && cx520>0)||(pccx<0 && cx520<0)) {
						ccx = pccx + cx520;
					}else if((pccx>0 && cx520<0)||(pccx<0 && cx520>0)) {
						ccx = cx520;
					}else if(pccx == 0) {
						ccx = cx520;
					}
					
					updateCCX.setInt(1, ccx);
					updateCCX.setInt(2, stockID);
					updateCCX.setInt(3, dateID);
					updateCCX.executeUpdate();

				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStock(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			queryStmnt.setString(1, symbol);
			ResultSet rs = queryStmnt.executeQuery();

			while (rs.next()) {
				int dateID = rs.getInt(1);
				typSumQueryStmnt.setString(1, symbol);
				typSumQueryStmnt.setInt(2, dateID - 4);
				typSumQueryStmnt.setInt(3, dateID);
				ResultSet rs2 = typSumQueryStmnt.executeQuery();
				if (rs2.next()) {
					int tealSum = rs2.getInt(1);
					int yellowSum = rs2.getInt(2);
					int pinkSum = rs2.getInt(3);
					int sc5 = 2 * tealSum - 2 * yellowSum - 5 * pinkSum;

					scUpdateStmnt.setInt(1, sc5);
					scUpdateStmnt.setInt(2, stockID);
					scUpdateStmnt.setInt(3, dateID);
					scUpdateStmnt.executeUpdate();

				}

				yp10SumCalStmnt.setInt(1, stockID);
				yp10SumCalStmnt.setInt(2, dateID - 9);
				yp10SumCalStmnt.setInt(3, dateID);
				ResultSet rs3 = yp10SumCalStmnt.executeQuery();
				if (rs3.next()) {
					int yellowSum = rs3.getInt(1);
					int pinkSum = rs3.getInt(2);
					int YP10 = yellowSum + 2 * pinkSum;
					yp10SumUpdateStmnt.setInt(1, YP10);
					yp10SumUpdateStmnt.setInt(2, stockID);
					yp10SumUpdateStmnt.setInt(3, dateID);
					yp10SumUpdateStmnt.executeUpdate();

				}

			

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void main(String[] args) {

	}

}
