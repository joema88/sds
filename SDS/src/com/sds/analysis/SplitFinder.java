package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class SplitFinder {

	private static PreparedStatement markCapStmnt = null;

	private static void init() {
		markCapStmnt = DB.getMarkcapStmnt();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		processSplitHistory();

	}
	
	
	public static void processSplitHistory() {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				
			

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = endDateId; k >= strtDateId; k--) {
				for (int k = endDateId; k >= 8924; k--) {
					// for (int k = currentDateID; k >= currentDateID; k--) {
					boolean exist = false;
					int adjustment = 0;
					int lcMax = 10;
					int lc = 0;
					do {
						dateIDExistStmnt.setInt(1, stockID);
						dateIDExistStmnt.setInt(2, k);

						ResultSet dateIDExist = dateIDExistStmnt.executeQuery();

						dateIDExist.next();

						int count = dateIDExist.getInt(1);

						if (count > 0) {
							exist = true;
						} else {
							k--;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist)
						processSlpitStocks(k,stockID);
				}

				//System.out.println("process done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processSlpitStocks(int dateId, int stockId) {
		init();
		
		try {
			markCapStmnt.setInt(1, stockId);
			markCapStmnt.setInt(2, dateId);
			
			ResultSet rs = markCapStmnt.executeQuery();
			// String query = "SELECT DATEID,STOCKID,CLOSE,MARKCAP FROM
			// BBROCK WHERE STOCKID=? AND DATEID <= ? ORDER BY DATEID DESC LIMIT 2";

			float pClose = -10.0f;
			float pMarketCap = -10.0f;
			while (rs.next()) {

				float close = rs.getFloat(3);
				float marketCap = rs.getFloat(4);
				if (pClose < 0.0f) {
					pClose = close;
					pMarketCap = marketCap;
				} else {
					if (pMarketCap > 0.1f && marketCap > 0.1f) {
						float marketCapRatio = pMarketCap / marketCap;
						float priceRatio = pClose / close;

						if ((priceRatio > 1.2f * marketCapRatio) || (priceRatio < 0.8f * marketCapRatio)) {
							System.out.println("Split stock found at " + dateId + " StockID " + stockId);
							break;

						}else {
							pClose = close;
							pMarketCap = marketCap;
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

}
