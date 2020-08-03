package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class ColorSummary {

	private static PreparedStatement queryColorSumStmnt = null;
	private static PreparedStatement updateColorSumStmnt = null;
	private static PreparedStatement updateColorOMStmnt = null;
	private static PreparedStatement noColorSumStmnt = null;
	private static PreparedStatement updateColorRankingStmnt = null;
	private static PreparedStatement colorCalSumStmnt = null;
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	private static int startDateID = 8455;
	private static boolean oldStocksCal = true;
	private static PreparedStatement dateIDStartStmnt = null;
	private static PreparedStatement oldStocks = null;
	private static int oldStockSelectDateId = 8400;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	// private static int startDateID = 8707;
	// correct daily cal
	// private static int startDateID = 8964;
	private static void init() {
		if (queryColorSumStmnt == null) {
			queryColorSumStmnt = DB.getColorSumStmnt();
			updateColorSumStmnt = DB.getUpdateColorSumStmnt();
			updateColorOMStmnt = DB.getOMColorUpdateStmnt();
			noColorSumStmnt = DB.getNoColorSumStmnt();
			colorCalSumStmnt = DB.getColorCalSumStmnt();
			updateColorRankingStmnt = DB.updateColorRankingStmnt();
			dateIDStartStmnt = DB.getDateIDStarttmnt();
			oldStocks = DB.getOldStockIDs();
		}

	}

	public static void main(String[] args) {
		try {
			init();
			long t1 = System.currentTimeMillis();
			ResultSet rs = null;
			if (!oldStocksCal) {
				PreparedStatement allStocks = DB.getAllStockIDs();
				allStocks.setInt(1, 1);
				rs = allStocks.executeQuery();
			} else {
				oldStocks.setInt(1, 8400);
				rs = oldStocks.executeQuery();
				
			}

			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				startDateID  = getStockStartDateID(stockID);
				System.out.println("StockID "+stockID +" startDateID "+startDateID);
				for (int k = startDateID+252; k <= 8966; k++) {
					//if (k < 8964) {

					//} else {
						updateColorSummary(stockID, k);
						updateOMColorSummary(stockID, k);
						updateColorRanking(stockID, k);
					//}
				}
				long t2 = System.currentTimeMillis();
				System.out.println(sc + " total stocks processed, time cost " + (t2 - t1) / (1000 * 60));
				Thread.sleep(2000);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static int getStockStartDateID(int stockID) {
		int startDateId = 0;
		try {
			dateIDStartStmnt.setInt(1, stockID);
			ResultSet rsd = dateIDStartStmnt.executeQuery();
			rsd.next();
			startDateId = rsd.getInt(1);
			rsd.close();
			rsd = null;
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return startDateId;

	}

	public static void updateColorRanking(int stockID, int dateId) {
		try {
			init();

			colorCalSumStmnt.setInt(1, stockID);
			colorCalSumStmnt.setInt(2, dateId);

			ResultSet rs = colorCalSumStmnt.executeQuery();

			if (rs.next()) {
				// TSM, YSM, PSM, DSM, TOM, POM,YOM
				int tSum = rs.getInt(1);
				int ySum = rs.getInt(2);
				int pSum = rs.getInt(3);
				int dSUM = rs.getInt(4);
				int tom = rs.getInt(5);
				int pom = rs.getInt(6);
				int yom = rs.getInt(7);
				rs.close();
				rs = null;

				colorCalSumStmnt.setInt(1, stockID);
				colorCalSumStmnt.setInt(2, dateId - 252);

				ResultSet rs1 = colorCalSumStmnt.executeQuery();

				if (rs1.next()) {
					int tSum1 = rs1.getInt(1);
					int ySum1 = rs1.getInt(2);
					int pSum1 = rs1.getInt(3);
					int dSUM1 = rs1.getInt(4);
					rs1.close();
					rs1 = null;

					// (TSM-YSM-2*PSM)/DSM, (TOM-YOM-2*POM)/25
					float mor = 1.0f * (tom - yom - 2 * pom) / 25.0f;
					float trk = 1.0f * (tSum - ySum - 2 * pSum) / (1.0f * dSUM);
					float yor = 1.0f * ((tSum - tSum1) - (ySum - ySum1) - 2 * (pSum - pSum1)) / (1.0f * (dSUM - dSUM1));

					updateColorRankingStmnt.setFloat(1, mor);
					updateColorRankingStmnt.setFloat(2, yor);
					updateColorRankingStmnt.setFloat(3, trk);
					updateColorRankingStmnt.setInt(4, stockID);
					updateColorRankingStmnt.setInt(5, dateId);

					updateColorRankingStmnt.executeUpdate();

				} // end if(rs1.next())

			} // end if(rs.next())

		} catch (Exception ex) {

		}
	}

	public static void updateOMColorSummary(int stockID, int dateId) {
		try {
			init();
			int tSum = 0;
			int ySum = 0;
			int pSum = 0;
			int nSum = 0;
			queryColorSumStmnt.setInt(1, stockID);
			queryColorSumStmnt.setInt(2, dateId - 24);
			queryColorSumStmnt.setInt(3, dateId);

			ResultSet rs = queryColorSumStmnt.executeQuery();

			if (rs.next()) {
				tSum = rs.getInt(1);
				ySum = rs.getInt(2);
				pSum = rs.getInt(3);
				rs.close();
				rs = null;
			}

			noColorSumStmnt.setInt(1, stockID);
			noColorSumStmnt.setInt(2, dateId - 24);
			noColorSumStmnt.setInt(3, dateId);

			ResultSet rs1 = noColorSumStmnt.executeQuery();

			if (rs1.next()) {
				nSum = rs1.getInt(1);
				rs1.close();
				rs1 = null;
			}

			updateColorOMStmnt.setInt(1, tSum);
			updateColorOMStmnt.setInt(2, nSum);
			updateColorOMStmnt.setInt(3, ySum);
			updateColorOMStmnt.setInt(4, pSum);
			updateColorOMStmnt.setInt(5, stockID);
			updateColorOMStmnt.setInt(6, dateId);

			updateColorOMStmnt.executeUpdate();
		} catch (Exception ex) {

		}
	}

	public static void updateColorSummary(int stockID, int dateId) {
		try {
			init();
			int tSum = 0;
			int ySum = 0;
			int pSum = 0;
			int nSum = 0;
			int dSum = 0;
			queryColorSumStmnt.setInt(1, stockID);
			queryColorSumStmnt.setInt(2, startDateID);
			queryColorSumStmnt.setInt(3, dateId);

			ResultSet rs = queryColorSumStmnt.executeQuery();

			if (rs.next()) {
				tSum = rs.getInt(1);
				ySum = rs.getInt(2);
				pSum = rs.getInt(3);
				dSum = rs.getInt(4);
				rs.close();
				rs = null;
			}

			noColorSumStmnt.setInt(1, stockID);
			noColorSumStmnt.setInt(2, startDateID);
			noColorSumStmnt.setInt(3, dateId);

			ResultSet rs1 = noColorSumStmnt.executeQuery();

			if (rs1.next()) {
				nSum = rs1.getInt(1);
				rs1.close();
				rs1 = null;
			}

			updateColorSumStmnt.setInt(1, tSum);
			updateColorSumStmnt.setInt(2, nSum);
			updateColorSumStmnt.setInt(3, ySum);
			updateColorSumStmnt.setInt(4, pSum);
			updateColorSumStmnt.setInt(5, dSum);
			updateColorSumStmnt.setInt(6, stockID);
			updateColorSumStmnt.setInt(7, dateId);

			updateColorSumStmnt.executeUpdate();
		} catch (Exception ex) {

		}
	}

}
