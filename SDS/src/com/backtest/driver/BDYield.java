package com.backtest.driver;

import java.sql.*;
import java.util.*;

import com.sds.db.DB;
import java.io.*;

public class BDYield {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	private static int startDateID = 9007;
	private static int endDateID = 9025;
	private static float investAmount = 1000.00f;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	// private static int startDateID = 8707;
	// DATEID = 6695, count = 1311, 2011/7/22 starting point
	// private static int startDateID = 6695;
	private static PreparedStatement qualifiedLowStmnt = null;

	private static int totalSC = 0;
	private static int totalLC = 0;
	private static int totalHC = 0;
	private static int totalCount = 0;
	private static int totalSCRandom = 0;
	private static int totalCountRandom = 0;
	private static int totalRandomHC = 0;
	private static int totalRandomLC = 0;
	private static int incidentCount = 0;

	private static float peakYield = 0.0f;
	private static float troughYield = 0.0f;
	private static float peakYieldRandom = 0.0f;
	private static float troughYieldRandom = 0.0f;
	private static float yieldQaulified1 = 0.5f;
	private static float yieldQaulified2 = -0.5f;
	private static boolean debug = false;
	private static Hashtable excludeStocks = null;
	private static FileWriter fileWriter = null;

	public static void main(String[] args) {
		try {

			excludeStocks = new Hashtable();
			//sept stock split
			excludeStocks.put("ACET", "ACET");
			excludeStocks.put("AEHL", "AEHL");
			excludeStocks.put("APEX", "APEX");
			excludeStocks.put("AROW", "AROW");
			excludeStocks.put("BIOC", "BIOC");
			excludeStocks.put("HGSH", "HGSH");
			excludeStocks.put("EGLE", "EGLE");
			excludeStocks.put("GOVX", "GOVX");
			excludeStocks.put("GECC", "GECC");
			excludeStocks.put("HGEN", "HGEN");
			excludeStocks.put("MRNS", "MRNS");
			excludeStocks.put("NOG", "NOG");
			excludeStocks.put("SYTA", "SYTA");
			excludeStocks.put("SNSS", "SNSS");
			excludeStocks.put("TOMZ", "TOMZ");
			excludeStocks.put("TREX", "TREX");
			excludeStocks.put("WLL", "WLL");
			//OCT SPLIT
			excludeStocks.put("AXAS", "AXAS");
			excludeStocks.put("CHFS", "CHFS");
			excludeStocks.put("KDNY", "KDNY");
			excludeStocks.put("GLBS", "GLBS");
			excludeStocks.put("GECC", "GECC");
			excludeStocks.put("HPR", "HPR");
			excludeStocks.put("NTES", "NTES");
			excludeStocks.put("NEE", "NEE");
			excludeStocks.put("NVUS", "NVUS");
			excludeStocks.put("RUSHA", "RUSHA");
			excludeStocks.put("NCTY", "NCTY");
			excludeStocks.put("TC", "TC");
			long t1 = System.currentTimeMillis();
			PreparedStatement UTurn = DB.getUTurnStmnt();
			PreparedStatement nextDayPrice = DB.getPriceByDateID();
			float totalInvestment = 0.0f;
			float totalValueNow = 0.0f;
			Hashtable boughtStocks = new Hashtable();
			for (int k = startDateID; k < endDateID; k++) {

				// String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL,
				// MARKCAP,CLOSE,DPC, UPC, DM, FUC AS UTURN "
				// +" FROM BBROCK a, SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and
				// a.DATEID=c.DATEID "
				// +" and a.DATEID=? AND a.FUC>4 ORDER BY MARKCAP DESC";
				UTurn.setInt(1, k);
				ResultSet rs = UTurn.executeQuery();
				while (rs.next()) {

					int stockID = rs.getInt(2);
					String symbol = rs.getString(4);
					// buy this stock at next day close price
					// String query = "select CLOSE, a.DATEID,a.STOCKID, CDATE, b.SYMBOL FROM BBROCK
					// a, SYMBOLS b,DATES c WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and
					// a.STOCKID = ? AND a.DATEID=?";
					if (excludeStocks.containsKey(symbol)) {
						System.out.println("Exclude split stock "+symbol);
					}else if (!excludeStocks.containsKey(symbol)) {
						nextDayPrice.setInt(1, stockID);
						nextDayPrice.setInt(2, k + 1);

						ResultSet rs1 = nextDayPrice.executeQuery();
						if (rs1.next()) {
							float price = rs1.getFloat(1);
							String stockId = "" + rs1.getInt(3);
							String cdate = rs1.getString(4);
							String stock = rs1.getString(5);
							int buyShare = (int) (investAmount / price);
							totalInvestment = totalInvestment + buyShare * price;
							if (!boughtStocks.containsKey(stockId)) {
								boughtStocks.put(stockId, "" + buyShare);
							} else {
								System.out.println("********* Stock ID exist**********");
								int existShare = Integer.parseInt(boughtStocks.get(stockId).toString());
								int totalShare = existShare + buyShare;
								boughtStocks.put(stockId, "" + totalShare);
							}
							System.out.println("Buy stock " + stock + " on " + cdate + " at $" + price + " " + buyShare
									+ " shares for total investment of $" + buyShare * price);
						}
					}

				}

			}

			Enumeration en = boughtStocks.keys();
			while (en.hasMoreElements()) {
				int nextStockId = Integer.parseInt(en.nextElement().toString());
				int shares = Integer.parseInt(boughtStocks.get("" + nextStockId).toString());

				nextDayPrice.setInt(1, nextStockId);
				nextDayPrice.setInt(2, endDateID);

				ResultSet rs2 = nextDayPrice.executeQuery();
				if (rs2.next()) {
					float price = rs2.getFloat(1);
					totalValueNow = totalValueNow + price * shares;
				}

			}

			float yield = 100.0f * (totalValueNow - totalInvestment) / totalInvestment;
			System.out.println("");
			System.out.println("------------  Result -------------");
			System.out.println("Total investment amount is " + totalInvestment + " total value now " + totalValueNow);
			System.out.println("The investment yield now is " + yield + "%");

			// then check SPY Yield 6183

			float spyStart = 0.0f;
			float spyEnd = 0.0f;
			nextDayPrice.setInt(1, 6183);
			nextDayPrice.setInt(2, startDateID + 1);
			ResultSet rs3 = nextDayPrice.executeQuery();
			if (rs3.next()) {
				spyStart = rs3.getFloat(1);
			}

			nextDayPrice.setInt(1, 6183);
			nextDayPrice.setInt(2, endDateID);
			ResultSet rs4 = nextDayPrice.executeQuery();
			if (rs4.next()) {
				spyEnd = rs4.getFloat(1);
			}
			float spyYield = 100.0f * (spyEnd - spyStart) / spyStart;
			System.out.println("");
			System.out.println(
					"This compares to SPY buy at " + spyStart + " and hold at " + spyEnd + " yeild " + spyYield + "%");
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void checkYield(PreparedStatement dailyPrice, int dateID, int holdDays, int stockId) {
		try {
			incidentCount++;
			dailyPrice.setInt(1, stockId);
			dailyPrice.setInt(2, dateID);
			dailyPrice.setInt(3, dateID + holdDays);

			ResultSet rs = dailyPrice.executeQuery();

			float startPrice = 0.0f;
			float minPrice = 0.0f;
			float maxPrice = 0.0f;
			int minDays = 0;
			int maxDays = 0;

			int count = 0;
			boolean exist = false;
			while (rs.next()) {
				float price = rs.getFloat(1);
				int dateId = rs.getInt(2);
				if (startPrice < 0.001f) {
					startPrice = price;
					minPrice = price;
					maxPrice = price;
				}

				if (price > maxPrice) {
					maxPrice = price;
					maxDays = count;
					exist = true;
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
					exist = true;
				}

				count++;
			}

			if (exist) {
				float pYield = (maxPrice - startPrice) / startPrice;
				float tYield = (minPrice - startPrice) / startPrice;
				if (pYield > yieldQaulified1) {
					totalHC++;
					totalSC++;
					if (pYield > 3.0f && debug)
						System.out.println("Find suspected high yield " + pYield + " for stock " + stockId);
					if (debug)
						System.out.println("Find up qualified yield " + pYield + " for stock " + stockId);
				}

				if (pYield < yieldQaulified2) {
					totalSC++;
					totalLC++;
					if (pYield < -0.8f && debug)
						System.out.println("Find suspected low yield " + pYield + " for stock " + stockId);

					if (debug)
						System.out.println("Find down qualified yield " + pYield + " for stock " + stockId);
				}

				peakYield = peakYield + pYield;
				troughYield = troughYield + tYield;
				totalCount++;
				if (debug) {
					System.out.println("Average peak yield " + 100.0f * peakYield / totalCount);
					System.out.println(
							"TotalCount " + totalCount + " peakYield " + peakYield + " troughYield " + troughYield);
					System.out.println("Average trough yield " + 100.0f * troughYield / totalCount);
					System.out.println("Success rate " + totalSC + "/" + totalCount);
				} else {
					if (debug)
						System.out.println("0, " + incidentCount + ", " + totalSC + ", "
								+ 100.0f * peakYield / totalCount + ", " + 100.0f * troughYield / totalCount);
				}

				int compareStockID = 0;
				do {
					compareStockID = DB.getParallelStock(dateID, stockId);
				} while (excludeStocks.containsKey("" + compareStockID));

				checkRandomYield(dailyPrice, dateID, holdDays, compareStockID);
			}

		} catch (Exception ex) {

		}
	}

	public static void checkRandomYield(PreparedStatement dailyPrice, int dateID, int holdDays, int stockId) {
		try {
			dailyPrice.setInt(1, stockId);
			dailyPrice.setInt(2, dateID);
			dailyPrice.setInt(3, dateID + holdDays);

			ResultSet rs = dailyPrice.executeQuery();

			float startPrice = 0.0f;
			float minPrice = 0.0f;
			float maxPrice = 0.0f;
			int minDays = 0;
			int maxDays = 0;

			int count = 0;
			boolean exist = false;

			while (rs.next()) {
				float price = rs.getFloat(1);
				int dateId = rs.getInt(2);
				if (startPrice < 0.001f) {
					startPrice = price;
					minPrice = price;
					maxPrice = price;
				}

				if (price > maxPrice) {
					maxPrice = price;
					maxDays = count;
					exist = true;
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
					exist = true;
				}

				count++;
			}

			if (exist) {
				float pYield = (maxPrice - startPrice) / startPrice;
				float tYield = (minPrice - startPrice) / startPrice;
				if (pYield > yieldQaulified1) {
					totalSCRandom++;
					totalRandomHC++;
					if (pYield > 3.0f && debug)
						System.out.println("Find suspected random high yield " + pYield + " for stock " + stockId);

				}

				if (pYield < yieldQaulified2) {
					totalSCRandom++;
					totalRandomLC++;
					if (pYield < -0.8f && debug)
						System.out.println("Find suspected random low yield " + pYield + " for stock " + stockId);

				}
				peakYieldRandom = peakYieldRandom + pYield;
				troughYieldRandom = troughYieldRandom + tYield;
				totalCountRandom++;

				if (debug) {
					System.out.println("Average peak random yield " + 100.0f * peakYieldRandom / totalCountRandom);
					System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);

					System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);
				} else {
					System.out.print(incidentCount + ", " + totalSC + ", " + 100.0f * peakYield / totalCount + ", "
							+ 100.0f * troughYield / totalCount);
					System.out.println(", " + totalSCRandom + ", " + 100.0f * peakYieldRandom / totalCountRandom + ", "
							+ 100.0f * troughYieldRandom / totalCountRandom);

					fileWriter.write(incidentCount + ", " + totalSC + ", " + 100.0f * peakYield / totalCount + ", "
							+ 100.0f * troughYield / totalCount + ", " + totalSCRandom + ", "
							+ 100.0f * peakYieldRandom / totalCountRandom + ", "
							+ 100.0f * troughYieldRandom / totalCountRandom + "\n");
					fileWriter.flush();

				}
			}
		} catch (Exception ex) {

		}
	}

}
