package com.backtest.driver;

import java.sql.*;
import java.util.*;

import com.sds.db.DB;

public class BuyLow {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	// private static int startDateID = 8455;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	//private static int startDateID = 8707;
	// DATEID = 6695, count = 1311, 2011/7/22 starting point
		private static int startDateID = 6695;
	private static PreparedStatement qualifiedLowStmnt = null;

	private static int eDays = 40;
	private static int tDays = 36;
	private static int daysForHold = 250; // 3 months
	private static int totalSC = 0;
	private static int totalLC = 0;
	private static int totalHC = 0;
	private static int totalCount = 0;
	private static int totalSCRandom = 0;
	private static int totalCountRandom = 0;
	private static int totalRandomHC = 0;
	private static int totalRandomLC = 0;

	private static float peakYield = 0.0f;
	private static float troughYield = 0.0f;
	private static float peakYieldRandom = 0.0f;
	private static float troughYieldRandom = 0.0f;
	private static float yieldQaulified1 = 0.5f;
	private static float yieldQaulified2 = -0.5f;
	private static boolean debug = true;
	private static Hashtable excludeStocks = null;

	public static void main(String[] args) {
		try {

			excludeStocks = new Hashtable();
			excludeStocks.put("896", "896");
			excludeStocks.put("4680", "4680");
			excludeStocks.put("4418", "4418");
			excludeStocks.put("4692", "4692");
			excludeStocks.put("4113", "4113");
			excludeStocks.put("4704", "4704");
			excludeStocks.put("4062", "4062");
			excludeStocks.put("4034", "4034");
			excludeStocks.put("4702", "4702");
			excludeStocks.put("3116", "3116");
			excludeStocks.put("2631", "2631");// okay, 2631
			excludeStocks.put("4691", "4691");
			excludeStocks.put("4262", "4262");
			excludeStocks.put("4264", "4264");
			excludeStocks.put("4945", "4945");
			excludeStocks.put("4487", "4487");
			excludeStocks.put("4529", "4529");
			excludeStocks.put("4533", "4533");
			excludeStocks.put("4551", "4551");
			excludeStocks.put("4740", "4740");
			excludeStocks.put("5196", "5196");
			excludeStocks.put("4584", "4584");
			excludeStocks.put("4635", "4635");
			excludeStocks.put("4949", "4949");
			excludeStocks.put("4892", "4892");
			excludeStocks.put("4667", "4667");
			excludeStocks.put("5991", "5991");
			excludeStocks.put("4862", "4862");
			excludeStocks.put("4690", "4690");
			excludeStocks.put("5238", "5238");
			excludeStocks.put("5238", "5238");
			excludeStocks.put("5158", "5158");
			excludeStocks.put("4683", "4683");
			excludeStocks.put("4758", "4758");
			excludeStocks.put("4763", "4763");
			excludeStocks.put("5881", "5881");
			excludeStocks.put("5376", "5376");
			excludeStocks.put("5488", "5488");
			// add WIMI, 3775 all zero case to bull, to new to caluculate
			
			excludeStocks.put("5372", "5372");
			excludeStocks.put("702", "702");
			excludeStocks.put("3966", "3966");
			excludeStocks.put("4668", "4668");
			excludeStocks.put("4882", "4882");
			excludeStocks.put("4958", "4958");
			excludeStocks.put("5030", "5030");
			excludeStocks.put("5047", "5047");
			excludeStocks.put("5393", "5393");
			excludeStocks.put("5851", "5851");
			excludeStocks.put("5856", "5856");

			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();

			qualifiedLowStmnt = DB.getQualifiedLowStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				if (!excludeStocks.containsKey("" + stockID))
					for (int k = startDateID; k <= 8979; k++) {
						// String query = "select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, CLOSE,
						//UPC, UDS, DPC, DDS FROM BBROCK a, SYMBOLS b,DATES c  
						//WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and a.STOCKID=?  and  a.DATEID = ? ";

					    float upc = 0.0f;
					    float dpc = 0.0f;
					    int uds = 0;
					    int dds = 0;
					    float cPrice = 0.0f;
					    
						qualifiedLowStmnt.setInt(1, stockID);
						qualifiedLowStmnt.setInt(2, k);

						ResultSet rs1 = qualifiedLowStmnt.executeQuery();
					
						int toleranceCount = 0;
						while (rs1.next()) {
							cPrice = rs1.getFloat(5);
							upc = rs1.getFloat(6);
							uds = rs1.getInt(7);
							dpc = rs1.getFloat(8);
							dds = rs1.getInt(9);
						

							//(TOM-YOM-2*POM)/25>0.01??
							//if (upc<0.0001&&upc>-0.0001&&dpc<-25.0f && dds>=20 && cPrice>0.05 ) { //lowest close point of last 30 days with -20% drop
							if (dpc<-75.0f && dds<=30 && cPrice>0.05 ) { 
						//	if (dpc<-60.0f && cPrice>0.05 ) { 
							if ((k + 39) < 8980) {
									if (debug)
										System.out.println("Found candidate " + stockID + " at dateID " + k);
									checkYield(dailyPrice, k, daysForHold, stockID);
									//naturally increase as we could not predict the lowest point at this time
									k = k + 5;
								}
								break;
							}
						}

					}
				long t2 = System.currentTimeMillis();
				if (sc % 100 == 0)
					System.out.println(sc + " total stocks processed, time cost " + (t2 - t1) / (1000 * 60));
				Thread.sleep(1000);
			}
			long t3 = System.currentTimeMillis();
			System.out.println(sc + " stocks have been processed, time cost in minutes... " + (t3 - t1) / (1000 * 60));
			System.out.println("Average peak yield " + 100.0f * peakYield / totalCount);
			System.out.println("Average trough yield " + 100.0f * troughYield / totalCount);
			System.out.println("totalHC: " + totalHC + "  totalLC: " + totalLC);

			System.out.println("Success rate " + totalSC + "/" + totalCount);
			System.out.println("Average peak random yield " + 100.0f * peakYieldRandom / totalCountRandom);
			System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);
			System.out.println("totalRandomHC: " + totalRandomHC + "  totalRandomLC: " + totalRandomLC);

			System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);
			Thread.sleep(100);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void checkYield(PreparedStatement dailyPrice, int dateID, int holdDays, int stockId) {
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
					totalHC++;
					totalSC++;
					if (pYield > 3.0f)
						System.out.println("Find suspected high yield " + pYield + " for stock " + stockId);
					if (debug)
						System.out.println("Find up qualified yield " + pYield + " for stock " + stockId);
				}

				if (pYield < yieldQaulified2) {
					totalSC++;
					totalLC++;
					if (pYield < -0.8f)
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
					if (pYield > 3.0f)
						System.out.println("Find suspected random high yield " + pYield + " for stock " + stockId);

				}

				if (pYield < yieldQaulified2) {
					totalSCRandom++;
					totalRandomLC++;
					if (pYield < -0.8f)
						System.out.println("Find suspected random low yield " + pYield + " for stock " + stockId);

				}
				peakYieldRandom = peakYieldRandom + pYield;
				troughYieldRandom = troughYieldRandom + tYield;
				totalCountRandom++;

				if (debug) {
					System.out.println("Average peak random yield " + 100.0f * peakYieldRandom / totalCountRandom);
					System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);

					System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);
				}
			}
		} catch (Exception ex) {

		}
	}

}
