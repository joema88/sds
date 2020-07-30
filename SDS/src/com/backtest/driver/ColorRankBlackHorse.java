package com.backtest.driver;

import java.sql.*;
import java.util.*;

import com.sds.db.DB;

public class ColorRankBlackHorse {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	// private static int startDateID = 8455;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	private static int startDateID = 8707;
	private static PreparedStatement colorRankPriceCheckStmnt = null;

	private static int eDays = 40;
	private static int tDays = 36;
	private static int daysForHold = 250; // 3 months
	private static int totalSC = 0;
	private static int totalCount = 0;
	private static int totalSCRandom = 0;
	private static int totalCountRandom = 0;
	private static float peakYield = 0.0f;
	private static float troughYield = 0.0f;
	private static float peakYieldRandom = 0.0f;
	private static float troughYieldRandom = 0.0f;
	private static float yieldQaulified1 = 0.5f;
	private static float yieldQaulified2 = -0.5f;
	private static boolean debug = false;
	private static Hashtable excludeStocks = null;

	public static void main(String[] args) {
		try {

			excludeStocks = new Hashtable();
			excludeStocks.put("896", "896");
			
			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();

			colorRankPriceCheckStmnt = DB.checkRankPriceStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				if (!excludeStocks.containsKey(""+stockID))
					for (int k = startDateID; k <= 8964; k++) {
						// check TRK+YOR+MOR<-0.5 continuously and within 40 days, close +50%
						// logic: within a few days up, no TEAL, the stock still make big progress
						// big money explosive buy, MRNA, NVAX as examples
						float sumTYM = 0.0f;
						float trk = 0.0f;
						float yor = 0.0f;
						float mor = 0.0f;
						float cPrice = 0.0f;
						float lPrice = 0.0f;
						float hPrice = 0.0f;
						int cDateId = 0;
						int hDateId = 0;
						int lDateId = 0;
						colorRankPriceCheckStmnt.setInt(1, stockID);
						colorRankPriceCheckStmnt.setInt(2, k);
						colorRankPriceCheckStmnt.setInt(3, k + 39);

						ResultSet rs1 = colorRankPriceCheckStmnt.executeQuery();
						// select a.DATEID,a.STOCKID, CDATE, b.SYMBOL, CLOSE, MOR, YOR,
						// TRK,(MOR+YOR+TRK) as TTR,
						// PASS, APAS, BT9,TSM,YSM,PSM,DSM FROM BBROCK a, SYMBOLS b,DATES c
						// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and a.STOCKID=? and
						// a.DATEID >= ? and a.DATEID <= ? ";

						int toleranceCount = 0;
						while (rs1.next()) {
							cDateId = rs1.getInt(1);
							cPrice = rs1.getFloat(5);
							mor = rs1.getFloat(6);
							yor = rs1.getFloat(7);
							trk = rs1.getFloat(8);
							sumTYM = rs1.getFloat(9);
							if (mor > 0.05) {
								toleranceCount++;
							}
							if (yor > 0.05) {
								toleranceCount++;
							}
							if (trk > 0.05) {
								toleranceCount++;
							}
							if (sumTYM > -0.5 || toleranceCount >= 1) {
								break;
							}
							if (hPrice < 0.0001f) {
								hPrice = cPrice;
								lPrice = cPrice;
								hDateId = cDateId;
								lDateId = cDateId;
							}

							if (cPrice > hPrice) {
								hPrice = cPrice;
								hDateId = cDateId;
							}

							if (cPrice < lPrice) {
								lPrice = cPrice;
								lDateId = cDateId;
							}

							if (lDateId < hDateId && hPrice > 1.50f * lPrice) {
								if ((k + 39) < 8939) {
									if (debug)
										System.out.println("Found candidate " + stockID + " between DateId " + lDateId
												+ " and " + hDateId);
									checkYield(dailyPrice, k + 40, daysForHold, stockID);
									k = k + 39;
								}
								break;
							}
						}

					}
				long t2 = System.currentTimeMillis();
				if (sc % 100 == 0)
					System.out.println(sc + " total stocks processed, time cost " + (t2 - t1) / (1000 * 60));
				Thread.sleep(100);
			}
			long t3 = System.currentTimeMillis();
			System.out.println(sc + " stocks have been processed, time cost in minutes... " + (t3 - t1) / (1000 * 60));
			System.out.println("Average peak yield " + 100.0f * peakYield / totalCount);
			System.out.println("Average trough yield " + 100.0f * troughYield / totalCount);
			System.out.println("Success rate " + totalSC + "/" + totalCount);
			System.out.println("Average peak random yield " + 100.0f * peakYieldRandom / totalCountRandom);
			System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);

			System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);

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
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
				}

				count++;
			}

			float pYield = (maxPrice - startPrice) / startPrice;
			float tYield = (minPrice - startPrice) / startPrice;
			if (pYield > yieldQaulified1) {
				totalSC++;
	          if(pYield>3.0f)
					System.out.println("Find suspected high yield " + pYield + " for stock " + stockId);
				if (debug)
					System.out.println("Find up qualified yield " + pYield + " for stock " + stockId);
			}

			if (pYield < yieldQaulified2) {
				totalSC++;
				 if(pYield<-0.8f)
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
			}while(excludeStocks.containsKey(""+compareStockID));
			
			checkRandomYield(dailyPrice, dateID, holdDays, compareStockID);

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
				}

				if (price < minPrice) {
					minPrice = price;
					minDays = count;
				}

				count++;
			}

			float pYield = (maxPrice - startPrice) / startPrice;
			float tYield = (minPrice - startPrice) / startPrice;
			if (pYield > yieldQaulified1) {
				totalSCRandom++;
				 if(pYield>3.0f)
						System.out.println("Find suspected random high yield " + pYield + " for stock " + stockId);
				
			}

			if (pYield < yieldQaulified2) {
				totalSCRandom++;
				 if(pYield<-0.8f)
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

		} catch (Exception ex) {

		}
	}

}
