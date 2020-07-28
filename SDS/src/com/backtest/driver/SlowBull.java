package com.backtest.driver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.db.DB;

public class SlowBull {

	// within 24 days, 21 Teals at least, slow bull evaluation //failed
	// but make it variables
	// 30 days, 4 failed max, >=26 teals, twice, the next higher
	//30/30 seems good choiceS
	//----------------
	//eDay=40, tDays = 36 results
	//6368 stocks have been processed, time cost in minutes... 466
	//Average peak yield 29.46227
	//Average trough yield -16.858486
	//Success rate 371/2538
	//Average peak random yield 32.134335
	//Average trough random yield -20.745579
	//Success random rate 417/2538
	//-------------
	private static int eDays = 40;
	private static int tDays = 36;
	private static int startDateID = 1;
	private static int daysForHold = 250; // 3 months
	private static int totalSC = 0;
	private static int totalCount = 0;
	private static int totalSCRandom = 0;
	private static int totalCountRandom = 0;
	private static float peakYield = 0.0f;
	private static float troughYield = 0.0f;
	private static float peakYieldRandom = 0.0f;
	private static float troughYieldRandom = 0.0f;
	private static float yieldQaulified = 0.5f;
	

	public static void main(String[] args) {
		//getRandomStockID() ;
		try {
			float totalInput = 0.0f;
			float totalNow = 0.0f;

			long t1 = System.currentTimeMillis();
			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement sBulls = DB.getSumTeal();
			PreparedStatement dailyPrice = DB.getDailyPrice();

			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				// 7/17/2020
				// dBulls.setInt(1, 8958);
				// start 7/20, 8458
				int startId = DB.getStartDateId(stockID);

				for (int k = startId; k < 8962 - daysForHold;) {
					boolean found = false;
					sBulls.setInt(1, stockID);
					sBulls.setInt(2, k);
					sBulls.setInt(3, k + eDays - 1);

					ResultSet rs2 = sBulls.executeQuery();

					float totalInput1 = 0.0f;
					float totalNow1 = 0.0f;
					int totalShares = 0;
					int preDateId = 0;
					int[] startIndex = new int[1000];
					float[] prices = new float[1000];
					int count = 0;
					if (rs2.next()) {
						int dateId = k + eDays;
						int sumTeal = rs2.getInt(1);
						int pink = rs2.getInt(2);
						int yellow = rs2.getInt(3);
						//if (sumTeal >= tDays) {
						if ((pink+yellow) >= tDays) {
							// evaluate price action
							found = true;
							k = daysForHold + k + eDays;
							checkYield(dailyPrice, dateId, daysForHold, stockID);
						}

					}

					if (!found) {
						k++;
					}

				} // end for
				long t2 = System.currentTimeMillis();
				System.out.println(
						sc + " stocks have been processed, time cost in minutes... " + (t2 - t1) / (1000 * 60));
				System.out.println("Average peak yield " + 100.0f * peakYield / totalCount);
				System.out.println("Average trough yield " + 100.0f * troughYield / totalCount);
				System.out.println("Success rate " + totalSC + "/" + totalCount);
				System.out.println("Average peak random yield " + 100.0f * peakYieldRandom/ totalCountRandom);
				System.out.println("Average trough random yield " + 100.0f * troughYieldRandom / totalCountRandom);
				
				System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);


				Thread.sleep(4000);

			} // end while

			System.out.println("Average yield " + 100.0f * peakYield / totalCount);
			System.out.println("Success rate " + totalSC + "/" + totalCount);
			System.out.println("Average random yield " + 100.0f * peakYieldRandom/ totalCountRandom);
			System.out.println("Success random rate " + totalSCRandom + "/" + totalCountRandom);

			System.out.println("For all stocks total input " + totalInput + " total now " + totalNow);

			DB.closeConnection();
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
			if (pYield > yieldQaulified) {
				totalSC++;
			}

			peakYield = peakYield + pYield;
			troughYield = troughYield + tYield;
			totalCount++;
			
			int compareStockID = DB.getParallelStock(dateID, stockId);
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
			if (pYield > yieldQaulified) {
				totalSCRandom++;
			}

			peakYieldRandom  = peakYieldRandom  + pYield;
			troughYieldRandom = troughYieldRandom + tYield;
			totalCountRandom++;
			
			

		} catch (Exception ex) {

		}
	}

	
	
}
