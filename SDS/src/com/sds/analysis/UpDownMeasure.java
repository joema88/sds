package com.sds.analysis;

import java.sql.*;
import java.util.*;

import com.sds.db.DB;

public class UpDownMeasure {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	// private static int startDateID = 8455;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	private static int startDateID = 8707;
	private static PreparedStatement colorRankPriceCheckStmnt = null;

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
	private static int currentDateID = 8969;
	private static int upDownDays = 30;

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

			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement getDateIDByPrice = DB.getDateIDbyPrice();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			colorRankPriceCheckStmnt = DB.checkRankPriceStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement priceByDateID = DB.getPriceByDateID();
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

				boolean ignore = false;
				if (endDateId < currentDateID)
					ignore = true;

				// if (!ignore||!excludeStocks.containsKey("" + stockID))
				if (!ignore)
					for (int k = currentDateID; k >= strtDateId; k--) {
						boolean exist = false;
						int adjustment = 0;
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

						} while (!exist);

						try {
							dateIDStmnt.setInt(1, stockID);
							dateIDStmnt.setInt(2, k - upDownDays - 10);
							dateIDStmnt.setInt(3, k);

							ResultSet dateIDCount = dateIDStmnt.executeQuery();

							int dateIdStart = 0;
							int count = 0;
							while (dateIDCount.next()) {
								dateIdStart = dateIDCount.getInt(1);
								count++;
								if (count == upDownDays)
									break;
							}

							maxClose.setInt(1, stockID);
							maxClose.setInt(2, dateIdStart);
							maxClose.setInt(3, k);
							System.out.println("dateIdStart " + dateIdStart + " k " + k);
							ResultSet xpRS = maxClose.executeQuery();

							xpRS.next();

							float maxPrice = xpRS.getFloat(1);
							xpRS.close();
							xpRS = null;

							getDateIDByPrice.setInt(1, stockID);
							getDateIDByPrice.setInt(2, dateIdStart);
							getDateIDByPrice.setInt(3, k);
							getDateIDByPrice.setFloat(4, maxPrice - 0.0001f);
							getDateIDByPrice.setFloat(5, maxPrice + 0.0001f);

							ResultSet xDateIDStmnt = getDateIDByPrice.executeQuery();

							xDateIDStmnt.next();

							int xDateID = xDateIDStmnt.getInt(1);
							xDateIDStmnt.close();
							xDateIDStmnt = null;

							minClose.setInt(1, stockID);
							minClose.setInt(2, dateIdStart);
							minClose.setInt(3, k);

							ResultSet mpRS = minClose.executeQuery();

							mpRS.next();

							float minPrice = mpRS.getFloat(1);
							mpRS.close();
							mpRS = null;

							getDateIDByPrice.setInt(1, stockID);
							getDateIDByPrice.setInt(2, dateIdStart);
							getDateIDByPrice.setInt(3, k);
							getDateIDByPrice.setFloat(4, minPrice - 0.0001f);
							getDateIDByPrice.setFloat(5, minPrice + 0.0001f);

							ResultSet mDateIDStmnt = getDateIDByPrice.executeQuery();

							mDateIDStmnt.next();

							int mDateID = mDateIDStmnt.getInt(1);

							priceByDateID.setInt(1, stockID);
							priceByDateID.setInt(2, k);

							System.out.println(stockID + " " + k);
							ResultSet cPriceStmnt = priceByDateID.executeQuery();

							cPriceStmnt.next();

							float cPrice = cPriceStmnt.getFloat(1);

							float maxDrop = 100.0f * (cPrice - maxPrice) / maxPrice;
							float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
							int maxDistance = k - xDateID + 1;
							if (maxDistance > upDownDays)
								maxDistance = upDownDays;
							int minDistsance = k - mDateID + 1;
							if (minDistsance > upDownDays)
								minDistsance = upDownDays;
							distanceChangeUpdate.setFloat(1, upFromMin);
							distanceChangeUpdate.setInt(2, minDistsance);
							distanceChangeUpdate.setFloat(3, maxDrop);
							distanceChangeUpdate.setInt(4, maxDistance);
							distanceChangeUpdate.setInt(5, stockID);
							distanceChangeUpdate.setInt(6, k);

							distanceChangeUpdate.executeUpdate();
						} catch (Exception ex) {

						}

					}
				System.out.println(stockID + " processed done");
				Thread.sleep(2000);
				long t2 = System.currentTimeMillis();
				if (sc % 100 == 0)
					System.out.println(sc + " total stocks processed, time cost " + (t2 - t1) / (1000 * 60));
				Thread.sleep(100);
			}
			long t3 = System.currentTimeMillis();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayUpDown(int stockID, int dateId) {
		try {
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement getDateIDByPrice = DB.getDateIDbyPrice();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement priceByDateID = DB.getPriceByDateID();
		
			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - upDownDays - 10);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;
			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;
				if (count == upDownDays)
					break;
			}

			maxClose.setInt(1, stockID);
			maxClose.setInt(2, dateIdStart);
			maxClose.setInt(3, dateId);
			ResultSet xpRS = maxClose.executeQuery();

			xpRS.next();

			float maxPrice = xpRS.getFloat(1);
			xpRS.close();
			xpRS = null;

			getDateIDByPrice.setInt(1, stockID);
			getDateIDByPrice.setInt(2, dateIdStart);
			getDateIDByPrice.setInt(3, dateId);
			getDateIDByPrice.setFloat(4, maxPrice - 0.0001f);
			getDateIDByPrice.setFloat(5, maxPrice + 0.0001f);

			ResultSet xDateIDStmnt = getDateIDByPrice.executeQuery();

			xDateIDStmnt.next();

			int xDateID = xDateIDStmnt.getInt(1);
			xDateIDStmnt.close();
			xDateIDStmnt = null;

			minClose.setInt(1, stockID);
			minClose.setInt(2, dateIdStart);
			minClose.setInt(3, dateId);

			ResultSet mpRS = minClose.executeQuery();

			mpRS.next();

			float minPrice = mpRS.getFloat(1);
			mpRS.close();
			mpRS = null;

			getDateIDByPrice.setInt(1, stockID);
			getDateIDByPrice.setInt(2, dateIdStart);
			getDateIDByPrice.setInt(3, dateId);
			getDateIDByPrice.setFloat(4, minPrice - 0.0001f);
			getDateIDByPrice.setFloat(5, minPrice + 0.0001f);

			ResultSet mDateIDStmnt = getDateIDByPrice.executeQuery();

			mDateIDStmnt.next();

			int mDateID = mDateIDStmnt.getInt(1);

			priceByDateID.setInt(1, stockID);
			priceByDateID.setInt(2, dateId);

			ResultSet cPriceStmnt = priceByDateID.executeQuery();

			cPriceStmnt.next();

			float cPrice = cPriceStmnt.getFloat(1);

			float maxDrop = 100.0f * (cPrice - maxPrice) / maxPrice;
			float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
			int maxDistance = dateId - xDateID + 1;
			if (maxDistance > upDownDays)
				maxDistance = upDownDays;
			int minDistsance = dateId - mDateID + 1;
			if (minDistsance > upDownDays)
				minDistsance = upDownDays;
			distanceChangeUpdate.setFloat(1, upFromMin);
			distanceChangeUpdate.setInt(2, minDistsance);
			distanceChangeUpdate.setFloat(3, maxDrop);
			distanceChangeUpdate.setInt(4, maxDistance);
			distanceChangeUpdate.setInt(5, stockID);
			distanceChangeUpdate.setInt(6, dateId);

			distanceChangeUpdate.executeUpdate();

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
