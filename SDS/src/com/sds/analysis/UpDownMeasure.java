package com.sds.analysis;

import java.sql.*;
import java.util.*;

import com.sds.db.DB;

public class UpDownMeasure {
	// DATEID = 8455, count = 5203, 2018/7/18 starting point
	// private static int startDateID = 8455;

	// 2019/7/19, start here so we have one year ranking also, 252 days 1 year
	// private static int startDateID = 8707;
	private static int startDateID = 8994; // RECALCULATE FROM 8980
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
	private static int currentDateID = 8996;
	private static int upDays = 30; // this is used to measure 40% up days
	private static int downDays = 250; // this is used to measure 60% down days

	public static void main(String[] args) {

		// DAILY ROUTINE
		 currentDateID = 9008;
		// processUpDownHistory();//no longer do DM update
		// processDMAHistory(); //DM update here
		//processDMRankAvgDMHistory();

		processFUCHistory();

		// ROUTINE AFTER STOCK SPLIT PROCESSING...
		// After stock split, we need to download history, recalculate this
		// update recalculate stock DM, DA
	//	int stockId = 1621;
  // processStockUpDownHistory(stockId);
	//processStockDMAHistory(stockId);
		// update DMRankAvg SET ADM = ?, RK = ?
//processStockDMRankAvgDMHistory(stockId);
		// update FUC history
//	processStockFUCHistory(stockId);
		// transfer missing data
	}

	public static void processStockUpDownHistory(int stockID) {
		try {

			excludeStocks = new Hashtable();

			long t1 = System.currentTimeMillis();
			PreparedStatement dailyPrice = DB.getDailyPrice();
			PreparedStatement maxClose = DB.getMaxClose();
			PreparedStatement minClose = DB.getMinClose();
			PreparedStatement getDateIDByPrice = DB.getDateIDbyPrice();
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement resetUpDownToZero = DB.resetUpDownToZero();

			colorRankPriceCheckStmnt = DB.checkRankPriceStmnt();
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement priceByDateID = DB.getPriceByDateID();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			for (int k = endDateId; k >= strtDateId; k--) {

				// for (int k = currentDateID; k >= currentDateID; k--) {
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
					priceByDateID.setInt(1, stockID);
					priceByDateID.setInt(2, k);

					System.out.println(stockID + " " + k);
					ResultSet cPriceStmnt = priceByDateID.executeQuery();

					cPriceStmnt.next();
					float cPrice = cPriceStmnt.getFloat(1);

					dateIDStmnt.setInt(1, stockID);
					dateIDStmnt.setInt(2, k - downDays - 20);
					dateIDStmnt.setInt(3, k);

					ResultSet dateIDCount = dateIDStmnt.executeQuery();

					int dateIdStartUp = 0;
					int dateIdStartDown = 0;
					int dateIdStart = 0;
					int count = 0;
					while (dateIDCount.next()) {
						dateIdStart = dateIDCount.getInt(1);
						count++;
						if (count == upDays) {// 30days
							dateIdStartUp = dateIdStart;
						}
						if (count == downDays) { // 250days
							dateIdStartDown = dateIdStart;
							break;
						}
					}

					maxClose.setInt(1, stockID);
					maxClose.setInt(2, dateIdStartDown);
					maxClose.setInt(3, k);
					System.out.println("dateIdStartDown " + dateIdStartDown + " k " + k);
					ResultSet xpRS = maxClose.executeQuery();

					int xDateID = 0;
					float maxDrop = 0.0f;
					int tempDateId = 0;
					float tempDrop = 0.0f;
					float temMaxPrice = 0.0f;
					float maxPrice = 0.0f;
					while (xpRS.next()) {
						tempDateId = xpRS.getInt(1);
						temMaxPrice = xpRS.getFloat(2);
						tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
						if (xDateID == 0) { // get the last year biggest price drop regardless
							xDateID = tempDateId;
							maxDrop = tempDrop;
							maxPrice = temMaxPrice;
						} else if (tempDrop < -60.0f && tempDateId > xDateID) {
							// essentially we want to find the closest point to current
							// which has a 60% price drop
							xDateID = tempDateId;
							maxDrop = tempDrop;
							maxPrice = temMaxPrice;
						}
					}

					minClose.setInt(1, stockID);
					minClose.setInt(2, dateIdStartUp);
					minClose.setInt(3, k);

					ResultSet mpRS = minClose.executeQuery();

					mpRS.next();

					float minPrice = mpRS.getFloat(1);
					int mDateID = mpRS.getInt(2);
					mpRS.close();
					mpRS = null;

					float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
					int maxDistance = k - xDateID;
					if (maxDistance > downDays)
						maxDistance = downDays;
					int minDistsance = k - mDateID;
					if (minDistsance > upDays)
						minDistsance = upDays;

					// this measure what it feels like if you invest fixed amount of money
					// at the top and the bottom average loss
					// let us assume invest $1000 at each point at maxPrice and minPrice
					float shares = 1000.0f / maxPrice + 1000.0f / minPrice;
					float totalWorth = shares * cPrice;
					float changePercentage = 100.0f * (totalWorth - 2000.0f) / 2000.0f;
					int changeDays = maxDistance - minDistsance;
					if (changeDays < 0)
						changeDays = minDistsance - maxDistance;

					// "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?, DM=?,
					// DA=? WHERE STOCKID = ? AND DATEID = ?";

					// String query = "UPDATE BBROCK SET UPC = ?, UDS = ?,
					// DPC = ?, DDS= ? WHERE STOCKID = ? AND DATEID = ?";

					distanceChangeUpdate.setFloat(1, upFromMin);
					if (minDistsance > 255) {
						distanceChangeUpdate.setInt(2, 255);
					} else {
						distanceChangeUpdate.setInt(2, minDistsance);
					}
					distanceChangeUpdate.setFloat(3, maxDrop);
					if (maxDistance > 255) {
						distanceChangeUpdate.setInt(4, 255);
					} else {
						distanceChangeUpdate.setInt(4, maxDistance);
					}
					distanceChangeUpdate.setInt(5, stockID);
					distanceChangeUpdate.setInt(6, k);
					System.out.println("k " + k + " changeDays " + changeDays + " changePercentage " + changePercentage
							+ " upFromMin " + upFromMin + " minDistsance " + minDistsance + " maxDrop " + maxDrop);
					System.out.println("changeDays " + changeDays + " minDistsance" + minDistsance + ", maxDistance  "
							+ maxDistance);

					distanceChangeUpdate.executeUpdate();
				} catch (Exception ex) {
					ex.printStackTrace(System.out);
				}

			}
			System.out.println(stockID + " processed done");
			Thread.sleep(2000);
			long t2 = System.currentTimeMillis();

			long t3 = System.currentTimeMillis();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processUpDownHistory() {
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
			PreparedStatement resetUpDownToZero = DB.resetUpDownToZero();

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
				if (endDateId < currentDateID) {
					resetUpDownToZero.setInt(1, stockID);
					resetUpDownToZero.executeUpdate();
					ignore = true;
				}
				// if (!ignore||!excludeStocks.containsKey("" + stockID))
				if (!ignore)
					for (int k = currentDateID; k >= strtDateId; k--) {

						// for (int k = currentDateID; k >= currentDateID; k--) {
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
							priceByDateID.setInt(1, stockID);
							priceByDateID.setInt(2, k);

							System.out.println(stockID + " " + k);
							ResultSet cPriceStmnt = priceByDateID.executeQuery();

							cPriceStmnt.next();
							float cPrice = cPriceStmnt.getFloat(1);

							dateIDStmnt.setInt(1, stockID);
							dateIDStmnt.setInt(2, k - downDays - 20);
							dateIDStmnt.setInt(3, k);

							ResultSet dateIDCount = dateIDStmnt.executeQuery();

							int dateIdStartUp = 0;
							int dateIdStartDown = 0;
							int dateIdStart = 0;
							int count = 0;
							while (dateIDCount.next()) {
								dateIdStart = dateIDCount.getInt(1);
								count++;
								if (count == upDays) {// 30days
									dateIdStartUp = dateIdStart;
								}
								if (count == downDays) { // 250days
									dateIdStartDown = dateIdStart;
									break;
								}
							}

							maxClose.setInt(1, stockID);
							maxClose.setInt(2, dateIdStartDown);
							maxClose.setInt(3, k);
							System.out.println("dateIdStartDown " + dateIdStartDown + " k " + k);
							ResultSet xpRS = maxClose.executeQuery();

							int xDateID = 0;
							float maxDrop = 0.0f;
							int tempDateId = 0;
							float tempDrop = 0.0f;
							float temMaxPrice = 0.0f;
							float maxPrice = 0.0f;
							while (xpRS.next()) {
								tempDateId = xpRS.getInt(1);
								temMaxPrice = xpRS.getFloat(2);
								tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
								if (xDateID == 0) { // get the last year biggest price drop regardless
									xDateID = tempDateId;
									maxDrop = tempDrop;
									maxPrice = temMaxPrice;
								} else if (tempDrop < -60.0f && tempDateId > xDateID) {
									// essentially we want to find the closest point to current
									// which has a 60% price drop
									xDateID = tempDateId;
									maxDrop = tempDrop;
									maxPrice = temMaxPrice;
								}
							}

							minClose.setInt(1, stockID);
							minClose.setInt(2, dateIdStartUp);
							minClose.setInt(3, k);

							ResultSet mpRS = minClose.executeQuery();

							mpRS.next();

							float minPrice = mpRS.getFloat(1);
							int mDateID = mpRS.getInt(2);
							mpRS.close();
							mpRS = null;

							float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
							int maxDistance = k - xDateID;
							if (maxDistance > downDays)
								maxDistance = downDays;
							int minDistsance = k - mDateID;
							if (minDistsance > upDays)
								minDistsance = upDays;

							// this measure what it feels like if you invest fixed amount of money
							// at the top and the bottom average loss
							// let us assume invest $1000 at each point at maxPrice and minPrice
							float shares = 1000.0f / maxPrice + 1000.0f / minPrice;
							float totalWorth = shares * cPrice;
							float changePercentage = 100.0f * (totalWorth - 2000.0f) / 2000.0f;
							int changeDays = maxDistance - minDistsance;
							if (changeDays < 0)
								changeDays = minDistsance - maxDistance;

							// "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?, DM=?,
							// DA=? WHERE STOCKID = ? AND DATEID = ?";
							distanceChangeUpdate.setFloat(1, upFromMin);
							if (minDistsance > 255) {
								distanceChangeUpdate.setInt(2, 255);
							} else {
								distanceChangeUpdate.setInt(2, minDistsance);
							}
							distanceChangeUpdate.setFloat(3, maxDrop);
							if (maxDistance > 255) {
								distanceChangeUpdate.setInt(4, 255);
							} else {
								distanceChangeUpdate.setInt(4, maxDistance);
							}
							distanceChangeUpdate.setInt(5, stockID);
							distanceChangeUpdate.setInt(6, k);
							System.out.println("k " + k + " changeDays " + changeDays + " changePercentage "
									+ changePercentage + " upFromMin " + upFromMin + " minDistsance " + minDistsance
									+ " maxDrop " + maxDrop);
							System.out.println("changeDays " + changeDays + " minDistsance" + minDistsance
									+ ", maxDistance  " + maxDistance);

							distanceChangeUpdate.executeUpdate();
						} catch (Exception ex) {
							ex.printStackTrace(System.out);
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
			PreparedStatement distanceChangeUpdate = DB.getUpdateUpDownDistance();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement priceByDateID = DB.getPriceByDateID();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;
			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;
				if (count == upDays) {// 30days
					dateIdStartUp = dateIdStart;
				}
				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			priceByDateID.setInt(1, stockID);
			priceByDateID.setInt(2, dateId);

			System.out.println(stockID + " " + dateId);
			ResultSet cPriceStmnt = priceByDateID.executeQuery();

			cPriceStmnt.next();
			float cPrice = cPriceStmnt.getFloat(1);

			maxClose.setInt(1, stockID);
			maxClose.setInt(2, dateIdStartDown);
			maxClose.setInt(3, dateId);
			System.out.println("dateIdStartDown " + dateIdStartDown + " dateId " + dateId);
			ResultSet xpRS = maxClose.executeQuery();

			int xDateID = 0;
			float maxDrop = 0.0f;
			int tempDateId = 0;
			float tempDrop = 0.0f;
			float temMaxPrice = 0.0f;
			float maxPrice = 0.0f;
			while (xpRS.next()) {
				tempDateId = xpRS.getInt(1);
				temMaxPrice = xpRS.getFloat(2);
				tempDrop = -100.0f * ((temMaxPrice - cPrice) / temMaxPrice);
				if (xDateID == 0) { // get the last year biggest price drop regardless
					xDateID = tempDateId;
					maxDrop = tempDrop;
					maxPrice = temMaxPrice;
				} else if (tempDrop < -60.0f && tempDateId > xDateID) {
					// essentially we want to find the closest point to current
					// which has a 60% price drop
					xDateID = tempDateId;
					maxDrop = tempDrop;
					maxPrice = temMaxPrice;
				}
			}

			minClose.setInt(1, stockID);
			minClose.setInt(2, dateIdStartUp);
			minClose.setInt(3, dateId);

			ResultSet mpRS = minClose.executeQuery();

			mpRS.next();

			float minPrice = mpRS.getFloat(1);
			int mDateID = mpRS.getInt(2);
			mpRS.close();
			mpRS = null;

			float upFromMin = 100.0f * (cPrice - minPrice) / minPrice;
			int maxDistance = dateId - xDateID;
			if (maxDistance > downDays)
				maxDistance = downDays;
			int minDistsance = dateId - mDateID;
			if (minDistsance > upDays)
				minDistsance = upDays;

			// String query = "UPDATE BBROCK SET UPC = ?, UDS = ?, DPC = ?, DDS= ?
			// WHERE STOCKID = ? AND DATEID = ?";

			distanceChangeUpdate.setFloat(1, upFromMin);
			if (minDistsance > 255) {
				distanceChangeUpdate.setInt(2, 255);
			} else {
				distanceChangeUpdate.setInt(2, minDistsance);
			}
			distanceChangeUpdate.setFloat(3, maxDrop);
			if (maxDistance > 255) {
				distanceChangeUpdate.setInt(4, 255);
			} else {
				distanceChangeUpdate.setInt(4, maxDistance);
			}

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

	public static void processTodayFUC(int stockID, int dateId) {
		try {
			if (stockID == 2 && dateId == 8595) {
				System.out.println("Testing...");
			}
			PreparedStatement fud = DB.getFUD();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement updateFUC = DB.updateFUC();
			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - upDays - 10);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == upDays) { // 30days
					dateIdStartUp = dateIdStart;
					break;
				}
			}

			fud.setInt(1, stockID);
			fud.setInt(2, dateIdStartUp);
			fud.setInt(3, dateId);
			ResultSet fuds = fud.executeQuery();

			boolean firstFty = false;
			boolean dmg100 = false;
			// String query = "select UPC,DM,FUC, DATEID FROM BBROCK WHERE STOCKID = ?
			// AND DATEID>=? AND DATEID<=? ORDER BY DATEID DESC";

			int tc = 0;
			int fucValue = 0;
			while (fuds.next()) {
				float upc = fuds.getFloat(1);
				float dm = fuds.getFloat(2);
				int fuc = fuds.getInt(3);
				int date = fuds.getInt(4);
				tc++;
				if (tc == 1 && dm > 100.0f && upc > 40.0f) {
					fucValue = 8;
				} else if (tc == 1 && upc > 40.0f && dm < 100.0f) {
					fucValue = 4;
				}

				if (((tc > 1 && fuc >= 1) || (tc > 1 && upc >= 40.0f)) && fucValue > 1) {
					if (fuc == 8 && fucValue == 8) {
						fucValue = 1;
					} else if (fuc < 8 && fucValue == 8) {
						fucValue = 8;
					} else if (fuc < 8 && fucValue < 8) {
						fucValue = 1;
					}
				}
			}

			// if else
			updateFUC.setInt(1, fucValue);
			updateFUC.setInt(2, stockID);
			updateFUC.setInt(3, dateId);
			updateFUC.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTodayDMA(int stockID, int dateId) {
		try {
			// String query = "select DPC, DATEID FROM BBROCK WHERE STOCKID = ?
			// AND DATEID>=? AND DATEID<=? ORDER BY DPC ASC LIMIT 1";

			PreparedStatement minDPC = DB.getMinDPC();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement currentUPC = DB.getCurrentUPC();
			PreparedStatement updateMaxUpDown = DB.updateMaxUpDown();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			currentUPC.setInt(1, stockID);
			currentUPC.setInt(2, dateId);

			System.out.println(stockID + " " + dateId);
			ResultSet cUPCStmnt = currentUPC.executeQuery();

			cUPCStmnt.next();
			float cUPC = cUPCStmnt.getFloat(1);

			minDPC.setInt(1, stockID);
			minDPC.setInt(2, dateIdStartDown);
			minDPC.setInt(3, dateId);
			ResultSet xpRS = minDPC.executeQuery();

			xpRS.next();

			float mDPC = xpRS.getFloat(1);
			int mDateID = xpRS.getInt(2);

			float MD = cUPC - mDPC;
			int MA = dateId - mDateID;

			updateMaxUpDown.setFloat(1, MD);
			if (MA > 250)
				MA = 250;
			updateMaxUpDown.setInt(2, MA);
			updateMaxUpDown.setInt(3, stockID);
			updateMaxUpDown.setInt(4, dateId);
			updateMaxUpDown.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}
	// private static PreparedStatement dmRank = null;
	// private static PreparedStatement avgDM = null;

	public static void processStockDMRankAvgDMHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			System.out.println("-----------Begin---------");

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			for (int k = endDateId; k >= strtDateId + downDays + 20; k--) {
				// for (int k = currentDateID; k >= 8979; k--) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
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

				processTodayDMRankAvgDM(stockID, k);
			}

			System.out.println("process TodayDMRankAvgDM done for " + stockID);
			Thread.sleep(2000);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processDMRankAvgDMHistory() {
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
				// for (int k = currentDateID; k >= 8979; k--) {
				for (int k = currentDateID; k >= currentDateID; k--) {
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

					processTodayDMRankAvgDM(stockID, k);
				}

				System.out.println("process TodayDMRankAvgDM done for " + stockID);
				// Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayDMRankAvgDM(int stockID, int dateId) {
		try {
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			PreparedStatement avgDM = DB.getAvgDMStmnt();
			PreparedStatement rankDM = DB.getDMRankStmnt();
			PreparedStatement updateDMRankAvg = DB.getUpdateDMRankAVGStmnt();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - downDays - 20);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartDown = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == downDays) { // 250days
					dateIdStartDown = dateIdStart;
					break;
				}
			}

			avgDM.setInt(1, stockID);
			avgDM.setInt(2, dateIdStartDown);
			avgDM.setInt(3, dateId);
			ResultSet xpRS = avgDM.executeQuery();

			xpRS.next();

			float dmAVG = xpRS.getFloat(1);

			rankDM.setInt(1, stockID);
			rankDM.setInt(2, dateIdStartDown);
			rankDM.setInt(3, dateId);

			ResultSet rankRS = rankDM.executeQuery();

			int rank = 0;
			while (rankRS.next()) {
				rank++;
				if (rankRS.getInt(1) == dateId) {
					break;
				}

			}

			updateDMRankAvg.setFloat(1, dmAVG);
			if (rank > 250)
				rank = 250;
			updateDMRankAvg.setInt(2, rank);
			updateDMRankAvg.setInt(3, stockID);
			updateDMRankAvg.setInt(4, dateId);
			updateDMRankAvg.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockDMAHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// for (int k = endDateId; k >= strtDateId; k--) {
			for (int k = endDateId; k >= strtDateId + downDays + 20; k--) {
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
					processTodayDMA(stockID, k);
			}

			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processDMAHistory() {
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

				if (stockID == 48) {
					System.out.println("AMZN");
				}

				dateIdRange.setInt(1, stockID);

				ResultSet dateRS = dateIdRange.executeQuery();

				dateRS.next();

				int strtDateId = dateRS.getInt(1);
				int endDateId = dateRS.getInt(2);

				// for (int k = endDateId; k >= strtDateId; k--) {
				for (int k = currentDateID; k >= currentDateID; k--) {
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
						processTodayDMA(stockID, k);
				}

				System.out.println("process done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStockFUCHistory(int stockID) {
		try {

			long t1 = System.currentTimeMillis();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			// for (int k = strtDateId + upDays; k <= endDateId; k++) {
			for (int k = strtDateId + downDays + 20; k <= endDateId; k++) {
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
						k++;
						adjustment++;
					}
					lc++;

				} while (!exist && lc < lcMax);

				if (exist)
					processTodayFUC(stockID, k);
			}

			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processFUCHistory() {
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

				// for (int k = strtDateId + upDays; k <= endDateId; k++) {
				for (int k = endDateId; k >= currentDateID; k--) {
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
							k++;
							adjustment++;
						}
						lc++;

					} while (!exist && lc < lcMax);

					if (exist)
						processTodayFUC(stockID, k);
				}

				try {
					Thread.sleep(10);
				} catch (Exception ex) {

				}
				System.out.println("process done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}