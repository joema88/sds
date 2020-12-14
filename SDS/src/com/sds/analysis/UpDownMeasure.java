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
	private static int currentDateID = 9045;
	private static int upDays = 30; // this is used to measure 40% up days
	private static int downDays = 250; // this is used to measure 60% down days
	private static int tbkStartDateId = 8261;
	// int buyDateId = 9007; //buy date
	private static int buyDateId = 9034; // sell date, accumulator start here

	public static int[] buyDateIds = new int[3];

	public static void initBuyDateIDS() {
		// 9007,9028
		buyDateIds[0] = 9007;
		buyDateIds[1] = 9028;
		buyDateIds[2] = 9034;
	}

	public static void main(String[] args) {

		// DAILY ROUTINE
		// currentDateID = 9045;
		initCurrentDateID();
		// processUpDownHistory();//no longer do DM update
		// daily step 1
		// processDMAHistory(); //DM update here
		// daily step 2, today only
		// processDMRankAvgDMHistory();
		// daily step 3
		// processFUCHistory();
		// daily step 4
		// Summary.processDailyUTurnSummary(currentDateID);
		// int buyDateId = 9007; //buy date

		// daily step 5
		// processTodayAllPDY(currentDateID, buyDateId,-1);
		// daily step 6
		// processTodayIndustryAVGPDY(currentDateID, -1);
		// daily step 7, update daily OBI (Over bought indicator)
		// processOBIHistory(1);
		// daily step 8, update daily f1, f8 count
		// processF18Today(currentDateID) ;
		// daily step 9, process D2, D9 for each stock
		// processD2D9History(true);
		// daily step 10, process today's VBI
		// processVBIHistory(true);
		// daily step 11, process EE8
		// processTodayEE8(currentDateID);
		// daily step 12, process IAYD
		// processTodayIndustryAVGPDYDelta(currentDateID, -1);
		// daily step 13, process BDA [(Delta of SAY)*100 + (Delta of IAYD)]
		// processTodayBDA(currentDateID, -1);
		// daily step 14, this may have to be switched with step 14 once finalized
		// processRTSHistory(true);
		// daily step 15, process last day TBK, last 30 days breakout bullish pattern
		// base on 30 days breakout mark set in step 14
		// processTBKHistory(true);

		// process entire Rolling Thirty days Sum(P+Y) and MCP(if qualified>=90%)
		// history
		// processRTSHistory(false);
		//// process entire TBK history
		// processStockTBKHistory(963);
		resetAllStocksTBKHistory();
		processTBKHistory(false);
		// process entire BDA history
		// processBDAHistory();
		// calculate entire history
		// processIndustryAVGPDYDeltaHistory(-1);
		// processEE8History();
		// processStockVBIHistory(963);
		// processVBIHistory(false);
		// processD2D9History(false);
		// processPDYHistory(buyDateId);
		// processIndustryAVGPDYHistory(buyDateId);
		// processOBIHistory(320) ;

		// ROUTINE AFTER STOCK SPLIT PROCESSING...
		// After stock split, we need to download history, recalculate this
		// update recalculate stock DM, DA
		// int stockId = 1621;
		// processStockUpDownHistory(stockId);
		// processStockDMAHistory(stockId);
		// update DMRankAvg SET ADM = ?, RK = ?
		// processStockDMRankAvgDMHistory(stockId);
		// update FUC history
		// processStockFUCHistory(stockId);
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

	public static void processF18History(int length) {
		try {
			PreparedStatement f1UpdateStmnt = DB.f1UpdateStmnt();
			PreparedStatement f8UpdateStmnt = DB.f8UpdateStmnt();
			PreparedStatement fucStmnt = DB.getFUCHistoryStmnt();
			// String query = "select a.DATEID,b.CDATE, COUNT(*) FROM BBROCK a, DATES b
			// WHERE a.DATEID=b.DATEID and FUC>? GROUP BY DATEID ORDER BY DATEID DESC limit
			// ?;";

			fucStmnt.setInt(1, 0);
			fucStmnt.setInt(2, length);
			ResultSet rs1 = fucStmnt.executeQuery();
			while (rs1.next()) {
				int dateId = rs1.getInt(1);
				int f1Count = rs1.getInt(3);
				f1UpdateStmnt.setInt(1, f1Count);
				f1UpdateStmnt.setInt(2, dateId);
				f1UpdateStmnt.executeUpdate();
				System.out.println(dateId + " " + f1Count);

			}

			fucStmnt.setInt(1, 4);
			fucStmnt.setInt(2, length);
			ResultSet rs2 = fucStmnt.executeQuery();
			while (rs2.next()) {
				int dateId = rs2.getInt(1);
				int f1Count = rs2.getInt(3);
				f8UpdateStmnt.setInt(1, f1Count);
				f8UpdateStmnt.setInt(2, dateId);
				f8UpdateStmnt.executeUpdate();
				System.out.println(dateId + " " + f1Count);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processF18Today(int dateId) {
		try {
			PreparedStatement f1UpdateStmnt = DB.f1UpdateStmnt();
			PreparedStatement f8UpdateStmnt = DB.f8UpdateStmnt();
			PreparedStatement fucStmnt = DB.getFUCTodayStmnt();
			// String query = "select a.DATEID,b.CDATE, COUNT(*) FROM BBROCK a, DATES b
			// WHERE a.DATEID=b.DATEID and FUC>? GROUP BY DATEID ORDER BY DATEID DESC limit
			// ?;";
			fucStmnt.setInt(1, 0);
			fucStmnt.setInt(2, dateId);
			ResultSet rs1 = fucStmnt.executeQuery();
			while (rs1.next()) {
				// int dateId = rs1.getInt(1);
				int f1Count = rs1.getInt(3);
				f1UpdateStmnt.setInt(1, f1Count);
				f1UpdateStmnt.setInt(2, dateId);
				f1UpdateStmnt.executeUpdate();

			}

			fucStmnt.setInt(1, 4);
			fucStmnt.setInt(2, dateId);
			ResultSet rs2 = fucStmnt.executeQuery();
			while (rs2.next()) {
				// int dateId = rs2.getInt(1);
				int f1Count = rs2.getInt(3);
				f8UpdateStmnt.setInt(1, f1Count);
				f8UpdateStmnt.setInt(2, dateId);
				f8UpdateStmnt.executeUpdate();

			}

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

			if (dateId == 8454) {
				updateMaxUpDown.setFloat(1, 0);
			} else {
				updateMaxUpDown.setFloat(1, MD);
			}
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

	public static void processD2D9History(int stockID) {
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

			// 8923)
			// for (int k = endDateId; k >= strtDateId; k--) {
			// for (int k = endDateId; k >= strtDateId; k--) {
			for (int k = endDateId; k >= 8923; k--) { // we only have info after 8923
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
					processD2Today(stockID, k);
			}

			for (int k = endDateId; k >= strtDateId; k--) {
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
					processD9Today(stockID, k);
			}
			System.out.println("process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processD2Today(int stockID, int dateId) {
		try {

			// "select CLOSE, DATEID, VOLUME,MARKCAP, D2 from BBROCK WHERE STOCKID=? and
			// DATEID>=?
			// and DATEID<=? ORDER BY DATEID ASC";
			PreparedStatement dailyPrice = DB.getDailyPrice();
			// String query = "UPDATE BBROCK SET D2=? WHERE STOCKID=? and DATEID=?";
			PreparedStatement updateD2 = DB.updateD2();

			dailyPrice.setInt(1, stockID);
			dailyPrice.setInt(2, dateId);
			dailyPrice.setInt(3, dateId);

			ResultSet rs = dailyPrice.executeQuery();

			if (rs.next()) {
				float close = rs.getFloat(1);
				float vol = rs.getFloat(3);
				float markcap = rs.getFloat(4);
				if (markcap > 0.0001 && vol > 1.0f && close > 0.01f) {
					float d2 = 1000000.0f * markcap / (vol * close);
					updateD2.setFloat(1, d2);
					updateD2.setInt(2, stockID);
					updateD2.setInt(3, dateId);
					updateD2.executeUpdate();

				}
			}

		} catch (Exception ex) {

		}
	}

	public static void processD9Today(int stockID, int dateId) {
		try {

			// String query = "select AVG(DD) from BBROCK WHERE
			// STOCKID=? and DATEID>=? and DATEID<=?";

			PreparedStatement avgD2 = DB.getAvgD2();
			// String query = "UPDATE BBROCK SET D9=? WHERE STOCKID=? and DATEID=?";
			PreparedStatement updateD9 = DB.updateD9();

			avgD2.setInt(1, stockID);
			avgD2.setInt(2, dateId - 9);
			avgD2.setInt(3, dateId);

			ResultSet rs = avgD2.executeQuery();

			if (rs.next()) {
				float d9 = rs.getFloat(1);

				updateD9.setFloat(1, d9);
				updateD9.setInt(2, stockID);
				updateD9.setInt(3, dateId);
				updateD9.executeUpdate();

			}

		} catch (Exception ex) {

		}
	}

	public static void processStockVBIHistory(int stockID) {
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

			// 8923 is the starting date with volume and marketcap
			// need 12 bottom to avoid end error
			for (int k = endDateId; k >= 8935; k--) {
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

				if (exist) {
					if (k == 8986) {
						System.out.println("K is " + k);
					}
					processVBIToday(stockID, k);
				}
			}

			System.out.println("VBI process done for " + stockID);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processVBIHistory(boolean lastOnly) {
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

				// 8923 is the starting date with volume and marketcap
				// need 12 bottom to avoid end error
				for (int k = endDateId; k >= 8935; k--) {
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

					if (exist) {
						processVBIToday(stockID, k);
						if (lastOnly)
							break;
					}
				}

				System.out.println("VBI process done for " + stockID);
			}

		} catch (

		Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processVBIToday(int stockID, int dateId) {
		try {

			// String query = "select DD, D9 FROM BBROCK
			// WHERE STOCKID =? and DATEID<=? AND DATEID>=? order by DATEID DESC";

			PreparedStatement ddd9Stmnt = DB.getDDD9Stmnt();
			// String query = "UPDATE BBROCK SET VBI=?
			// WHERE STOCKID =? and DATEID=?";

			PreparedStatement updateVBI = DB.updateVBIStmnt();

			ddd9Stmnt.setInt(1, stockID);
			ddd9Stmnt.setInt(2, dateId);
			ddd9Stmnt.setInt(3, dateId - 5);

			ResultSet rs = ddd9Stmnt.executeQuery();

			int VBI = 0;
			int VB1 = 0;
			int VB2 = 0;
			float d20 = 0.0f;
			float d90 = 0.0f;
			int count = 0;
			while (rs.next()) {
				float d2 = rs.getFloat(1);
				float d9 = rs.getFloat(2);
				if (d20 < 0.01f && d90 < 0.01f) {
					d20 = d2;
					d90 = d9;
				} else {
					if (3 * d20 < d2) { // DD must be reduced more than 2/3
						if (d90 < d9 && d90 >= 0.9f * d9) { // D9 reduced but not more than 10%
							VBI = 118;
						} else if (d90 < d9 && d90 < 0.9f * d9 && d90 > 0.8f * d9) {
							VBI = 108;
						} else if (d90 > d9 && d90 <= 1.1f * d9) { // not sure if details matters
							VBI = 28;
						} else if (d90 > d9 && d90 > 1.1f * d9 && d90 < 1.2f * d9) { // not sure if details matters
							VBI = 18;
						}

						if (VB1 == 0) {
							VB1 = VBI;
							VBI = 0;
						} else {
							VB2 = VBI;
							VBI = 0;
						}
					}
				}
				count++;
				if (count >= 3)
					break;

			}

			if (VB1 >= VB2) {
				VBI = VB1;
			} else {
				VBI = VB2;
			}

			updateVBI.setInt(1, VBI);
			updateVBI.setInt(2, stockID);
			updateVBI.setInt(3, dateId);
			updateVBI.executeUpdate();

		} catch (Exception ex) {

		}
	}

	public static void processOBIHistory(int length) {
		try {
			PreparedStatement OBIHistoryStmnt = DB.getOBIHistoryStmnt();
			PreparedStatement UpdateOBIStmnt = DB.getUpdateOBIStmnt();

			// the last 1 and half data is more reliable
			OBIHistoryStmnt.setInt(1, length);
			ResultSet rs = OBIHistoryStmnt.executeQuery();

			while (rs.next()) {
				int dateId = rs.getInt(1);
				int obi = rs.getInt(3);

				UpdateOBIStmnt.setInt(1, obi);
				UpdateOBIStmnt.setInt(2, dateId);
				UpdateOBIStmnt.executeUpdate();
				System.out.println("DateId done " + dateId);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayOBI(int dateId) {
		try {
			PreparedStatement todayOBIStmnt = DB.getTodayOBIStmnt();
			PreparedStatement UpdateOBIStmnt = DB.getUpdateOBIStmnt();

			// the last 1 and half data is more reliable
			todayOBIStmnt.setInt(1, dateId);
			ResultSet rs = todayOBIStmnt.executeQuery();

			while (rs.next()) {
				// int dateId = rs.getInt(1);
				int obi = rs.getInt(3);

				UpdateOBIStmnt.setInt(1, obi);
				UpdateOBIStmnt.setInt(2, dateId);
				UpdateOBIStmnt.executeUpdate();
				System.out.println("DateId done " + dateId);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void initCurrentDateID() {
		try {
			PreparedStatement cDateIDStmt = DB.getCurrentDateID();
			ResultSet rs = cDateIDStmt.executeQuery();
			if (rs.next()) {
				currentDateID = rs.getInt(1);
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// currentDateID
	public static void processEE8History() {
		// remember update currentDateID
		initCurrentDateID();
		// 8933 is the 11 th day after we have volume, mark info which
		// are required to calculate VBI info, so start from here
		for (int k = 8933; k <= currentDateID; k++) {
			processTodayEE8(k);
		}
	}

	public static void processStockEE8History(int stockID) {
		// remember update currentDateID
		// 8933 is the 11 th day after we have volume, mark info which
		// are required to calculate VBI info, so start from here
		initCurrentDateID();
		for (int k = 8933; k <= currentDateID; k++) {
			processTodayEE8ForStock(k, stockID);
		}
	}

	public static void processTodayEE8(int dateId) {
		try {
			PreparedStatement todayVBIFXStmnt = DB.getTodayVBIFUCX();

			// String query = "select FUC,VBI,STOCKID,CLOSE, DATEID
			// FROM BBROCK WHERE DATEID=? AND (FUC>? OR VBI>?) ORDER BY STOCKID ASC";

			todayVBIFXStmnt.setInt(1, dateId);
			todayVBIFXStmnt.setInt(2, 1); // FUC=8 or FUC=4
			todayVBIFXStmnt.setInt(3, 100); // VBI=118 or VBI=108
			ResultSet rs = todayVBIFXStmnt.executeQuery();

			while (rs.next()) {
				int stkid = rs.getInt(3);

				processTodayEE8ForStock(dateId, stkid);
				System.out.println("EE8ForStock done for stockid " + stkid);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processTodayEE8ForStock(int dateId, int stockID) {
		try {
			PreparedStatement stockVBIFXStmnt = DB.getStockVBIFUCX();
			PreparedStatement updateEE8 = DB.updateEE8();

			// String query = "select FUC,VBI,STOCKID,CLOSE, DATEID
			// FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=? ORDER BY DATEID
			// DESC";

			stockVBIFXStmnt.setInt(1, stockID);
			stockVBIFXStmnt.setInt(2, dateId - 6);
			stockVBIFXStmnt.setInt(3, dateId);
			ResultSet rs = stockVBIFXStmnt.executeQuery();

			int fucMax = 0;
			int vbiMax = 0;
			int count = 0;
			while (rs.next()) {
				int fuc = rs.getInt(1);
				int vbi = rs.getInt(2);

				if (fuc > fucMax) {
					fucMax = fuc;
				}
				if (vbi > vbiMax) {
					vbiMax = vbi;
				}

				count++;
				if (count >= 3) {
					break;
				}
			}

			// String query = "UPDATE BBROCK SET EE8=? WHERE STOCKID=?
			// AND DATEID=?";
			int EE8 = 0;
			if (fucMax == 8 && vbiMax == 118) {
				EE8 = 88;
			} else if (fucMax == 8 && vbiMax == 108) {
				EE8 = 84;
			} else if (fucMax == 4 && vbiMax == 118) {
				EE8 = 48;
			} else if (fucMax == 4 && vbiMax == 108) {
				EE8 = 44;
			}
			updateEE8.setInt(1, EE8);
			updateEE8.setInt(2, stockID);
			updateEE8.setInt(3, dateId);
			updateEE8.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processD2D9History(boolean lastOnly) {
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

				// 8923 is the starting date with volume and marketcap
				for (int k = endDateId; k >= 8923; k--) {
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

					if (exist) {
						processD2Today(stockID, k);
						if (lastOnly)
							break;
					}
				}

				// 8923 is the starting date with volume and marketcap
				for (int k = endDateId; k >= 8923; k--) {
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

					if (exist) {
						processD9Today(stockID, k);
						if (lastOnly)
							break;
					}
				}
				System.out.println("process D2D9 done for " + stockID);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processRTSHistory(boolean lastOnly) {
		// get all stocks, may be current??
		try {

			long t1 = System.currentTimeMillis();

			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);

				processStockRTSHistory(stockID, lastOnly);

				System.out.println("process StockRTSHistory done for " + stockID);
				Thread.sleep(2000);
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		// loop through stocks and calculate each history of RTS
		// call processStockRTSHistory(int stockId, boolean lastOnly)

	}

	public static void processStockRTSHistory(int stockId, boolean lastOnly) {
		try {
			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			initCurrentDateID();
			int beginDateId = 0;
			int endDateId = currentDateID;

			if (lastOnly && currentDateID > 0) {
				processStockRTSToday(stockId, currentDateID);
			} else {
				// String query = "select MIN(DATEID), MAX(DATEID)
				// FROM BBROCK WHERE STOCKID = ? ";
				dateIdRange.setInt(1, stockId);
				ResultSet rs = dateIdRange.executeQuery();
				if (rs.next()) {
					beginDateId = rs.getInt(1);
					endDateId = rs.getInt(2);
				}
				int begin = beginDateId + 30;

				if (begin < 8261)// 800 days is enough for now,2017/10/09
					begin = 8261;

				for (int k = endDateId; k >= begin; k--) {
					processStockRTSToday(stockId, k);
				}
			}
			// loop through stocks and calculate each history of RTS
			// call processStockRTSToday(int stockId, int dateId)
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processStockRTSToday(int stockId, int dateId) {
		try {
			PreparedStatement past30Stmnt = DB.getPast30Stmnt();
			PreparedStatement updateRtsMcp = DB.updateRTS_MCP();
			// String query = "SELECT MAX(CLOSE),SUM(YELLOW),SUM(PINK),
			// AVG(VOLUME) FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=?";
			past30Stmnt.setInt(1, stockId);
			past30Stmnt.setInt(2, dateId - 29);
			past30Stmnt.setInt(3, dateId);

			ResultSet rs = past30Stmnt.executeQuery();
			if (rs.next()) {
				float mcp = rs.getFloat(1);
				int ySum = rs.getInt(2);
				int pSum = rs.getInt(3);
				// String query = "UPDATE BBROCK SET RTS=?, MCP=?
				// WHERE STOCKID=? AND DATEID=?";
				int rts = ySum + pSum;

				if (rts >= 27) { // 90% of 30 days =27
					// update RTS, MCP
					updateRtsMcp.setInt(1, rts);
					updateRtsMcp.setFloat(2, mcp);
					updateRtsMcp.setInt(3, stockId);
					updateRtsMcp.setInt(4, dateId);
					updateRtsMcp.executeUpdate();

				} else {
					// update RTS only
					updateRtsMcp.setInt(1, rts);
					updateRtsMcp.setFloat(2, 0.00000000f);
					updateRtsMcp.setInt(3, stockId);
					updateRtsMcp.setInt(4, dateId);
					updateRtsMcp.executeUpdate();
				}
			}

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

	public static void resetAllStocksTBKHistory() {
		try {
			PreparedStatement resetTBK = DB.resetTBKStmnt();
			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);
			ResultSet rs = allStocks.executeQuery();
			System.out.println("-----------Begin reset all stocks TBK to zero---------");
			while (rs.next()) {
				int stockID = rs.getInt(1);
				// reset TBK for this stock to zero as we will recalculate values
				resetTBK.setInt(1, stockID);
				resetTBK.executeUpdate();

			}
			System.out.println("-----------Done reset all stocks TBK to zero---------");
			Thread.sleep(2000);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTBKHistory(boolean lastOnly) {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// process last 200 days for the moment 9058 to 8858
		// begin = 8261;
		// for (int w = currentDateID; w > 8261; w--) {
		for (int w = tbkStartDateId; w <= currentDateID; w++) { // TBK must start from early to latest
			System.out.println("Processing TBK at " + w);
			try {
				processTodayTBK(w, -1);
				// sleep in 2 seconds
				Thread.sleep(2000);
				if (lastOnly) {
					break;
				}
			} catch (Exception ex) {

			}

		}

	}

	public static void processStockTBKHistory(int stockID) {
		try {
			initCurrentDateID();// init currentDateID value based on DB

			PreparedStatement resetTBK = DB.resetTBKStmnt();
			// reset TBK for this stock to zero as we will recalculate values
			resetTBK.setInt(1, stockID);
			resetTBK.executeUpdate();

			PreparedStatement startStmnt = DB.getDateIDStarttmnt();
			// String query = "SELECT DATEID FROM BBROCK WHERE STOCKID =?
			// ORDER BY DATEID ASC limit 1";
			startStmnt.setInt(1, stockID);
			ResultSet rs1 = startStmnt.executeQuery();

			if (rs1.next()) {
				int startDate = rs1.getInt(1);
				boolean reset = false;
				// 30 is the past days number
				int start = tbkStartDateId;
				if (startDate > tbkStartDateId)
					start = startDate + 30;
				for (int w = start; w <= currentDateID; w++) {

					processTodayTBK(w, stockID);
					// sleep in 2 seconds
					// Thread.sleep(2000);
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void processTodayTBK(int dateID, int stockId) {
		PreparedStatement getPureTeal = DB.getPureTeal();
		PreparedStatement updateTBK = DB.updateTBKStmnt();
		// PreparedStatement past30Stmnt = DB.getPast30Stmnt();
		PreparedStatement closestMCPStmnt = DB.getClosetMCPStmnt();
		PreparedStatement closePriceStmnt = DB.getClosePriceStmnt();
		PreparedStatement tbkStmnt = DB.getTBKStmnt();

		try {

			// String query = "select STOCKID FROM BBROCK WHERE DATEID=?
			// AND STOCKID>=? AND STOCKID<=? AND TEAL=1 AND YELLOW=0 AND
			// PINK=0 ORDER BY STOCKID ASC";
			getPureTeal.setInt(1, dateID);
			if (stockId <= 0) {
				getPureTeal.setInt(2, 0);
				getPureTeal.setInt(3, 1000000);
			} else {
				getPureTeal.setInt(2, stockId);
				getPureTeal.setInt(3, stockId);
			}

			ResultSet rs1 = getPureTeal.executeQuery();

			while (rs1.next()) {
				int nextStockID = rs1.getInt(1);
				try {
					System.out.println("Processing TBK at " + dateID + " for " + nextStockID);
					// 1. reset TBK to zero for this stock
					// may be should take out this logic do it separately
					// as one time operation

					// 2. select the closest MCP and RTS
					// String query = "select DATEID,MCP,RTS FROM BBROCK
					// WHERE MCP>0.001 AND RTS>=? AND STOCKID = ?
					// AND DATEID<? AND DATEID>? ORDER BY DATEID DESC LIMIT 50";
					closestMCPStmnt.setInt(1, 27);// 27 is 90% of 30 days, this could be up further
					closestMCPStmnt.setInt(2, nextStockID);
					closestMCPStmnt.setInt(3, dateID);
					closestMCPStmnt.setInt(4, dateID - 500);// almost two years should be enough long

					ResultSet rs2 = closestMCPStmnt.executeQuery();
					if (rs2.next()) {
						// 3. verify that close price>MCP
						int cDateId = rs2.getInt(1);
						float mcp = rs2.getFloat(2);
						int rts = rs2.getInt(3);

						if (dateID == 8907 || dateID == 8945 || dateID == 8949 || dateID == 8950 || dateID == 8951
								|| dateID == 8952 || dateID == 8953 || dateID == 8954) {
							System.out.println("8907 debug starts...");
						}

						// String query = "SELECT CLOSE,PDY,BDY FROM BBROCK
						// WHERE STOCKID = ? AND DATEID =? ";

						closePriceStmnt.setInt(1, nextStockID);
						closePriceStmnt.setInt(2, dateID);

						ResultSet rs3 = closePriceStmnt.executeQuery();
						if (rs3.next()) {
							float closePrice = rs3.getFloat(1);
							if (closePrice > mcp) {
								// now check between cDateId and dateID, how many TBK>=8
								// String query = "SELECT TBK, DATEID, CLOSE FROM BBROCK
								// WHERE TBK>=? AND STOCKID = ? AND DATEID >=? AND
								// DATEID<=? ORDER BY DATEID DESC";
								tbkStmnt.setInt(1, 8); // only interested TBK>=8 cases
								tbkStmnt.setInt(2, nextStockID);
								tbkStmnt.setInt(3, cDateId + 1);
								tbkStmnt.setInt(4, dateID - 1);

								ResultSet rs4 = tbkStmnt.executeQuery();
								int tbkCount = 0;
								int tbkMax = 0;
								int lastestTBKDateID = 0;
								int lastestTBKVal = 0;
								while (rs4.next()) {
									if (tbkCount == 0) {
										lastestTBKVal = rs4.getInt(1);
										lastestTBKDateID = rs4.getInt(2);
										tbkMax = lastestTBKVal;
									}

									int tempTBK = rs4.getInt(1);
									if (tempTBK > tbkMax) {
										tbkMax = tempTBK;
									}
									tbkCount++;
								} // end while

								// 4. check if there is TBK =18 in the past 30 days
								// and if it is the 3rd days within last 5 >MCP

								// b. if it is the 3rd days within last 5 >MCP since TBK=8, then update TBK =18

								// c. if there is TBK = 8 or TBK=88, but at least there is 20??(SUM(p+y) in
								// between, then new TBK=8 maybe??)
								int tbkFinal = 0;
								if (tbkCount == 0) {
									// a. first ever closePrice>closetMCP then update TBK =8
									tbkFinal = 8;

								} else { // tbkCount>0
									// now we need to check between lastestTBKDateID and dateID
									if (lastestTBKVal == 8) { // a failed breakout, then we need to >30 days
										// before next closePrice>MCP considered to be valid
										if ((dateID - lastestTBKDateID) > 30) {
											tbkFinal = 8;
										} else if ((dateID - lastestTBKDateID) <= 5) {
											// now check if we have three days closePrice>MCP
											// if so tbkFinal = 18; or more based on tbkMax
											// not done yet
											// now check between cDateId and dateID, how many TBK>=8
											// String query = "SELECT TBK, DATEID, CLOSE FROM BBROCK
											// WHERE TBK>=? AND STOCKID = ? AND DATEID >=? AND
											// DATEID<=? ORDER BY DATEID DESC";
											tbkStmnt.setInt(1, 0); // check every day close price, so TBK>=0
											tbkStmnt.setInt(2, nextStockID);
											tbkStmnt.setInt(3, lastestTBKDateID);
											tbkStmnt.setInt(4, dateID);

											ResultSet rs5 = tbkStmnt.executeQuery();

											int aboveCount = 0;
											while (rs5.next()) {
												float close = rs5.getFloat(3);
												if (close > mcp) {
													aboveCount++;
												}
											}

											if (aboveCount == 3) {
												tbkFinal = tbkMax + 10;
											}
										}

									} else { // more than 8
										if ((dateID - lastestTBKDateID) > 30) {
											// more than 30 days gap, new signal
											tbkFinal = 8;
										} else {
											// ignore
										}

									}

								}

								// then update TBK
								// String query = "UPDATE BBROCK SET TBK=?
								// WHERE STOCKID =? and DATEID=?";

								updateTBK.setInt(1, tbkFinal);
								updateTBK.setInt(2, nextStockID);
								updateTBK.setInt(3, dateID);
								updateTBK.executeUpdate();
							} // end if (closePrice > mcp)
						} // if (rs3.next())
					} // if (rs2.next()) {

				} catch (Exception ex) {

				}
			} // while (rs1.next()) {

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	// this is the old logic using immediately previous 30 days'
	// sum(P+Y) and max lose price as reference
	// The new model relax the gap between breakout and rolling 30 days mark
	// Thus is more generic, also we could improve the requirement (bigger sum(Y+P))
	// and thus improving accuracy and catch more positive cases at the same time
	public static void processTodayTBK_OLD(int dateID, int stockId) {
		PreparedStatement getPureTeal = DB.getPureTeal();
		PreparedStatement updateTBK = DB.updateTBKStmnt();
		PreparedStatement past30Stmnt = DB.getPast30Stmnt();
		int pastDays = 30;
		float tier1 = 0.7f;
		float tier2 = 0.8f;
		float tier3 = 0.9f;
		float tier4 = 1.0f;
		float priceMaxLow = 0.01f;

		try {

			// String query = "select STOCKID FROM BBROCK WHERE DATEID=?
			// AND STOCKID>=? AND STOCKID<=? AND TEAL=1 AND YELLOW=0 AND
			// PINK=0 ORDER BY STOCKID ASC";
			getPureTeal.setInt(1, dateID);
			if (stockId <= 0) {
				getPureTeal.setInt(2, 0);
				getPureTeal.setInt(3, 1000000);
			} else {
				getPureTeal.setInt(2, stockId);
				getPureTeal.setInt(3, stockId);
			}

			ResultSet rs1 = getPureTeal.executeQuery();

			while (rs1.next()) {
				int nextStockID = rs1.getInt(1);
				try {
					System.out.println("Processing TBK at " + dateID + " for " + nextStockID);

					// String query = "SELECT MAX(CLOSE),SUM(YELLOW),SUM(PINK),
					// AVG(VOLUME) FROM BBROCK WHERE STOCKID=? AND DATEID>=? AND DATEID<=?";

					past30Stmnt.setInt(1, nextStockID);
					past30Stmnt.setInt(2, dateID - pastDays);
					past30Stmnt.setInt(3, dateID - 1);

					ResultSet rs2 = past30Stmnt.executeQuery();
					if (rs2.next()) {
						float maxClose = rs2.getFloat(1);
						int sumYellow = rs2.getInt(2);
						int sumPink = rs2.getInt(3);
						float avgVol = rs2.getFloat(4);
						int totalBars = sumYellow + sumPink;
						// at least Y+P bar number in past days meets low bar
						if (totalBars >= tier1 * pastDays) {
							// get today
							past30Stmnt.setInt(1, nextStockID);
							past30Stmnt.setInt(2, dateID);
							past30Stmnt.setInt(3, dateID);
							ResultSet rs3 = past30Stmnt.executeQuery();
							if (rs3.next()) {
								float close = rs3.getFloat(1);
								float vol = rs3.getFloat(4);
								// -->TBK(58)/18 (18 if price<>) or 80%(>=24)-->TBK=68/28 or
								// 90%(>=27)-->TBK=78/38 or 100%(>=30)-->TBK=88/48, Teal number not considered
								// the last bar close price>max(previous 30 days) or at least within 1% (then
								// wait for new high)
								int tbk = 0;
								if (totalBars >= tier4 * pastDays) {// 100% Yellow
									if (close > maxClose) {
										tbk = 88;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 48;
									}
								} else if (totalBars >= tier3 * pastDays) {// 90% Yellow
									if (close > maxClose) {
										tbk = 78;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 38;
									}
								} else if (totalBars >= tier2 * pastDays) {// 80% Yellow
									if (close > maxClose) {
										tbk = 68;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 28;
									}
								} else if (totalBars >= tier1 * pastDays) {// 70% Yellow
									if (close > maxClose) {
										tbk = 58;
									} else if ((priceMaxLow + 1.0f) * close > maxClose) {
										tbk = 18;
									}
								}

								// then update TBK
								// String query = "UPDATE BBROCK SET TBK=?
								// WHERE STOCKID =? and DATEID=?";

								updateTBK.setInt(1, tbk);
								updateTBK.setInt(2, nextStockID);
								updateTBK.setInt(3, dateID);
								updateTBK.executeUpdate();
							}
						}

					}
				} catch (Exception ex) {

				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	// process entire Boduang Delta (Delta of SAY*100+ Delta of IAYD) Alarm history
	public static void processBDAHistory() {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// from the 1st day that we have SAY data 9/25/2020
		// as SAY
		for (int w = 9007; w <= currentDateID; w++) {

			processTodayBDA(w, -1);

		}

	}

	public static void processStockBDAHistory(int stockID) {

		long t1 = System.currentTimeMillis();

		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		// from the 1st day that we have SAY data 9/25/2020
		// as SAY
		for (int w = 9007; w <= currentDateID; w++) {

			processTodayBDA(w, stockID);

		}

	}

	public static void processTodayBDA(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();

			PreparedStatement stockInfoHistory = DB.getStockInfoHistory();
			PreparedStatement DBAdUpdate = DB.updateBDAStmnt();

			System.out.println("Processing BDA dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int currentIndId = 0;
			int currentSubIndId = 0;

			Hashtable IndStocks = new Hashtable();
			float indSumBDY = 0.0f;
			int indSumPDY = 0;
			while (rs.next()) {
				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
				// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
				// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=?
				// and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
				// ORDER BY MARKCAP ASC";

				subIndStockInfo.setInt(1, dateId);
				subIndStockInfo.setInt(2, currentIndId);
				subIndStockInfo.setInt(3, currentSubIndId);

				ResultSet rs0 = subIndStockInfo.executeQuery();

				while (rs0.next()) {
					// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
					// b.SYMBOL, close, BDY,PDY,SAY,IAYD FROM BBROCK a,
					// SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and
					// a.DATEID>=? and a.DATEID<=? and a.STOCKID=b.STOCKID
					// and a.STOCKID=? ORDER BY a.DATEID DESC";

					int stockId = rs0.getInt(3);

					stockInfoHistory.setInt(1, (dateId - 5));
					stockInfoHistory.setInt(2, dateId);
					stockInfoHistory.setInt(3, stockId);

					ResultSet rs1 = stockInfoHistory.executeQuery();

					float say1 = 0.0f;
					float say2 = 0.0f;
					float iayd1 = 0.0f;
					float iayd2 = 0.0f;

					int lcc = 0;
					while (rs1.next()) {
						String symb = rs1.getString(4);
						if (lcc == 0) {
							say1 = rs1.getFloat(8);
							iayd1 = rs1.getFloat(9);
						} else {
							say2 = rs1.getFloat(8);
							iayd2 = rs1.getFloat(9);

							boolean update = false;

							if (stockID > 0 && stockID == (20000 + stockId)) {
								update = true;
							} else if (stockID < 0) {
								update = true;
							}

							if (update) {

								// calculate BDA and update
								float BDA = (say1 - say2) * 100 + (iayd1 - iayd2);
								// String query = "UPDATE BBROCK SET BDA=?
								// WHERE STOCKID =? and DATEID=?";

								DBAdUpdate.setFloat(1, BDA);

								if (stockID > 0) {
									DBAdUpdate.setInt(2, stockID);
								} else {
									DBAdUpdate.setInt(2, stockId);
								}
								DBAdUpdate.setInt(3, dateId);
								DBAdUpdate.executeUpdate();
							}
						}

						lcc++;
						if (lcc >= 2) {
							lcc = 0;
							break;
						}

					}
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is done after individual stock calculation of such
	// this is at Industry sector and sub sector levels
	public static void processIndustryAVGPDYHistory(int buyPoint) {

		long t1 = System.currentTimeMillis();

		initBuyDateIDS(); // init buyDateIds array, currently manual update
		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		for (int w = 0; w < buyDateIds.length; w++) {
			int endDate = currentDateID;
			if ((w + 1) < buyDateIds.length) {
				endDate = buyDateIds[w + 1];
			}
			for (int k = buyDateIds[w] + 1; k <= endDate && buyDateIds[w] >= buyPoint; k++) {
				processTodayIndustryAVGPDY(k, -1);
			}

		}

	}

	public static void processTodayIndustryAVGPDY(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();
			PreparedStatement indAvgYieldUpdate = DB.getIndAvgYieldUpdateStmnt();
			PreparedStatement subIndAvgYieldUpdate = DB.getSubIndAvgYieldUpdateStmnt();

			System.out.println("Processing dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int preIndId = 0;
			int currentIndId = 0;
			int currentSubIndId = 0;

			Hashtable IndStocks = new Hashtable();
			float indSumBDY = 0.0f;
			int indSumPDY = 0;
			while (rs.next()) {
				if (preIndId == 0) {
					preIndId = rs.getInt(2);
				}

				if (currentIndId != preIndId) { // new industry
					// update industry avg for all stocks
					System.out.println("Processing dateId--" + dateId + " for Industry " + currentIndId);

					Enumeration en = IndStocks.keys();

					while (en.hasMoreElements()) {
						String sym = en.nextElement().toString();
						int stockId = Integer.parseInt(IndStocks.get(sym).toString());
						// String query = "UPDATE BBROCK SET IAY = ?, IPY=?
						// WHERE STOCKID = ? AND DATEID =? ";
						boolean update = false;
						if (stockID > 0 && stockID == (20000 + stockId)) {
							update = true;
						} else if (stockID < 0) {
							update = true;
						}

						if (update) {
							float iay = indSumBDY / (IndStocks.size() * 1.0f);
							float ipy = (indSumPDY * 1.0f) / (IndStocks.size() * 1.0f);
							indAvgYieldUpdate.setFloat(1, iay);
							indAvgYieldUpdate.setFloat(2, ipy);
							if (stockID > 0) {
								indAvgYieldUpdate.setInt(3, stockID);
							} else {
								indAvgYieldUpdate.setInt(3, stockId);
							}
							indAvgYieldUpdate.setInt(4, dateId);
							indAvgYieldUpdate.executeUpdate();
						}
					}
					// reset sum
					indSumBDY = 0.0f;
					indSumPDY = 0;
					IndStocks = new Hashtable();
				}

				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
				// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
				// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=?
				// and a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
				// ORDER BY MARKCAP ASC";

				subIndStockInfo.setInt(1, dateId);
				subIndStockInfo.setInt(2, currentIndId);
				subIndStockInfo.setInt(3, currentSubIndId);

				ResultSet rs1 = subIndStockInfo.executeQuery();
				Hashtable subIndStocks = new Hashtable();
				float sumBDY = 0.0f;
				int sumPDY = 0;

				while (rs1.next()) {
					String symb = rs1.getString(4);
					int stockId = rs1.getInt(3);
					float bdy = rs1.getFloat(6);
					int pdy = rs1.getInt(7);
					subIndStocks.put(symb, "" + stockId);
					sumBDY = sumBDY + bdy;
					sumPDY = sumPDY + pdy;
				}

				indSumBDY = indSumBDY + sumBDY;
				indSumPDY = indSumPDY + sumPDY;

				float avgBDY = sumBDY / (subIndStocks.size() * 1.0f);
				float avgPDY = (sumPDY * 1.0f) / (subIndStocks.size() * 1.0f);

				Enumeration en = subIndStocks.keys();
				System.out.println("Processing dateId--" + dateId + " for Industry " + currentIndId + " sunInd "
						+ currentSubIndId);
				while (en.hasMoreElements()) {
					String sym = en.nextElement().toString();
					int stockId = Integer.parseInt(subIndStocks.get(sym).toString());
					IndStocks.put(sym, "" + stockId);
					// String query = "UPDATE BBROCK SET SAY = ?, SPY=?
					// WHERE STOCKID = ? AND DATEID =? ";

					boolean update = false;
					if (stockID > 0 && stockID == (20000 + stockId)) {
						update = true;
					} else if (stockID < 0) {
						update = true;
					}

					if (update) {
						subIndAvgYieldUpdate.setFloat(1, avgBDY);
						subIndAvgYieldUpdate.setFloat(2, avgPDY);

						if (stockID > 0) {
							subIndAvgYieldUpdate.setInt(3, stockID);
						} else {
							subIndAvgYieldUpdate.setInt(3, stockId);
						}
						subIndAvgYieldUpdate.setInt(4, dateId);
						subIndAvgYieldUpdate.executeUpdate();
					}
				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is done after individual stock calculation of such
	// this is at Industry sector and sub sector levels
	// compare the half smaller cap stocks performance against the bigger half
	// cap stocks of the same category AvgPDY (Delta)
	public static void processIndustryAVGPDYDeltaHistory(int buyPoint) {

		long t1 = System.currentTimeMillis();

		initBuyDateIDS(); // init buyDateIds array, currently manual update
		initCurrentDateID();// init currentDateID value based on DB
		// need a for loop here for history

		for (int w = 0; w < buyDateIds.length; w++) {
			int endDate = currentDateID;
			if ((w + 1) < buyDateIds.length) {
				endDate = buyDateIds[w + 1];
			}
			for (int k = buyDateIds[w] + 1; k <= endDate && buyDateIds[w] >= buyPoint; k++) {
				processTodayIndustryAVGPDYDelta(k, -1);
			}

		}

	}

	// calculate IAYD , each day Industry/subindustry category stock AVG BDY
	// for the smaller cap (half of total count) minus the bigger cap portion
	// as we observe a bull sector, smaller cap stocks rocket up harder than bigger
	// one
	// like SOLO, XPEV compared to NIO, TSLA around NOV, 2020
	public static void processTodayIndustryAVGPDYDelta(int dateId, int stockID) {
		try {
			PreparedStatement indIdStmnt = DB.getIndIDStmnt();
			PreparedStatement subIndStockInfo = DB.getAllSubIndStockInfo();
			PreparedStatement subIndAvgYieldUpdate = DB.updateIndAvgYieldDelta();
			PreparedStatement subStockCount = DB.getSubIndStockCount();

			System.out.println("Processing IndustryAVGPDYDelta dateId--" + dateId);
			indIdStmnt.setInt(1, dateId); // only after the buyPoint 1 day we have calculation

			ResultSet rs = indIdStmnt.executeQuery();
			int preIndId = 0;
			int currentIndId = 0;
			int currentSubIndId = 0;

			while (rs.next()) {

				// select COUNT(*),b.INDID, INDUSTRY,b.SUBID, SUBINDUSTRY FROM BBROCK a, SYMBOLS
				// b,DATES c, INDUSTRY d, SUBINDUSTRY e
				// WHERE a.STOCKID = b.STOCKID and a.DATEID=c.DATEID and b.INDID=d.INDID and
				// b.INDID=e.INDID and b.SUBID=e.SUBID and a.DATEID = ?
				// GROUP BY b.INDID,b.SUBID ORDER BY b.INDID ASC,b.SUBID ASC

				currentIndId = rs.getInt(2);
				currentSubIndId = rs.getInt(4);

				// String query = "SELECT count(*) FROM BBROCK a,
				// SYMBOLS b, DATES c WHERE a.DATEID=c.DATEID and
				// a.DATEID=? and a.STOCKID=b.STOCKID and b.INDID=?
				// and b.SUBID=?";

				subStockCount.setInt(1, dateId);
				subStockCount.setInt(2, currentIndId);
				subStockCount.setInt(3, currentSubIndId);
				ResultSet rs0 = subStockCount.executeQuery();

				int totalSubCount = 0;
				if (rs0.next()) {
					totalSubCount = rs0.getInt(1);
				}

				if (totalSubCount > 0) {
					int halfCount = totalSubCount / 2 + totalSubCount % 2;
					// String query = "SELECT a.DATEID, CDATE, a.STOCKID,
					// b.SYMBOL, close, BDY,PDY FROM BBROCK a, SYMBOLS b,
					// DATES c WHERE a.DATEID=c.DATEID and a.DATEID=? and
					// a.STOCKID=b.STOCKID and b.INDID=? and b.SUBID=?
					// ORDER BY MARKCAP ASC";
					subIndStockInfo.setInt(1, dateId);
					subIndStockInfo.setInt(2, currentIndId);
					subIndStockInfo.setInt(3, currentSubIndId);

					Hashtable subIndStocks = new Hashtable();
					ResultSet rs1 = subIndStockInfo.executeQuery();
					Hashtable subIndStocks1 = new Hashtable();
					float sumBDY1 = 0.0f;
					int sumPDY1 = 0;

					Hashtable subIndStocks2 = new Hashtable();
					float sumBDY2 = 0.0f;
					int sumPDY2 = 0;

					if (currentIndId == 20 && currentSubIndId == 1 && dateId == 9047) {
						System.out.println("debugging....");
					}

					int lc = 0;
					while (rs1.next()) {
						lc++;
						String symb = rs1.getString(4);
						int stockId = rs1.getInt(3);
						float bdy = rs1.getFloat(6);
						int pdy = rs1.getInt(7);
						if (lc <= halfCount) {
							subIndStocks1.put(symb, "" + stockId);
							sumBDY1 = sumBDY1 + bdy;
							sumPDY1 = sumPDY1 + pdy;
						} else {
							subIndStocks2.put(symb, "" + stockId);
							sumBDY2 = sumBDY2 + bdy;
							sumPDY2 = sumPDY2 + pdy;
						}

						subIndStocks.put(symb, "" + stockId);
					}

					float avgBDY1 = sumBDY1 / (subIndStocks1.size() * 1.0f);
					float avgBDY2 = sumBDY2 / (subIndStocks2.size() * 1.0f);
					float iayd = avgBDY1 - avgBDY2;

					Enumeration en = subIndStocks.keys();
					System.out.println("Processing IAYD dateId--" + dateId + " for Industry " + currentIndId
							+ " sunInd " + currentSubIndId);
					while (en.hasMoreElements()) {
						String sym = en.nextElement().toString();
						int stockId = Integer.parseInt(subIndStocks.get(sym).toString());
						// String query = "UPDATE BBROCK SET IAYD=?
						// WHERE STOCKID = ? AND DATEID =? ";

						boolean update = false;
						if (stockID > 0 && stockID == (20000 + stockId)) {
							update = true;
						} else if (stockID < 0) {
							update = true;
						}

						if (update) {
							subIndAvgYieldUpdate.setFloat(1, iayd);
							if (stockID > 0) {
								subIndAvgYieldUpdate.setInt(2, stockID);
							} else {
								subIndAvgYieldUpdate.setInt(2, stockId);
							}
							subIndAvgYieldUpdate.setInt(3, dateId);
							try {
								subIndAvgYieldUpdate.executeUpdate();
							} catch (Exception ex) {
								ex.printStackTrace(System.out);
								System.out.println("iayd " + iayd + ", stockId " + stockId + ", dateId " + dateId);
							}
						}
					}
				}
			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	// Accumulated yield of stocks since each stage buying to selling
	// the purpose of such calculation is to find sector rotation or sector
	// advantage, this is individual stock calculation
	public static void processPDYHistory(int buyPoint) {
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
				// for (int k = endDateId; k >= currentDateID; k--) {
				// 9007 is the latest buying point, hard-coded for now
				// int buyPoint = 9007;
				for (int k = buyPoint; k <= endDateId; k++) {
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
						processTodayPDY(stockID, k, buyPoint);
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

	public static void processTodayAllPDY(int dateId, int buyDateId, int stockID) {
		try {
			if (stockID < 0) {
				PreparedStatement allStocks = DB.getAllCurrentStockIDs();
				allStocks.setInt(1, dateId);

				ResultSet rs = allStocks.executeQuery();
				int sc = 0;
				System.out.println("-----------Begin---------");
				while (rs.next()) {
					sc++;
					stockID = rs.getInt(1);
					System.out.println("Processing stock " + stockID);
					processTodayPDY(stockID, dateId, buyDateId);
				}
			} else {
				processTodayPDY(stockID, dateId, buyDateId);
			}
		} catch (Exception ex) {

		}
	}

	public static void processTodayPDY(int stockID, int dateId, int buyDateId) {
		try {
			// String query = "SELECT CLOSE,PDY,BDY FROM BBROCK
			// WHERE STOCKID = ? AND DATEID =? ";

			if (stockID == 5) {
				System.out.println("Testing...");
			}
			PreparedStatement closeStmnt = DB.getClosePriceStmnt();
			PreparedStatement dateIDStmnt = DB.getDateIDStmnt();
			// String query = "UPDATE BBROCK SET BDY = ? , PDY=?
			// WHERE STOCKID = ? AND DATEID=?";

			PreparedStatement updateBDYPDY = DB.updateBDYPDY();

			dateIDStmnt.setInt(1, stockID);
			dateIDStmnt.setInt(2, dateId - 7);
			dateIDStmnt.setInt(3, dateId);

			ResultSet dateIDCount = dateIDStmnt.executeQuery();

			int dateIdStart = 0;
			int count = 0;

			int dateIdStartUp = 0;

			while (dateIDCount.next()) {
				dateIdStart = dateIDCount.getInt(1);
				count++;

				if (count == 2) { // the next day
					dateIdStartUp = dateIdStart;
					break;
				}
			}

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, dateId);
			ResultSet rs0 = closeStmnt.executeQuery();

			rs0.next();
			float close0 = rs0.getFloat(1);
			int pdy0 = rs0.getInt(2);
			float bdy0 = rs0.getFloat(3);

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, dateIdStartUp);
			ResultSet rs1 = closeStmnt.executeQuery();

			rs1.next();
			float close1 = rs1.getFloat(1);
			int pdy1 = rs1.getInt(2);
			float bdy1 = rs1.getFloat(3);

			closeStmnt.setInt(1, stockID);
			closeStmnt.setInt(2, buyDateId);
			ResultSet rs2 = closeStmnt.executeQuery();

			rs2.next();
			float close2 = rs2.getFloat(1);
			int pdy2 = rs2.getInt(2);
			float bdy2 = rs2.getFloat(3);

			float bdy = 100.0f * (close0 - close2) / close2;
			int pdy = pdy1;
			if (close0 > close1) {
				pdy = pdy + 1;
			}

			// fresh buy point start, reset pdy = 1
			if ((dateId - buyDateId) == 1) {
				pdy = 1;
			}

			// String query = "UPDATE BBROCK SET BDY = ? , PDY=?
			// WHERE STOCKID = ? AND DATEID=?";
			updateBDYPDY.setFloat(1, bdy);
			updateBDYPDY.setInt(2, pdy);
			updateBDYPDY.setInt(3, stockID);
			updateBDYPDY.setInt(4, dateId);
			updateBDYPDY.executeUpdate();

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}
