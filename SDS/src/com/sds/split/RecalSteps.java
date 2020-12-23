package com.sds.split;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.analysis.ALTBT9;
import com.sds.analysis.BPY;
import com.sds.analysis.ColorSummary;
import com.sds.analysis.OneBullPattern;
import com.sds.analysis.PT9;
import com.sds.analysis.Summary;
import com.sds.analysis.TwoBullPattern;
import com.sds.analysis.UpDownMeasure;
import com.sds.db.DB;

public class RecalSteps {
	private static boolean copySuccess = true;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// int stockID = 24651;
		int stockID = 4897;
		String symbol = "NCSM";// PDS
		int splitDateId = 9053; // THE DATEID THAT CLOSE PRICE JUMPED OR DROPPED
		int endDateId = 9056;
		float splitRatio = 20.0f;
		boolean reseveSplit = true;
		// step 1, copy over basic data, works!
		// e.g. false is 1:5 split, true is 5:1 reverse split

		copyData(stockID, splitDateId, splitRatio, reseveSplit);

		if (copySuccess) {
			// do the recaculation on alias stock
			// basically do the calculation on the alias stock
			// with stockID = 20000+originalStockID;
			// symbol=originalStockSymbol+"2";
			recalculateAliasStock(stockID + 20000, symbol + "2");

			// clean up, delete the original stocks, update the copy over stock to real
			// stockid
			// delete inserted stock alias record
			cleanUp(stockID);

			// recalculate industry, sub-industry, and whole aggregate information
			// going forward from that point
			recalculateAggregates(splitDateId, endDateId);
			
		}
	}

	public static void cleanUp(int stockID) {
		// clean up, delete the original stocks, update the copy over stock to real
		// stockid
		// delete inserted stock alias record
		try {
			// delete the original stock record
			PreparedStatement del = DB.deleteStockRecord();
			del.setInt(1, stockID);
			del.execute();

			// update the alias stockid to the real one
			PreparedStatement updateStockID = DB.updateAliasStockID();
			updateStockID.setInt(1, stockID);
			updateStockID.setInt(2, stockID + 20000);
			updateStockID.execute();

			// delete the stock alias symbol
			PreparedStatement delSymb = DB.deleteSymbol();
			delSymb.setInt(1, stockID + 20000);
			delSymb.execute();
		} catch (Exception ex) {

		}

	}

	public static void recalculateAggregates(int splitDateId, int endDateId) {
		// recalculate industry, sub-industry, and whole aggregate information
		// going forward from that point
		int dateId = 0;
		for (dateId = splitDateId; dateId <= endDateId; dateId++) {
			Summary.processDailyUTurnSummary(dateId);
			UpDownMeasure.processTodayIndustryAVGPDY(dateId, -1);
			
		}
		UpDownMeasure.processOBIHistory(endDateId - splitDateId + 1);
		UpDownMeasure.processF18History(endDateId - splitDateId + 1);
		UpDownMeasure.processIndustryAVGPDYDeltaHistory(-1);
		//this has to be done after AVGPDYDeltaHistory
		UpDownMeasure.processBDAHistory();
	}

	public static void recalculateAliasStock(int stockID, String symbol) {
		try {

			System.out.println("Processing summary...");
			Summary.processStock(symbol, 0);
			System.out.println("Processing CCX History...");
			Summary.processCCXHistory(symbol, 0);
			System.out.println("Processing PT9...");
			PT9.processStockHistory(symbol, 0, 0);
			BPY.processStockHistory(symbol, 0, 0);
			System.out.println("Processing bull pattern one ...");
			OneBullPattern.processStock(symbol, -1, -1, false);
			OneBullPattern.findPassPoints(symbol, -1, false);
			TwoBullPattern.mergeBDCXHistory(symbol, -1, false);
			System.out.println("Processing ALTERNATIVE bull pattern one ...");
			ALTBT9.findAltBT9(symbol, -1);
			System.out.println("Process marking pass points");
			ALTBT9.markPassPoints(symbol, -1);
			System.out.println("After Process marking pass points");
			TwoBullPattern.updatePTCP2History(symbol, -1, false);

			UpDownMeasure.processD2D9History(stockID);
			UpDownMeasure.processStockTBKHistory(stockID);

			PreparedStatement dateIdRange = DB.getStockDateIDRange();
			PreparedStatement dateIDExistStmnt = DB.checkDateIDExistsStmnt();

			dateIdRange.setInt(1, stockID);

			ResultSet dateRS = dateIdRange.executeQuery();

			dateRS.next();

			int strtDateId = dateRS.getInt(1);
			int endDateId = dateRS.getInt(2);

			for (int k = strtDateId; k <= endDateId; k++) {
				System.out.println("Processing date id " + k);
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean existDate = false;
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
						existDate = true;
					} else {
						k++;
						adjustment++;
					}
					lc++;

				} while (!existDate && lc < lcMax);

				if (existDate) {
					System.out.println("ColorSummary Processing date id " + k);
					// color summary and ranking
					ColorSummary.updateColorSummary(stockID, k);
					ColorSummary.updateOMColorSummary(stockID, k);
					ColorSummary.updateColorRanking(stockID, k);

					// process up and down
					UpDownMeasure.processTodayUpDown(stockID, k);
					UpDownMeasure.processTodayDMA(stockID, k);
					UpDownMeasure.processTodayDMRankAvgDM(stockID, k);
					UpDownMeasure.processTodayFUC(stockID, k);
					Summary.processDailyUTurnSummary(k);
					if (k >= 8923) { // we only have D2, D9 info after this date
						UpDownMeasure.processVBIToday(stockID, k);
					}
				}
			}

			// next section
			UpDownMeasure.initBuyDateIDS();
			int[] buyDateStart = UpDownMeasure.buyDateIds;
			int index = 0;
			PreparedStatement updatePDYToOne = DB.updatePDYToOne();

			for (int k = 9006; k <= endDateId; k++) {
				// for (int k = currentDateID; k >= currentDateID; k--) {
				boolean existDate = false;
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
						existDate = true;
					} else {
						k--;
						adjustment++;
					}
					lc++;

				} while (!existDate && lc < lcMax);

				if (existDate) {

					if ((index + 1) == buyDateStart.length) {
						if (k > buyDateStart[index]) {
							UpDownMeasure.processTodayAllPDY(k, buyDateStart[index], stockID);
							UpDownMeasure.processTodayIndustryAVGPDY(k, stockID);
						}
					} else if (k > buyDateStart[index] && k <= buyDateStart[index + 1]) {
						UpDownMeasure.processTodayAllPDY(k, buyDateStart[index], stockID);
						UpDownMeasure.processTodayIndustryAVGPDY(k, stockID);
					}

					if (index > 0 && k == buyDateStart[index] + 1) {
						// need to set PDY = 1
						updatePDYToOne.setInt(1, stockID);
						updatePDYToOne.setInt(2, k);
						updatePDYToOne.execute();
					}

					if ((index + 1) < buyDateStart.length && k == buyDateStart[index + 1]) {
						index++;
					}

					UpDownMeasure.processTodayIndustryAVGPDY(k, stockID);
					UpDownMeasure.processTodayOBI(k);
					UpDownMeasure.processF18Today(k);
				}

			}
			
			//recalculate EE8 history for alias stock
			UpDownMeasure.processStockEE8History(stockID);
			//recalculate DBA history for alias stock
			UpDownMeasure.processStockBDAHistory(stockID);
			//recalculate RTS History 30 days breakout mark
			UpDownMeasure.processStockRTSHistory(stockID, false); 
			//recalculate stock breakout history based on RTS marks above
			UpDownMeasure.processStockTBKHistory(stockID);
			//recalculate Average Volume (D9) Indicator
			UpDownMeasure.processStockAVIHistory(stockID, false);
			//recalculate stock TTA bull indicator
			UpDownMeasure.processStockTTAHistory(stockID, false);
			
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void copyData(int stockID, int splitDateId, float splitRatio, boolean reverse) {
		try {
			// TODO Auto-generated method stub, 4651 HPR
			PreparedStatement originalDataStmnt = DB.getOriginalData();
			PreparedStatement insertDataStmnt = DB.insertOriginalData();
			PreparedStatement stockSymbol = DB.getStockSymbol();
			PreparedStatement insertStockSymbol = DB.getSymbolInsertStatement();
			PreparedStatement deleteRecord = DB.deleteStockRecord();

			int newStockID = stockID + 20000;
			stockSymbol.setInt(1, stockID);
			ResultSet rs1 = stockSymbol.executeQuery();

			if (rs1.next()) {
				try {
					deleteRecord.setInt(1, newStockID);
					deleteRecord.execute();

					String symbol = rs1.getString(1);
					insertStockSymbol.setInt(1, stockID + 20000);
					insertStockSymbol.setString(2, symbol + "2");
					insertStockSymbol.execute();

				} catch (Exception ex) {
					// ignore, duplicate
				}

				originalDataStmnt.setInt(1, stockID);
				ResultSet rs2 = originalDataStmnt.executeQuery();

				// STOCKID,DATEID,PERCENT,CLOSE,NETCHANGE,ATR,OPEN,HIGH,
				// LOW,LOW52,HIGH52,MARKCAP,VOLUME,YELLOW,TEAL,PINK,
				float previousClose = 0.0f;
				float previousCap = 0.0f;
				while (rs2.next()) {
					int stockId = rs2.getInt(1) + 20000;
					int dateId = rs2.getInt(2);
					float percent = rs2.getFloat(3);
					float close = rs2.getFloat(4);
					float netChange = rs2.getFloat(5);
					float atr = rs2.getFloat(6);
					float open = rs2.getFloat(7);
					float high = rs2.getFloat(8);
					float low = rs2.getFloat(9);
					float low52 = rs2.getFloat(10);
					float high52 = rs2.getFloat(11);
					float markcap = rs2.getFloat(12);
					float volume = rs2.getFloat(13);
					int yellow = rs2.getInt(14);
					int teal = rs2.getInt(15);
					int pink = rs2.getInt(16);
					int cx520 = rs2.getInt(17);

					if (dateId < splitDateId && reverse) {

						if (previousClose > 0.01f) {
							float percent1 = 100.0f * (close * splitRatio - previousClose) / previousClose;

							float markcap1 = previousCap * (1.0f + percent1 / 100.f);

							if (markcap1 < 0.01f) {
								previousCap = markcap;
							} else if (markcap > 1.25f * markcap1 || markcap < 0.8f * markcap1) {
								previousCap = markcap1;
								markcap = markcap1;
							} else {
								previousCap = markcap;
							}
						} else {
							previousCap = markcap;

						}

						insertDataStmnt.setInt(1, newStockID);
						insertDataStmnt.setInt(2, dateId);
						insertDataStmnt.setFloat(3, percent);
						insertDataStmnt.setFloat(4, close * splitRatio);
						insertDataStmnt.setFloat(5, netChange * splitRatio);
						insertDataStmnt.setFloat(6, atr * splitRatio);
						insertDataStmnt.setFloat(7, open * splitRatio);
						insertDataStmnt.setFloat(8, high * splitRatio);
						insertDataStmnt.setFloat(9, low * splitRatio);
						insertDataStmnt.setFloat(10, low52 * splitRatio);
						insertDataStmnt.setFloat(11, high52 * splitRatio);
						insertDataStmnt.setFloat(12, markcap);
						insertDataStmnt.setFloat(13, volume / splitRatio);
						insertDataStmnt.setInt(14, yellow);
						insertDataStmnt.setInt(15, teal);
						insertDataStmnt.setInt(16, pink);
						insertDataStmnt.setInt(17, cx520);
						insertDataStmnt.execute();

						previousClose = close * splitRatio;

					} else if (dateId >= splitDateId && reverse) {
						insertDataStmnt.setInt(1, newStockID);
						insertDataStmnt.setInt(2, dateId);
						if (dateId == splitDateId) {
							percent = 100.0f * (close - previousClose) / previousClose;
							netChange = previousClose - close;

							markcap = previousCap * (1.0f + percent / 100.f);
						} else {
							float percent1 = 100.0f * (close - previousClose) / previousClose;

							float markcap1 = previousCap * (1.0f + percent1 / 100.f);

							if (markcap1 < 0.01f) {
								previousCap = markcap;
							} else if (markcap > 1.25f * markcap1 || markcap < 0.8f * markcap1) {
								previousCap = markcap1;
								markcap = markcap1;
							} else {
								previousCap = markcap;
							}
						}
						insertDataStmnt.setFloat(3, percent);
						insertDataStmnt.setFloat(4, close);
						insertDataStmnt.setFloat(5, netChange);
						insertDataStmnt.setFloat(6, atr);
						insertDataStmnt.setFloat(7, open);
						insertDataStmnt.setFloat(8, high);
						insertDataStmnt.setFloat(9, low);
						insertDataStmnt.setFloat(10, low52);
						insertDataStmnt.setFloat(11, high52);
						insertDataStmnt.setFloat(12, markcap);
						System.out.println(dateId+" "+markcap+": "+percent);
						insertDataStmnt.setFloat(13, volume);
						insertDataStmnt.setInt(14, yellow);
						insertDataStmnt.setInt(15, teal);
						insertDataStmnt.setInt(16, pink);
						insertDataStmnt.setInt(17, cx520);
						insertDataStmnt.execute();
						previousClose = close;
					} else if (dateId < splitDateId && !reverse) {

						if (previousClose > 0.01f) {
							float percent1 = 100.0f * (close/splitRatio - previousClose) / previousClose;

							float markcap1 = previousCap * (1.0f + percent1 / 100.f);

							if (markcap1 < 0.01f) {
								previousCap = markcap;
							} else if (markcap > 1.25f * markcap1 || markcap < 0.8f * markcap1) {
								previousCap = markcap1;
								markcap = markcap1;
							} else {
								previousCap = markcap;
							}
						} else {
							previousCap = markcap;

						}

						insertDataStmnt.setInt(1, newStockID);
						insertDataStmnt.setInt(2, dateId);
						insertDataStmnt.setFloat(3, percent);
						insertDataStmnt.setFloat(4, close / splitRatio);
						insertDataStmnt.setFloat(5, netChange / splitRatio);
						insertDataStmnt.setFloat(6, atr / splitRatio);
						insertDataStmnt.setFloat(7, open / splitRatio);
						insertDataStmnt.setFloat(8, high / splitRatio);
						insertDataStmnt.setFloat(9, low / splitRatio);
						insertDataStmnt.setFloat(10, low52 / splitRatio);
						insertDataStmnt.setFloat(11, high52 / splitRatio);
						insertDataStmnt.setFloat(12, markcap);
						insertDataStmnt.setFloat(13, volume * splitRatio);
						insertDataStmnt.setInt(14, yellow);
						insertDataStmnt.setInt(15, teal);
						insertDataStmnt.setInt(16, pink);
						insertDataStmnt.setInt(17, cx520);
						insertDataStmnt.execute();

						previousClose = close / splitRatio;
					} else if (dateId >= splitDateId && !reverse) {

						insertDataStmnt.setInt(1, newStockID);
						insertDataStmnt.setInt(2, dateId);
						if (dateId == splitDateId) {
							percent = 100.0f * (close - previousClose) / previousClose;
							netChange = previousClose - close;
							markcap = previousCap * (1.0f + percent / 100.f);
						} else {
							float percent1 = 100.0f * (close - previousClose) / previousClose;

							float markcap1 = previousCap * (1.0f + percent1 / 100.f);

							if (markcap1 < 0.01f) {
								previousCap = markcap;
							} else if (markcap > 1.25f * markcap1 || markcap < 0.8f * markcap1) {
								previousCap = markcap1;
								markcap = markcap1;
							} else {
								previousCap = markcap;
							}
						}

						insertDataStmnt.setFloat(3, percent);
						insertDataStmnt.setFloat(4, close);
						insertDataStmnt.setFloat(5, netChange);
						insertDataStmnt.setFloat(6, atr);
						insertDataStmnt.setFloat(7, open);
						insertDataStmnt.setFloat(8, high);
						insertDataStmnt.setFloat(9, low);
						insertDataStmnt.setFloat(10, low52);
						insertDataStmnt.setFloat(11, high52);
						insertDataStmnt.setFloat(12, markcap);
						insertDataStmnt.setFloat(13, volume);
						insertDataStmnt.setInt(14, yellow);
						insertDataStmnt.setInt(15, teal);
						insertDataStmnt.setInt(16, pink);
						insertDataStmnt.setInt(17, cx520);
						insertDataStmnt.execute();
						previousClose = close;
					}
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
			copySuccess = false;
		}
	}
}
