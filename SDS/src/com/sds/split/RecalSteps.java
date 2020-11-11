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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// int stockID = 24651;
		int stockID = 20545;
		// step 1, copy over basic data, works!
		// false is 1:5 split, true is 5:1 split
		copyData(stockID - 20000, 9012, 5, false);

		try {
			String symbol = "BIIB2";
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
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void copyData(int stockID, int splitDateId, int splitRatio, boolean reverse) {
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
						insertDataStmnt.setFloat(13, volume);
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
							 percent = 100.0f*(close - previousClose)/previousClose;
							 netChange = previousClose - close;
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
					} else if (dateId < splitDateId && !reverse) {
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
						insertDataStmnt.setFloat(13, volume);
						insertDataStmnt.setInt(14, yellow);
						insertDataStmnt.setInt(15, teal);
						insertDataStmnt.setInt(16, pink);
						insertDataStmnt.setInt(17, cx520);
						insertDataStmnt.execute();
						previousClose = close / splitRatio;
					}else if (dateId >= splitDateId && !reverse) {
						insertDataStmnt.setInt(1, newStockID);
						insertDataStmnt.setInt(2, dateId);
						if (dateId == splitDateId) {
							 percent = 100.0f*(close - previousClose)/previousClose;
							 netChange = previousClose - close;
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
		}
	}
}
