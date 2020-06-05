package com.sds.driver;

import com.sds.file.*;

import java.sql.*;

import com.sds.analysis.*;
import com.sds.db.*;

public class BuySellBackTestBullTwo {
	private static PreparedStatement ptcp2HistoryStmnt = null;
	private static PreparedStatement queryNextCX520 = null;
	private static PreparedStatement closeStmnt = null;
	private static PreparedStatement queryHighLowPrice = null;
	private static PreparedStatement lastCloseStmnt = null;
	private static PreparedStatement cx520Stmnt = null;

	// BUY AFTER 13TH DAYS OR MERGED (BDW>0), SELL WHEN AVG5<AVG20

	public static void main(String[] args) {
		init();
		String symbol = "CIDM";
		float capital = 10000.0f;
		try {
			int stockID = DB.getSymbolID(symbol);

			ptcp2HistoryStmnt.setInt(1, stockID);
			ptcp2HistoryStmnt.setInt(2, -1);

			ResultSet rs1 = ptcp2HistoryStmnt.executeQuery();

			float buyPrice = 0.0f;
			float sellPrice = 0.0f;
			int buyDate = 0;
			int sellDate = 0;
			float maxPrice = 0.0f;
			float minPrice = 0.0f;
			float gainLoss = 0.0f;
			float maxGain = 0.0f;
			float minGain = 0.0f;
			int txId = 0;

			while (rs1.next()) {
				float pctp2 = rs1.getFloat(1);
				int day2 = rs1.getInt(2);
				int dateId = rs1.getInt(3);
				buyPrice = 0.0f;
				sellPrice = 0.0f;
				buyDate = 0;
				sellDate = 0;
				maxPrice = 0.0f;
				minPrice = 0.0f;
				gainLoss = 0.0f;

				// validate buy date, after 13th day after BDW (after merge)
				// positive, buy on 14 th day with CX520 = 1
				buyDate = dateId + 14;
				cx520Stmnt.setInt(1, stockID);
				cx520Stmnt.setInt(2, buyDate);
				ResultSet rs0 = cx520Stmnt.executeQuery();
				rs0.next();
				int cx520 = rs0.getInt(1);

				if (cx520>0) {
					
					// find buy price
					closeStmnt.setInt(1, stockID);
					closeStmnt.setInt(2, buyDate);
					ResultSet rs2 = closeStmnt.executeQuery();
					if (rs2.next()) {
						buyPrice = rs2.getFloat(1);
					}

					// find sell date
					queryNextCX520.setInt(1, stockID);
					queryNextCX520.setInt(2, buyDate + 1);
					queryNextCX520.setInt(3, -1);
					ResultSet rs3 = queryNextCX520.executeQuery();
					if (rs3.next()) {
						sellDate = rs3.getInt(1) + 1;
					}
					// find sell price
					closeStmnt.setInt(1, stockID);
					closeStmnt.setInt(2, sellDate);
					ResultSet rs4 = closeStmnt.executeQuery();
					if (rs4.next()) {
						sellPrice = rs4.getFloat(1);
					}

					queryHighLowPrice.setInt(1, stockID);
					queryHighLowPrice.setInt(2, buyDate + 1);
					queryHighLowPrice.setInt(3, sellDate + 1);
					ResultSet rs5 = queryHighLowPrice.executeQuery();
					if (rs5.next()) {
						maxPrice = rs5.getFloat(1);
						minPrice = rs5.getFloat(2);
					}

					maxGain = 100.0f * (maxPrice - buyPrice) / buyPrice;
					minGain = 100.0f * (minPrice - buyPrice) / buyPrice;

					if (buyDate > 0 && sellDate > 0) {
						gainLoss = 100.0f * (sellPrice - buyPrice) / buyPrice;
						capital = capital * (1.0f + gainLoss / 100.0f);
						txId++;
						System.out.println("Transaction " + txId);
						System.out.println("Buy date :" + buyDate + " at price: " + buyPrice);
						System.out.println("Sell date :" + sellDate + " at price: " + sellPrice);
						System.out.println("Whole process Gain/Loss is : " + gainLoss + "%    " + capital);
						System.out.println("max gain during this period: " + maxGain + "%");
						System.out.println("min gain during this period: " + minGain + "%");
					}
				}
			}

			if (buyDate > 0 && sellDate == 0) {

				queryHighLowPrice.setInt(1, stockID);
				queryHighLowPrice.setInt(2, buyDate + 1);
				// set end date to unlimited, to current
				queryHighLowPrice.setInt(3, 100000000);
				ResultSet rs5 = queryHighLowPrice.executeQuery();
				if (rs5.next()) {
					maxPrice = rs5.getFloat(1);
					minPrice = rs5.getFloat(2);
				}

				maxGain = 100.0f * (maxPrice - buyPrice) / buyPrice;
				minGain = 100.0f * (minPrice - buyPrice) / buyPrice;

				lastCloseStmnt.setInt(1, stockID);
				ResultSet rs6 = lastCloseStmnt.executeQuery();

				if (rs6.next()) {
					sellPrice = rs6.getFloat(1);
					gainLoss = 100.0f * (sellPrice - buyPrice) / buyPrice;
					capital = capital * (1.0f + gainLoss / 100.0f);
				}

				txId++;
				System.out.println("Transaction " + txId);
				System.out.println("2Buy date :" + buyDate + " at price: " + buyPrice);
				// System.out.println("Sell date :" + sellDate + " at price: " + sellPrice);
				System.out.println("So far Gain/Loss is : " + gainLoss + "%    " + capital);
				System.out.println("max gain during this period: " + maxGain + "%");
				System.out.println("min gain during this period: " + minGain + "%");
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void init() {
		ptcp2HistoryStmnt = TwoBullDB.getPtcp2HistoryStmnt();
		queryNextCX520 = TwoBullDB.getNextCX520Stmnt();
		closeStmnt = DB.getClosePriceStmnt();
		queryHighLowPrice = TwoBullDB.getHighLowPrice();
		lastCloseStmnt = DB.getLastCloseStmnt();
		cx520Stmnt = DB.getCX520Stmnt();

	}
}
